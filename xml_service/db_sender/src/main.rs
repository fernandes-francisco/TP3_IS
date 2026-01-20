use anyhow::{Context, Result};
use native_tls::TlsConnector;
use postgres_native_tls::MakeTlsConnector;
use redis::AsyncCommands;
use serde::{Deserialize, Serialize};
use std::env;
use std::time::Duration;
use tokio_postgres::Client;

#[derive(Serialize, Deserialize, Debug)]
struct PipelineMsg {
    job_id: String,
    chunk_id: u32,
    xml_content: String,
    status: String, 
    mapper_version: String,
}

#[derive(Serialize)]
struct WebhookPayload {
    job_id: String,
    status: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    println!("DB Sender Service Starting...");
    
    let redis_host = env::var("REDIS_HOST").context("REDIS_HOST missing")?;
    let redis_port = env::var("REDIS_PORT").unwrap_or("6379".to_string());
    let redis_password = env::var("REDIS_PASSWORD").unwrap_or_default();
    let db_url = env::var("DATABASE_URL").context("DATABASE_URL missing")?;
    let webhook_url = env::var("WEBHOOK_URL").context("WEBHOOK_URL missing")?;

    println!("Environment variables loaded successfully");

    let redis_url = if !redis_password.is_empty() {
        format!("redis://:{}@{}:{}", redis_password, redis_host, redis_port)
    } else {
        format!("redis://{}:{}", redis_host, redis_port)
    };

    println!("Connecting to Redis at {}:{}...", redis_host, redis_port);
    let redis_client = redis::Client::open(redis_url.clone())
        .context("Failed to create Redis client")?;
    
    // Retry logic for Redis connection
    let mut redis_con = None;
    for attempt in 1..=10 {
        match redis_client.get_tokio_connection().await {
            Ok(conn) => {
                redis_con = Some(conn);
                println!("Connected to Redis successfully");
                break;
            }
            Err(e) => {
                eprintln!("Redis connection attempt {}/10 failed: {}", attempt, e);
                if attempt < 10 {
                    tokio::time::sleep(Duration::from_secs(2)).await;
                } else {
                    return Err(e).context("Failed to connect to Redis after 10 attempts");
                }
            }
        }
    }
    let mut redis_con = redis_con.unwrap();

    println!("Connecting to PostgreSQL...");
    let db_url = ensure_sslmode_require(&db_url);

    // Masked logging
    let masked = if let Some(start) = db_url.find("://") {
        let rest = &db_url[start + 3..];
        if let Some(at) = rest.find('@') {
            format!("{}://***@{}", &db_url[..start], &rest[at+1..])
        } else {
            db_url.clone()
        }
    } else {
        "***".to_string()
    };
    println!("DB URL (masked): {}", masked);

    let tls_connector = TlsConnector::builder()
        .danger_accept_invalid_certs(true)  // Accept Supabase pooler's certificate
        .build()
        .context("Failed to build TLS connector")?;
    let tls = MakeTlsConnector::new(tls_connector);

    // Retry logic for PostgreSQL connection using tokio-postgres + native-tls (Supabase compatible)
    let db_client: Client = loop {
        match tokio_postgres::connect(&db_url, tls.clone()).await {
            Ok((client, connection)) => {
                tokio::spawn(async move {
                    if let Err(e) = connection.await {
                        eprintln!("Database connection error: {}", e);
                    }
                });
                println!("Connected to PostgreSQL successfully");
                break client;
            }
            Err(e) => {
                eprintln!("PostgreSQL connection failed: {}. Retrying in 3 seconds...", e);
                use std::error::Error;
                if let Some(src) = e.source() {
                    eprintln!("  Caused by: {}", src);
                }
                tokio::time::sleep(Duration::from_secs(3)).await;
            }
        }
    };

    let http_client = reqwest::Client::builder()
        .timeout(Duration::from_secs(10))
        .build()
        .context("Failed to create HTTP client")?;

    println!("Persister Service Started. Listening on 'queue:db_persistence'...");

    loop {
        let result: Option<(String, String)> = redis_con.blpop("queue:db_persistence", 0.0).await?;
        
        if let Some((_, json_str)) = result {
            let msg: PipelineMsg = match serde_json::from_str(&json_str) {
                Ok(m) => m,
                Err(e) => {
                    eprintln!("JSON Error: {}", e);
                    continue;
                }
            };

            let mut final_status = msg.status.clone();
            
            if final_status == "OK" {
                println!("Persisting Job {} - Chunk {}", msg.job_id, msg.chunk_id);
                
                let insert_stmt = "INSERT INTO xml_storage (job_id, chunk_id, xml_documento, mapper_version) VALUES ($1, $2, $3::xml, $4)";
                
                match db_client.execute(
                    insert_stmt,
                    &[&msg.job_id, &(msg.chunk_id as i32), &msg.xml_content, &msg.mapper_version],
                ).await {
                    Ok(_) => {
                        println!("Saved Chunk {} to DB.", msg.chunk_id);
                    },
                    Err(e) => {
                        eprintln!("DB Insert Failed: {}", e);
                        final_status = "ERRO_PERSISTENCIA".to_string();
                    }
                }
            } else {
                println!("Skipping persistence for Job {} Chunk {} due to {}", msg.job_id, msg.chunk_id, final_status);
            }

            check_completion(&mut redis_con, &http_client, &msg.job_id, &webhook_url, &final_status).await;
        }
    }
}

fn ensure_sslmode_require(url: &str) -> String {
    if url.contains("sslmode=") {
        url.to_string()
    } else if url.contains('?') {
        format!("{url}&sslmode=require")
    } else {
        format!("{url}?sslmode=require")
    }
}

async fn check_completion(
    con: &mut redis::aio::Connection, 
    http: &reqwest::Client,
    job_id: &str, 
    webhook_url: &str,
    chunk_status: &str
) {
    let processed_key = format!("job:{}:processed", job_id);
    let total_key = format!("job:{}:total", job_id);
    let error_key = format!("job:{}:errors", job_id);

    let processed: i32 = match con.incr(&processed_key, 1).await {
        Ok(v) => v,
        Err(_) => return,
    };

    if chunk_status != "OK" {
        let _: () = con.incr(&error_key, 1).await.unwrap_or(());
    }

    let total_str: Option<String> = con.get(&total_key).await.unwrap_or(None);
    let total: i32 = total_str.unwrap_or("999999".to_string()).parse().unwrap_or(999999);

    println!("Progress Job {}: {} / {}", job_id, processed, total);

    if processed >= total {
        let error_count: i32 = con.get(&error_key).await.unwrap_or(0);
        
        let final_status = if error_count > 0 {
            "CONCLUIDO_COM_ERROS".to_string()
        } else {
            "OK".to_string()
        };

        println!("JOB {} FINISHED! Status: {}. Errors: {}", job_id, final_status, error_count);
        
        let payload = WebhookPayload {
            job_id: job_id.to_string(),
            status: final_status,
        };

        match http.post(webhook_url).json(&payload).send().await {
            Ok(_) => println!("Webhook sent successfully."),
            Err(e) => eprintln!("Failed to call webhook: {}", e),
        }
        
        let _: () = con.del(&[processed_key, total_key, error_key]).await.unwrap_or(());
    }
}