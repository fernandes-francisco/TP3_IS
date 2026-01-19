use anyhow::{Context, Result};
use redis::AsyncCommands;
use serde::{Deserialize, Serialize};
use std::env;
use tokio_postgres::NoTls;

#[derive(Serialize, Deserialize, Debug)]
struct XmlMsg {
    job_id: String,
    chunk_id: u32,
    xml_content: String,
}

#[derive(Serialize)]
struct WebhookPayload {
    job_id: String,
    status: String,
}


#[tokio::main]
async fn main() -> Result<()> {
    // 1. Configuration
    let redis_host = env::var("REDIS_HOST").context("REDIS_HOST missing")?;
    let redis_port = env::var("REDIS_PORT").unwrap_or("6379".to_string());
    let db_url = env::var("DATABASE_URL").context("DATABASE_URL missing")?;
    let webhook_url = env::var("WEBHOOK_URL").context("WEBHOOK_URL missing")?;

    // 2. Connect to Redis
    let redis_url = format!("redis://{}:{}", redis_host, redis_port);
    let redis_client = redis::Client::open(redis_url)?;
    let mut redis_con = redis_client.get_tokio_connection().await?;

    // 3. Connect to PostgreSQL 
    let (db_client, connection) = tokio_postgres::connect(&db_url, NoTls).await?;

    // Spawn the connection handler in the background
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("Database connection error: {}", e);
        }
    });

    // HTTP Client for Webhook
    let http_client = reqwest::Client::new();

    println!("Persister Service Started. Listening on 'queue:db_persistence'...");

    loop {
        // 4. Blocking Pop
        let result: Option<(String, String)> = redis_con.blpop("queue:db_persistence", 0.0).await?;
        
        if let Some((_, json_str)) = result {
            let msg: XmlMsg = match serde_json::from_str(&json_str) {
                Ok(m) => m,
                Err(e) => {
                    eprintln!("JSON Error: {}", e);
                    continue;
                }
            };

            println!("Persisting Job {} - Chunk {}", msg.job_id, msg.chunk_id);

            // 5. INSERT into DB 
            let insert_stmt = "INSERT INTO xml_storage (job_id, chunk_id, xml_documento) VALUES ($1, $2, $3::xml)";
            
            match db_client.execute(
                insert_stmt,
                &[&msg.job_id, &(msg.chunk_id as i32), &msg.xml_content],
            ).await {
                Ok(_) => {
                    println!("Saved Chunk {} to DB.", msg.chunk_id);
                    // 6. Check Job Completion
                    check_completion(&mut redis_con, &http_client, &msg.job_id, &webhook_url).await;
                },
                Err(e) => {
                    eprintln!(" DB Insert Failed: {}", e);
                }
            }
        }
    }
}

async fn check_completion(
    con: &mut redis::aio::Connection, 
    http: &reqwest::Client,
    job_id: &str, 
    webhook_url: &str
) {
    let processed_key = format!("job:{}:processed", job_id);
    let total_key = format!("job:{}:total", job_id);

    // Atomic Increment
    let processed: i32 = match con.incr(&processed_key, 1).await {
        Ok(v) => v,
        Err(_) => return,
    };

    // Get Total 
    
    let total_str: Option<String> = con.get(&total_key).await.unwrap_or(None);
    let total: i32 = total_str.unwrap_or("999999".to_string()).parse().unwrap_or(999999);

    println!("Progress Job {}: {} / {}", job_id, processed, total);

    if processed >= total {
        println!("JOB {} FINISHED! Triggering Webhook...", job_id);
        
        let payload = WebhookPayload {
            job_id: job_id.to_string(),
            status: "COMPLETED".to_string(),
        };

        match http.post(webhook_url).json(&payload).send().await {
            Ok(_) => println!("Webhook sent successfully."),
            Err(e) => eprintln!("Failed to call webhook: {}", e),
        }
        
        // Cleanup Redis keys
        let _: () = con.del(&[processed_key, total_key]).await.unwrap_or(());
    }
}