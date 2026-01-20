use anyhow::{Context, Result};
use quick_xml::events::Event;
use quick_xml::Reader;
use redis::AsyncCommands;
use serde::{Deserialize, Serialize};
use std::env;

#[derive(Serialize, Deserialize, Debug)]
struct XmlMsg {
    job_id: String,
    chunk_id: u32,
    xml_content: String,
}

#[derive(Serialize, Deserialize, Debug)]
struct PipelineMsg {
    job_id: String,
    chunk_id: u32,
    xml_content: String,
    status: String, 
}

fn validate_schema(xml: &str) -> bool {
    let mut reader = Reader::from_str(xml);
    reader.trim_text(true);

    let mut buf = Vec::new();
    
    let mut has_root = false;
    let mut has_job_id = false;
    let mut asset_count = 0;
    let mut has_fundamentals = false;
    let mut has_daily_data = false;

    loop {
        match reader.read_event_into(&mut buf) {
            Ok(Event::Start(e)) => {
                match e.name().as_ref() {
                    b"MarketReport" => {
                        has_root = true;
                        if e.try_get_attribute("JobID").is_ok() && 
                           e.try_get_attribute("GeneratedAt").is_ok() {
                            has_job_id = true;
                        }
                    },
                    b"Asset" => asset_count += 1,
                    b"FundamentalData" => has_fundamentals = true,
                    b"DailyData" => has_daily_data = true,
                    _ => (),
                }
            }
            Ok(Event::Eof) => break, 
            Err(_) => return false, 
            _ => (),
        }
        buf.clear();
    }

    if !has_root || !has_job_id {
        return false;
    }

    if asset_count == 0 {
        return false;
    }

    if !has_fundamentals {
        return false;
    }

    if !has_daily_data {
        return false;
    }

    true
}

#[tokio::main]
async fn main() -> Result<()> {
    let redis_host = env::var("REDIS_HOST").context("REDIS_HOST missing")?;
    let redis_port = env::var("REDIS_PORT").unwrap_or("6379".to_string());
    let redis_password = env::var("REDIS_PASSWORD").unwrap_or_default();
    
    let redis_url = if !redis_password.is_empty() {
        format!("redis://:{}@{}:{}", redis_password, redis_host, redis_port)
    } else {
        format!("redis://{}:{}", redis_host, redis_port)
    };

    let client = redis::Client::open(redis_url)?;
    let mut con = client.get_tokio_connection().await?;

    println!("Validator Service Started (Manual Logic). Listening...");

    loop {
        let result: Option<(String, String)> = con.blpop("queue:xml_validation", 0.0).await?;
        
        if let Some((_, json_str)) = result {
            let in_msg: XmlMsg = match serde_json::from_str(&json_str) {
                Ok(m) => m,
                Err(e) => {
                    eprintln!("JSON Error: {}", e);
                    continue;
                }
            };

            let is_valid = validate_schema(&in_msg.xml_content);
            let status = if is_valid { "OK".to_string() } else { "ERRO_VALIDACAO".to_string() };

            if !is_valid {
                eprintln!("Validation Failed for Job {} Chunk {}", in_msg.job_id, in_msg.chunk_id);
            } else {
                println!("Validation OK for Job {} Chunk {}", in_msg.job_id, in_msg.chunk_id);
            }

            let out_msg = PipelineMsg {
                job_id: in_msg.job_id,
                chunk_id: in_msg.chunk_id,
                xml_content: in_msg.xml_content,
                status,
            };

            let json_out = serde_json::to_string(&out_msg)?;
            let _: () = con.rpush("queue:db_persistence", json_out).await?;
        }
    }
}