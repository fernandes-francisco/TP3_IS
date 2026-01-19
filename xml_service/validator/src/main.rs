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

fn validate_schema(xml: &str) -> bool {
    let mut reader = Reader::from_str(xml);
    reader.trim_text(true);

    let mut buf = Vec::new();
    
    // Validation Flags
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
                        // Check if required attributes exist
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
            Ok(Event::Eof) => break, // End of file
            Err(_) => return false, // XML Syntax Error
            _ => (),
        }
        buf.clear();
    }


    
    //Must have <MarketReport JobID="...">
    if !has_root || !has_job_id {
        eprintln!("Validation Failed: Missing Root or JobID attribute");
        return false;
    }

    //Must contain data
    if asset_count == 0 {
        eprintln!("Validation Failed: XML is empty (0 Assets)");
        return false;
    }

    //Must contain the Enriched Data
    if !has_fundamentals {
        eprintln!("Validation Failed: Missing <FundamentalData> section");
        return false;
    }

    //Must contain the Hierarchy 
    if !has_daily_data {
        eprintln!("Validation Failed: Missing <DailyData> hierarchy");
        return false;
    }

    true
}

#[tokio::main]
async fn main() -> Result<()> {
    // 1. Setup
    let redis_host = env::var("REDIS_HOST").context("REDIS_HOST missing")?;
    let redis_port = env::var("REDIS_PORT").unwrap_or("6379".to_string());
    
    let redis_url = format!("redis://{}:{}", redis_host, redis_port);
    let client = redis::Client::open(redis_url)?;
    let mut con = client.get_tokio_connection().await?;

    println!("Validator Service Started. Listening on 'queue:xml_validation'...");

    loop {
        // 2. Receive Message
        let result: Option<(String, String)> = con.blpop("queue:xml_validation", 0.0).await?;
        
        if let Some((_, json_str)) = result {
            // 3. Parse JSON wrapper
            let msg: XmlMsg = match serde_json::from_str(&json_str) {
                Ok(m) => m,
                Err(e) => {
                    eprintln!("JSON Error: {}", e);
                    continue;
                }
            };

            println!("Inspecting Job {} - Chunk {}", msg.job_id, msg.chunk_id);

            // 4. Run Logic
            if validate_schema(&msg.xml_content) {
                
                let _: () = con.rpush("queue:db_persistence", json_str).await?;
                println!("Valid. Pushed to DB Queue.");
            } else {
                //INVALID: Send to Dead Letter Queue
                eprintln!("REJECTED Job {} Chunk {}. Sending to Error Queue.", msg.job_id, msg.chunk_id);
                let _: () = con.rpush("queue:xml_errors", json_str).await?;
            }
        }
    }
}