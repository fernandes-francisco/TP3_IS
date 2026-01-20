use anyhow::{Context, Result};
use aws_sdk_s3::Client as S3Client;
use chrono::Utc;
use redis::AsyncCommands;
use serde::{Deserialize, Serialize};
use std::env;

#[derive(Debug, Deserialize)]
struct CsvRow { 
    #[serde(rename = "Ticker")]
    ticker: String,
    
    #[serde(rename = "Nome")]
    name: String,
    
    #[serde(rename = "Sector")]
    sector: String,

    #[serde(rename = "PriceSMA_EUR")]
    price_sma: String,
    #[serde(rename = "VolumeAvg")]
    volume_avg: String,

    #[serde(rename = "Market Cap")]
    market_cap: Option<String>,
    #[serde(rename = "PE Ratio (TTM)")]
    pe_ratio: Option<String>,
    #[serde(rename = "EPS (TTM)")]
    eps: Option<String>,
    #[serde(rename = "Open")]
    open_price: Option<String>,
    #[serde(rename = "Previous Close")]
    prev_close: Option<String>,
    #[serde(rename = "Beta (5Y Monthly)")]
    beta: Option<String>,

    Price_1: Option<String>, Volume_1: Option<String>,
    Price_2: Option<String>, Volume_2: Option<String>,
    Price_3: Option<String>, Volume_3: Option<String>,
    Price_4: Option<String>, Volume_4: Option<String>,
    Price_5: Option<String>, Volume_5: Option<String>,
    Price_6: Option<String>, Volume_6: Option<String>,
    Price_7: Option<String>, Volume_7: Option<String>,
    Price_8: Option<String>, Volume_8: Option<String>,
    Price_9: Option<String>, Volume_9: Option<String>,
    Price_10: Option<String>, Volume_10: Option<String>,
}

#[derive(Debug, Serialize)]
#[serde(rename = "MarketReport")]
struct MarketReport {
    #[serde(rename = "@JobID")]
    job_id: String,
    #[serde(rename = "@ChunkID")]
    chunk_id: u32,
    #[serde(rename = "@GeneratedAt")]
    generated_at: String,
    #[serde(rename = "Asset")]
    assets: Vec<Asset>,
}

#[derive(Debug, Serialize)]
struct Asset {
    #[serde(rename = "@Ticker")]
    ticker: String,
    #[serde(rename = "Identification")]
    identification: Identification,
    #[serde(rename = "FundamentalData")]
    fundamental_data: FundamentalData,
    #[serde(rename = "Indicators")]
    indicators: Indicators,
    #[serde(rename = "DailyData")]
    daily_data: DailyDataWrapper,
}

#[derive(Debug, Serialize)]
struct Identification {
    #[serde(rename = "Name")]
    name: String,
    #[serde(rename = "Sector")]
    sector: String,
}

#[derive(Debug, Serialize)]
struct FundamentalData {
    MarketCap: String,
    PERatio: String,
    EPS: String,
    OpenPrice: String,
    PrevClose: String,
    Beta: String,
}

#[derive(Debug, Serialize)]
struct Indicators {
    #[serde(rename = "PriceSMA")]
    price_sma: String,
    #[serde(rename = "AverageVolume")]
    avg_volume: String,
}

#[derive(Debug, Serialize)]
struct DailyDataWrapper {
    #[serde(rename = "Day")]
    days: Vec<Day>,
}

#[derive(Debug, Serialize)]
struct Day {
    #[serde(rename = "@index")]
    index: u8,
    #[serde(rename = "ClosingPrice")]
    closing_price: Price,
    #[serde(rename = "Volume")]
    volume: String,
}

#[derive(Debug, Serialize)]
struct Price {
    #[serde(rename = "@Currency")]
    currency: String,
    #[serde(rename = "$value")]
    value: String,
}

#[derive(Deserialize, Debug)]
struct InputMsg {
    job_id: String,
    s3_bucket: String,
    s3_key: String,
    chunk_id: u32,
}

#[derive(Serialize, Deserialize, Debug)]
struct XmlMsg {
    job_id: String,
    chunk_id: u32,
    xml_content: String,
}

fn convert_to_xml(csv_data: String, job_id: &str, chunk_id: u32) -> Result<String> {
    let mut reader = csv::Reader::from_reader(csv_data.as_bytes());
    let mut assets = Vec::new();
    for result in reader.deserialize() {
        let row: CsvRow = result?; 

        let fundamentals = FundamentalData {
            MarketCap: row.market_cap.unwrap_or("Unknown".into()),
            PERatio: row.pe_ratio.unwrap_or("".into()),
            EPS: row.eps.unwrap_or("".into()),
            OpenPrice: row.open_price.unwrap_or("".into()),
            PrevClose: row.prev_close.unwrap_or("".into()),
            Beta: row.beta.unwrap_or("".into()),
        };

        let mut days = Vec::new();
        
        let mut add_day = |idx: u8, p: Option<String>, v: Option<String>| {
            if let Some(price) = p {
                if !price.is_empty() {
                    days.push(Day {
                        index: idx,
                        closing_price: Price { currency: "EUR".into(), value: price },
                        volume: v.unwrap_or("0".into()),
                    });
                }
            }
        };

        add_day(1, row.Price_1, row.Volume_1);
        add_day(2, row.Price_2, row.Volume_2);
        add_day(3, row.Price_3, row.Volume_3);
        add_day(4, row.Price_4, row.Volume_4);
        add_day(5, row.Price_5, row.Volume_5);
        add_day(6, row.Price_6, row.Volume_6);
        add_day(7, row.Price_7, row.Volume_7);
        add_day(8, row.Price_8, row.Volume_8);
        add_day(9, row.Price_9, row.Volume_9);
        add_day(10, row.Price_10, row.Volume_10);

        assets.push(Asset {
            ticker: row.ticker,
            identification: Identification {
                name: row.name,
                sector: row.sector,
            },
            fundamental_data: fundamentals,
            indicators: Indicators {
                price_sma: row.price_sma,
                avg_volume: row.volume_avg,
            },
            daily_data: DailyDataWrapper { days },
        });
    }

    let report = MarketReport {
        job_id: job_id.to_string(),
        chunk_id,
        generated_at: Utc::now().to_rfc3339(),
        assets,
    };

    let mut xml_string = String::new();
    let mut serializer = quick_xml::se::Serializer::new(&mut xml_string);
    serializer.indent(' ', 2);
    
    report.serialize(serializer)?;
    
    Ok(xml_string)
}

#[tokio::main]
async fn main() -> Result<()> {
    let redis_host = env::var("REDIS_HOST").context("REDIS_HOST env var missing")?;
    let redis_port = env::var("REDIS_PORT").unwrap_or("6379".to_string());
    let redis_password = env::var("REDIS_PASSWORD").unwrap_or_default();
    
    let aws_config = aws_config::load_from_env().await;
    let s3_client = S3Client::new(&aws_config);

    let redis_url = if !redis_password.is_empty() {
        format!("redis://:{}@{}:{}", redis_password, redis_host, redis_port)
    } else {
        format!("redis://{}:{}", redis_host, redis_port)
    };

    let client = redis::Client::open(redis_url)?;
    let mut con = client.get_tokio_connection().await?;

    println!("Converter Service Started. Listening on 'queue:csv_processing'...");

    loop {
        let result: Option<(String, String)> = con.blpop("queue:csv_processing", 0.0).await?;
        
        if let Some((_, json_str)) = result {
            let input: InputMsg = match serde_json::from_str(&json_str) {
                Ok(msg) => msg,
                Err(e) => {
                    eprintln!("Failed to parse Redis message: {}", e);
                    continue;
                }
            };
            
            println!("Processing Job {} - Chunk {}", input.job_id, input.chunk_id);

            match process_job(&s3_client, &input).await {
                Ok(xml_output) => {
                    let output_msg = XmlMsg {
                        job_id: input.job_id,
                        chunk_id: input.chunk_id,
                        xml_content: xml_output,
                    };

                    let output_json = serde_json::to_string(&output_msg)?;
                    let _: () = con.rpush("queue:xml_validation", output_json).await?;
                    println!("Converted & Pushed to Validation Queue.");
                },
                Err(e) => {
                    eprintln!("Failed to convert chunk: {}", e);
                }
            }
        }
    }
}

async fn process_job(s3: &S3Client, input: &InputMsg) -> Result<String> {
    let obj = s3.get_object()
        .bucket(&input.s3_bucket)
        .key(&input.s3_key)
        .send()
        .await
        .context("Failed to download from S3")?;

    let data = obj.body.collect().await?.into_bytes();
    let csv_str = String::from_utf8(data.to_vec())?;

    let xml_str = convert_to_xml(csv_str, &input.job_id, input.chunk_id)?;
    Ok(xml_str)
}