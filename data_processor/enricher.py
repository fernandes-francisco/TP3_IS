import json
import os
import boto3
import redis
import requests
import io
import csv
import asyncio
import numpy as np
import yfinance as yf
import pandas as pd

REDIS_HOST = os.environ.get('REDIS_HOST')
REDIS_PORT = int(os.environ.get('REDIS_PORT', 6379))
REDIS_PASSWORD = os.environ.get('REDIS_PASSWORD', None)
S3_BUCKET_NAME = os.environ.get('S3_BUCKET_NAME')
COBOL_HOST = os.environ.get('COBOL_HOST')
COBOL_PORT = int(os.environ.get('COBOL_PORT', 8080))

s3 = boto3.client('s3')

def get_redis_client():
    return redis.Redis(
        host=REDIS_HOST, port=REDIS_PORT, password=REDIS_PASSWORD, 
        decode_responses=True, socket_connect_timeout=5
    )

def get_eur_rate():
    try:
        url = "http://www.floatrates.com/daily/usd.json"
        resp = requests.get(url, timeout=5)
        if resp.status_code == 200:
            return float(resp.json()['eur']['rate'])
    except Exception:
        pass
    return 0.95

async def get_cobol_sma(prices: list[float]):
    if not prices:
        return 0.0
    try:
        payload = ";".join(f"{p:.2f}" for p in prices) + "\n"
        reader, writer = await asyncio.open_connection(COBOL_HOST, COBOL_PORT)
        writer.write(payload.encode())
        await writer.drain()
        data = await reader.read(1024)
        writer.close()
        await writer.wait_closed()
        
        resp = data.decode().strip()
        return float(resp) if resp else 0.0
    except Exception:
        return float(np.mean(prices))
        
def get_ticker_sector(ticker):
    try:
        t = yf.Ticker(ticker)
        return t.info.get('sector', 'Unknown')
    except Exception:
        return 'Unknown'

async def process_chunk(record, redis_client):
    payload = json.loads(record['body'])
    job_id = payload['job_id']
    chunk_id = payload['chunk_id']
    raw_data = payload['data']
    
    eur_rate = get_eur_rate()

    tickers = [row.get('Ticker') for row in raw_data if row.get('Ticker')]
    unique_tickers = list(set(tickers))
    
    try:
        market_data = yf.download(unique_tickers, period="1mo", group_by="ticker", threads=True, progress=False)
    except:
        market_data = pd.DataFrame()

    tasks = []
    processed_rows = []

    for row in raw_data:
        ticker = row.get('Ticker')
        last_10_prices_eur = []
        last_10_vol = []
        vol_avg = 0.0
        if ticker:
            row['Sector'] = get_ticker_sector(ticker)
        else:
            row['Sector'] = 'Unknown'
        if ticker and not market_data.empty:
            try:
                if len(unique_tickers) > 1:
                    df = market_data[ticker]
                else:
                    df = market_data
                
                recent = df.tail(10)
                
                closes = recent['Close'].tolist()
                last_10_prices_eur = [c * eur_rate for c in closes]
                
                vols = recent['Volume'].tolist()
                last_10_vol = [float(v) for v in vols]
                
                if last_10_vol:
                    vol_avg = float(np.mean(last_10_vol))
            except:
                pass

        processed_rows.append({
            "basic_data": row,
            "prices": last_10_prices_eur,
            "volumes": last_10_vol,
            "vol_avg": vol_avg
        })
        
        tasks.append(get_cobol_sma(last_10_prices_eur))

    cobol_results = await asyncio.gather(*tasks)

    final_output = []
    headers = set()

    for i, item in enumerate(processed_rows):
        row = item['basic_data']
        prices = item['prices']
        volumes = item['volumes']
        
        for k in range(10): 
            col_num = k + 1
            
            if k < len(prices):
                row[f"Price_{col_num}"] = f"{prices[k]:.2f}"
            else:
                row[f"Price_{col_num}"] = ""
            
            if k < len(volumes):
                row[f"Volume_{col_num}"] = f"{volumes[k]:.0f}"
            else:
                row[f"Volume_{col_num}"] = ""

        row['VolumeAvg'] = f"{item['vol_avg']:.0f}"
        row['PriceSMA_EUR'] = f"{cobol_results[i]:.2f}"
        row['CurrencyUsed'] = "EUR"
        
        final_output.append(row)
        headers.update(row.keys())

    csv_buffer = io.StringIO()
    fieldnames = sorted(list(headers))
    
    writer = csv.DictWriter(csv_buffer, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerows(final_output)
    
    s3_key = f"processed/{job_id}/chunk_{chunk_id}.csv"
    
    s3.put_object(
        Bucket=S3_BUCKET_NAME,
        Key=s3_key,
        Body=csv_buffer.getvalue(),
        ContentType='text/csv'
    )

    msg = json.dumps({
        "job_id": job_id, 
        "s3_bucket": S3_BUCKET_NAME, 
        "s3_key": s3_key, 
        "chunk_id": chunk_id
    })
    redis_client.rpush("queue:xml_processing", msg)

def lambda_handler(event, context):
    r = get_redis_client()
    loop = asyncio.get_event_loop()
    for record in event['Records']:
        loop.run_until_complete(process_chunk(record, r))
    return {'statusCode': 200, 'body': 'Batch Processed'}