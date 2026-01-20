#DEPLOYED AS AN AWS LAMBDA FUNCTION
import os
import json
import uuid
import boto3
import redis
import requests
import csv
import codecs
from supabase import create_client, Client

SUPABASE_URL = os.environ.get('SUPABASE_URL')
SUPABASE_KEY = os.environ.get('SUPABASE_KEY')

SQS_QUEUE_URL = os.environ.get('SQS_QUEUE_URL')
REDIS_HOST = os.environ.get('REDIS_HOST')
REDIS_PORT = int(os.environ.get('REDIS_PORT', 6379))
REDIS_PASSWORD = os.environ.get('REDIS_PASSWORD', None)
BATCH_SIZE = 50 

sqs = boto3.client('sqs')
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

def get_redis_client():
    return redis.Redis(
        host=REDIS_HOST, 
        port=REDIS_PORT, 
        password=REDIS_PASSWORD, 
        decode_responses=True,
        socket_connect_timeout=5
    )

def lambda_handler(event, context):
    job_id = str(uuid.uuid4())
    print(f"Starting Job: {job_id}")

    try:
        
        if 'body' in event and isinstance(event['body'], str):
            payload = json.loads(event['body'])
        else:
            payload = event  

        bucket_name = payload.get('bucket')
        file_key = payload.get('key')

        print(f"Received Request -> Bucket: {bucket_name}, Key: {file_key}")

        if not bucket_name or not file_key:
            return {
                'statusCode': 400,
                'body': json.dumps({"error": "Missing 'bucket' or 'key' in request body"})
            }

        res = supabase.storage.from_(bucket_name).create_signed_url(file_key, 60)
        download_url = res['signedURL']

        response = requests.get(download_url, stream=True)
        response.raise_for_status()

        iterator = codecs.iterdecode(response.iter_lines(), 'utf-8')
        csv_reader = csv.DictReader(iterator)

        r = get_redis_client()
        r.set(f"job:{job_id}:processed", 0)
        r.expire(f"job:{job_id}:processed", 86400)

        batch = []
        chunk_counter = 0

        for row in csv_reader:
            batch.append(row)
            if len(batch) >= BATCH_SIZE:
                chunk_counter += 1
                send_batch_to_sqs(job_id, chunk_counter, batch)
                batch = [] 

        if batch:
            chunk_counter += 1
            send_batch_to_sqs(job_id, chunk_counter, batch)

        r.set(f"job:{job_id}:total", chunk_counter)
        r.expire(f"job:{job_id}:total", 86400)
        
        print(f"Job {job_id} successfully split into chunks.")
        
        new_path = f"inprocessing/{file_key}"
        try:
             supabase.storage.from_(bucket_name).move(file_key, new_path)
             print(f"Moved file to {new_path}")
        except Exception as move_err:
             print(f"Warning moving file: {move_err}")

        return {
            'statusCode': 200,
            'body': json.dumps({
                "message": f"Job {job_id} started.",
                "chunks": chunk_counter,
                "file": file_key
            })
        }

    except Exception as e:
        print(f"Error: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps({"error": str(e)})
        }

def send_batch_to_sqs(job_id, chunk_id, data):
    payload = {
        "job_id": job_id,
        "chunk_id": chunk_id,
        "data": data 
    }
    
    sqs.send_message(
        QueueUrl=SQS_QUEUE_URL,
        MessageBody=json.dumps(payload)
    )