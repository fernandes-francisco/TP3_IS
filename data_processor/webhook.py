import json
import os
import boto3
from supabase import create_client, Client

SUPABASE_URL = os.environ.get('SUPABASE_URL')
SUPABASE_KEY = os.environ.get('SUPABASE_KEY')
SUPABASE_BUCKET = 'tp3_bucket' 
AWS_BUCKET = os.environ.get('S3_BUCKET_NAME') 

s3 = boto3.client('s3')
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

def lambda_handler(event, context):
    try:
        body = json.loads(event.get('body', '{}'))
        
        job_id = body.get('job_id')
        status = body.get('status')
        
        print(f"Webhook received for Job {job_id} with Status: {status}")
        
        if not job_id or status != "COMPLETED":
            return {
                'statusCode': 400,
                'body': json.dumps({"error": "Invalid Request or Job not complete"})
            }
        prefix = f"processed/{job_id}/"
        
        response = s3.list_objects_v2(Bucket=AWS_BUCKET, Prefix=prefix)
        
        if 'Contents' in response:
            objects_to_delete = [{'Key': obj['Key']} for obj in response['Contents']]
            
            s3.delete_objects(
                Bucket=AWS_BUCKET,
                Delete={'Objects': objects_to_delete}
            )
            print(f"Deleted {len(objects_to_delete)} chunks from S3.")
        else:
            print("No S3 chunks found (already cleaned?).")

        file_to_remove = "inprocessing/market.csv"
        try:
            res = supabase.storage.from_(SUPABASE_BUCKET).remove(file_to_remove)
            print(f"Removed source file from Supabase: {file_to_remove}")
        except Exception as e:
            print(f"Warning: Could not delete Supabase file: {str(e)}")

        return {
            'statusCode': 200,
            'body': json.dumps({"message": "Cleanup Successful", "job_id": job_id})
        }

    except Exception as e:
        print(f"Error: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps({"error": str(e)})
        }