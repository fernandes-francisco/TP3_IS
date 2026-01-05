from supabase import create_client, Client
import os
from dotenv import load_dotenv
from fastapi import FastAPI, Request, HTTPException
load_dotenv()

url: str = os.environ.get("SUPABASE_URL")
key: str = os.environ.get("SUPABASE_KEY")
supabase: Client = create_client(url, key)

app = FastAPI()

@app.post("/webhook")
async def notification(request: Request):
    try:
        payload = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid JSON")
    if payload.get("type") == "INSERT":
        record = payload.get("record", {})

        file_path = record.get("name")
        bucket_id = record.get("bucket_id")
        try:
            response = supabase.storage.from_(bucket_id).download(file_path)
            local_filename = f"{file_path}"
            with open(local_filename, "wb") as f:
                f.write(response) 
            supabase.storage.from_(bucket_id).remove([file_path])
        except Exception as e:
            return {"status": "error", "message": str(e)}
        
@app.post("/webhook2")
async def notification(request: Request):
    try:
        payload = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid JSON")
    status = payload.get("status")
    return status