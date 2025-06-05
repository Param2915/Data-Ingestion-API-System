from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import List
from uuid import uuid4
import os
from store import Store
from worker import add_ingestion_request, start_worker

app = FastAPI(title="Data Ingestion API System", version="1.0.0")
store = Store()

# Start the worker thread when the app starts
start_worker(store)

class IngestRequest(BaseModel):
    ids: List[int]
    priority: str  # "HIGH", "MEDIUM", "LOW"

@app.get("/")
def root():
    return {"message": "Data Ingestion API System is running!", "status": "healthy"}

@app.post("/ingest")
def ingest(data: IngestRequest):
    # Validate priority
    if data.priority not in ["HIGH", "MEDIUM", "LOW"]:
        raise HTTPException(status_code=400, detail="Priority must be HIGH, MEDIUM, or LOW")
    
    ingestion_id = str(uuid4())
    store.create_ingestion(ingestion_id, data.ids, data.priority)
    add_ingestion_request(ingestion_id, data.ids, data.priority, store)
    return {"ingestion_id": ingestion_id}

@app.get("/status/{ingestion_id}")
def status(ingestion_id: str):
    status_data = store.get_ingestion_status(ingestion_id)
    if status_data is None:
        raise HTTPException(status_code=404, detail="Ingestion ID not found")
    return status_data

@app.get("/health")
def health_check():
    return {"status": "healthy", "message": "API is running"}

if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)