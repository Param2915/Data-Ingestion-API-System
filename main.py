from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import List
from uuid import uuid4
from store import Store
from worker import add_ingestion_request

app = FastAPI()
store = Store()

class IngestRequest(BaseModel):
    ids: List[int]
    priority: str  # "HIGH", "MEDIUM", "LOW"

@app.post("/ingest")
def ingest(data: IngestRequest):
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
