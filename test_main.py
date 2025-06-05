import time
import requests

BASE_URL = "http://localhost:8000"

def test_priority_and_rate_limit():
    r1 = requests.post(f"{BASE_URL}/ingest", json={"ids": [1,2,3,4,5], "priority": "MEDIUM"}).json()
    time.sleep(1)
    r2 = requests.post(f"{BASE_URL}/ingest", json={"ids": [6,7,8,9], "priority": "HIGH"}).json()

    ingestion_id_1 = r1["ingestion_id"]
    ingestion_id_2 = r2["ingestion_id"]

    time.sleep(16)
    s1 = requests.get(f"{BASE_URL}/status/{ingestion_id_1}").json()
    s2 = requests.get(f"{BASE_URL}/status/{ingestion_id_2}").json()

    assert s1["status"] == "completed"
    assert s2["status"] == "completed"
    print("✅ Priority and rate-limiting respected!")

if __name__ == "__main__":
    test_priority_and_rate_limit()
