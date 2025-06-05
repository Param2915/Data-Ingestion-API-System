import time

def simulate_external_api(id):
    time.sleep(0.1)
    return {"id": id, "data": "processed"}
