import threading
import time
from store import Store
from utils import simulate_external_api

queue_lock = threading.Lock()
task_queue = []

def worker_loop(store):
    while True:
        with queue_lock:
            if task_queue:
                ingestion_id, batch_id, ids = task_queue.pop(0)
            else:
                time.sleep(1)
                continue

        store.mark_batch_status(batch_id, "triggered")
        for id_ in ids:
            simulate_external_api(id_)
        store.mark_batch_status(batch_id, "completed")
        time.sleep(5)  # Respect 1 batch per 5 seconds

def add_ingestion_request(ingestion_id, ids, priority, store):
    def enqueue_batches():
        while True:
            batch_id, batch_ids = store.get_next_batch()
            if batch_id is None:
                break
            with queue_lock:
                task_queue.append((ingestion_id, batch_id, batch_ids))
        return

    threading.Thread(target=enqueue_batches, daemon=True).start()

threading.Thread(target=worker_loop, args=(Store(),), daemon=True).start()
