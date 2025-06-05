from collections import defaultdict
import time
from uuid import uuid4

class Store:
    def __init__(self):
        self.ingestions = {}
        self.batches = defaultdict(list)

    def create_ingestion(self, ingestion_id, ids, priority):
        created_at = time.time()
        batches = [ids[i:i+3] for i in range(0, len(ids), 3)]
        batch_list = []

        for b in batches:
            batch_id = str(uuid4())
            batch_list.append({"batch_id": batch_id, "ids": b, "status": "yet_to_start"})
            self.batches[batch_id] = {"ids": b, "status": "yet_to_start"}

        self.ingestions[ingestion_id] = {
            "ingestion_id": ingestion_id,
            "priority": priority,
            "created_at": created_at,
            "batches": batch_list
        }

    def get_next_batch(self):
        # Sort by priority then created_at
        priority_order = {"HIGH": 0, "MEDIUM": 1, "LOW": 2}
        all_batches = []

        for ing in self.ingestions.values():
            for batch in ing["batches"]:
                if batch["status"] == "yet_to_start":
                    all_batches.append((priority_order[ing["priority"]], ing["created_at"], batch["batch_id"], batch["ids"]))

        if not all_batches:
            return None, None

        all_batches.sort()
        batch = all_batches[0]
        return batch[2], batch[3]  # batch_id, ids

    def mark_batch_status(self, batch_id, status):
        if batch_id in self.batches:
            self.batches[batch_id]["status"] = status
            for ing in self.ingestions.values():
                for b in ing["batches"]:
                    if b["batch_id"] == batch_id:
                        b["status"] = status

    def get_ingestion_status(self, ingestion_id):
        ing = self.ingestions.get(ingestion_id)
        if not ing:
            return None

        statuses = [b["status"] for b in ing["batches"]]
        if all(s == "yet_to_start" for s in statuses):
            status = "yet_to_start"
        elif all(s == "completed" for s in statuses):
            status = "completed"
        else:
            status = "triggered"

        return {
            "ingestion_id": ingestion_id,
            "status": status,
            "batches": ing["batches"]
        }
