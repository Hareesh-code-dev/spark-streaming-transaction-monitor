# Requires: pip install faker

import json, time, random, os
from faker import Faker
from datetime import datetime

fake = Faker()
output_dir = "/Volumes/workspace/default/data_stream"

account_ids = [f"ACC{1000+i}" for i in range(60)]

def generate_transaction():
    return {
        "account_id": random.choice(account_ids),
        "amount": round(random.uniform(10, 5000), 2),
        "timestamp": datetime.utcnow().isoformat()
    }

for i in range(60):  # writes 60 batches over ~3 minutes
    txns = [generate_transaction() for _ in range(random.randint(1, 5))]
    with open(f"{output_dir}/txn_{i}.json", "w") as f:
        for t in txns:
            f.write(json.dumps(t) + "\n")
    print(f"Wrote batch {i} with {len(txns)} transactions")
    time.sleep(3)



