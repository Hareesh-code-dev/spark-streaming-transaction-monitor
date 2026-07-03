import json, time, random, os
from faker import Faker
from datetime import datetime

fake = Faker()
output_dir = "data_stream"
os.makedirs(output_dir, exist_ok=True)

account_ids = [f"ACC{1000+i}" for i in range(20)]

def generate_transaction():
    return {
        "account_id": random.choice(account_ids),
        "amount": round(random.uniform(10, 5000), 2),
        "timestamp": datetime.utcnow().isoformat()
    }

i = 0
while True:
    txns = [generate_transaction() for _ in range(random.randint(1, 5))]
    with open(f"{output_dir}/txn_{i}.json", "w") as f:
        for t in txns:
            f.write(json.dumps(t) + "\n")
    print(f"Wrote batch {i} with {len(txns)} transactions")
    i += 1
    time.sleep(3)
