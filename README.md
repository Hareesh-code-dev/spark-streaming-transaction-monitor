# Real-Time Transaction Monitoring Pipeline (Simulated Streaming)

## Overview
Simulates a real-time transaction feed and flags high-activity accounts using
Spark Structured Streaming on Databricks — a simplified velocity-based
fraud-detection pattern (flagging accounts with high transaction volume
concentrated in a short time window).

## Architecture
Data Generator (Python) → Landing Zone (Databricks Volume)
→ Spark Structured Streaming → Windowed Aggregation → Delta Lake

## Tech Stack
- Databricks (Free Edition, Serverless compute)
- PySpark Structured Streaming
- Delta Lake
- Python (Faker library for synthetic data)
- Unity Catalog Volumes (for file storage)

## What It Does
- Generates synthetic transaction events (account_id, amount, timestamp) over
  ~3 minutes to simulate a realistic time-spread feed
- Writes them as JSON files into a Databricks Volume, simulating an incoming stream
- Reads the folder as a stream with explicit schema enforcement
- Applies watermarking (2 minutes) to handle late-arriving data
- Computes 1-minute windowed aggregations per account (total amount, transaction count)
- Flags accounts exceeding a $5000 threshold within a single window
- Writes flagged results to a Delta table

## Sample Output


![Sample flagged output](sample%20output%20screenshot.png)



## Key Design Decisions
- **Watermarking**: allows the stream to tolerate late-arriving data without
  holding state in memory indefinitely. Set to 2 minutes as a realistic
  production-style value for late data tolerance, rather than tuned for
  fast local testing.
- **Windowing**: aggregates transactions into 1-minute buckets, closer to how
  real-time monitoring systems evaluate activity over rolling time periods.
- **File-based simulated source instead of Kafka**: kept scope realistic for a
  self-initiated learning project. The same streaming logic would apply to a
  Kafka/Event Hub source with minimal changes.
- **`trigger(availableNow=True)`**: used instead of continuous processing since
  Databricks Free Edition serverless compute doesn't support open-ended
  streaming triggers — this processes all currently available data and stops
  cleanly, which also fits a batch-style scheduled run pattern.
- **Delta Lake sink with append mode**: chosen over console output for
  persistence and downstream queryability; append mode is required for
  streaming writes to Delta.
- **Checkpointing**: ensures exactly-once processing — rerunning the job does
  not reprocess or duplicate already-handled files.

## Limitations / What I'd Do Differently in Production
- Replace the file-based source with Kafka or Azure Event Hub for real-time ingestion
- Add monitoring/alerting instead of relying on manual table checks
- Compare against each account's historical baseline rather than a single flat
  threshold, for more realistic fraud detection
- Partition output by date/account for scale
- Add automated data quality checks and checkpoint recovery testing

## How to Run
1. Run `generate_data.py` in a Databricks notebook to populate the Volume with
   sample transaction data (requires `pip install faker`)
2. Run `stream_job.py` in a separate notebook to process the stream, apply
   windowed aggregation, and write flagged accounts to a Delta table
