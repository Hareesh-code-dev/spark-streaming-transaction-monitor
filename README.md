# Real-Time Transaction Monitoring Pipeline (Simulated Streaming)

## Overview
Simulates a real-time transaction feed and flags high-activity accounts using
Spark Structured Streaming on Databricks — similar to a fraud-detection pattern.

## Architecture
Data Generator (Python) → Landing Zone (Databricks Volume)
→ Spark Structured Streaming → Windowed Aggregation → Console Output

## Tech Stack
- Databricks (Free Edition, Serverless compute)
- PySpark Structured Streaming
- Python (Faker library for synthetic data)
- Unity Catalog Volumes (for file storage)

## What It Does
- Generates synthetic transaction events (account_id, amount, timestamp)
- Writes them as JSON files into a Databricks Volume, simulating an incoming feed
- Reads the folder as a stream with explicit schema enforcement
- Applies watermarking (2 min) to handle late-arriving data
- Computes 1-minute windowed aggregations per account (total amount, transaction count)
- Flags accounts exceeding a $5000 threshold within a window

## Key Design Decisions
- **Watermarking**: allows the stream to handle data that arrives late, without holding
  state in memory indefinitely.
- **Windowing**: aggregates transactions into 1-minute buckets rather than across the
  whole stream, which is closer to how real-time monitoring systems work.
- **File-based simulated source instead of Kafka**: kept scope realistic for a
  self-initiated learning project; the same streaming logic would apply to a
  Kafka/Event Hub source with minimal changes.
- **`trigger(availableNow=True)`**: used instead of continuous processing since
  Databricks Free Edition serverless compute doesn't support open-ended streaming
  triggers — this processes all currently available data and stops cleanly.

## Limitations / What I'd Do Differently in Production
- Replace the file-based source with Kafka or Azure Event Hub for real-time ingestion
- Write output to Delta Lake instead of console, for persistence and downstream querying
- Add monitoring/alerting instead of printing to console
- Partition output by date/account for scale
- Add checkpoint recovery testing

## How to Run
1. Run `generate_data.py` in a Databricks notebook to populate the Volume with sample data
2. Run `stream_job.py` in a separate notebook to process the stream and view flagged accounts
