# Distributed Log Analytics Pipeline
Real-time log analytics pipeline built with Kafka, Spark Structured Streaming, HBase, and Hive. The project demonstrates ingesting JSON application logs, performing streaming aggregations, and persisting results for both low-latency access and analytical queries.

## Architecture & Data Flow
1. **Kafka Producers** emit JSON logs (`timestamp`, `service_name`, `level`, `message`, `host`) to a Kafka topic.
2. **Kafka Brokers** buffer and replicate events for durability and back-pressure handling.
3. **Spark Structured Streaming** reads from Kafka, parses JSON, and aggregates counts by log level over 1-minute windows.
4. Aggregates are written to:
   - **HBase** for fast lookups (simulated client writes).
   - **Hive** for long-term analytical queries.
5. Operators can tail events using the provided Kafka consumer for validation and troubleshooting.

See `architecture/architecture.md` for a detailed breakdown.

## Repository Layout
- `kafka/producer.py` – Simulated log producer publishing JSON events to Kafka.
- `kafka/consumer.py` – Consumer for tailing logs from Kafka.
- `spark/spark_streaming_job.py` – Structured Streaming job with log-level aggregations and sinks to HBase/Hive.
- `hive/create_tables.hql` – Hive DDL for external, partitioned tables.
- `hbase/hbase_schema.txt` – HBase table, column family, and row-key design.
- `config/application.conf` – Centralized endpoints and table names.
- `scripts/start_pipeline.sh` – Orchestration script to set up Hive and launch streaming and Kafka processes.
- `architecture/architecture.md` – Architecture description.

## Prerequisites
- Kafka cluster reachable at the configured `bootstrap.servers`.
- Spark 3.x with Hive support enabled.
- Hive metastore and warehouse directory accessible to Spark.
- (Optional) HBase cluster if replacing the simulated sink with a real client.
- Python 3.9+ with `kafka-python` and `pyspark` available on the runtime classpath.

## Setup
1. Install Python dependencies (Spark provides `pyspark`):
   ```bash
   pip install kafka-python
   ```
2. Create Hive tables:
   ```bash
   hive -f hive/create_tables.hql
   ```
3. Verify Kafka topic (default: `logs`) exists or create it.

## Running the Pipeline
Use the orchestration script (adjust environment variables as needed):
```bash
export BOOTSTRAP=localhost:9092
export TOPIC=logs
export CHECKPOINT=/tmp/spark-checkpoints/logs
export HBASE_TABLE=log_analytics:log_levels
export HIVE_DB=analytics
export HIVE_TABLE=log_level_counts
export INTERVAL=1.0

./scripts/start_pipeline.sh
```

Key commands inside the script:
- `hive -f hive/create_tables.hql` to prepare Hive metadata.
- `spark-submit spark/spark_streaming_job.py ...` to start the streaming job.
- `python kafka/consumer.py ...` to tail events.
- `python kafka/producer.py ...` to emit simulated logs.

## Tech Stack
- **Apache Kafka** for log ingestion and buffering.
- **Apache Spark Structured Streaming** for real-time processing.
- **Apache Hive** for analytical storage and SQL access.
- **Apache HBase** for low-latency lookups (simulated client in this repo).
- **Python** (`kafka-python`, `pyspark`) for producers, consumers, and the streaming job.
