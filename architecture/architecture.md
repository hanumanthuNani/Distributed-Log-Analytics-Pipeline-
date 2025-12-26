# Distributed Log Analytics Pipeline Architecture

## Components
- **Producers (Kafka)**: Application log emitters send JSON log events (`timestamp`, `service_name`, `level`, `message`, `host`) to a Kafka topic for durable buffering.
- **Kafka Brokers**: Persist the log stream and expose it to downstream consumers with strong ordering guarantees per partition.
- **Spark Structured Streaming**: Reads from Kafka, parses JSON payloads, and performs streaming aggregations such as log-level counts over sliding windows.
- **HBase**: Low-latency lookup store for recent aggregates and raw events keyed by time and host. Spark writes batch results via a simulated HBase client to illustrate put semantics.
- **Hive**: Long-term analytical store. Spark writes aggregated results into Hive tables so analysts can query historical trends with SQL.
- **Consumers/Debugging Tools**: Simple Kafka consumer for verifying event flow during development.
- **Orchestration Script**: `scripts/start_pipeline.sh` coordinates Hive setup, Spark streaming job launch, and Kafka producer/consumer startup.

## Data Flow
1. Applications generate JSON logs and publish them to Kafka via `kafka/producer.py`.
2. Kafka stores events in partitions for durability and back-pressure handling.
3. Spark Structured Streaming job (`spark/spark_streaming_job.py`) reads from Kafka, parses JSON, and performs aggregations (counts by log level within a 1-minute window).
4. Aggregated metrics are written to:
   - **HBase**: Simulated client writes key-value puts for fast retrieval by window and level.
   - **Hive**: Inserts aggregated results into partitioned Hive tables for SQL-based analytics.
5. Operators can tail events using `kafka/consumer.py` to validate ingest during development or troubleshooting.

## Deployment Notes
- Kafka brokers, Spark, HBase, and Hive should run on the same network; configure endpoints in `config/application.conf`.
- Hive tables are created via `hive/create_tables.hql` prior to starting the streaming job.
- The provided HBase interactions are implemented as a Python stub to illustrate the write pattern; replace with real client code (e.g., HappyBase) in production.
