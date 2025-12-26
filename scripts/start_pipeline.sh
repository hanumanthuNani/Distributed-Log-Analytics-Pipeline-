#!/usr/bin/env bash
# Orchestrate Hive setup, Spark streaming job, and Kafka producer/consumer.
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

BOOTSTRAP="${BOOTSTRAP:-localhost:9092}"
TOPIC="${TOPIC:-logs}"
CHECKPOINT="${CHECKPOINT:-/tmp/spark-checkpoints/logs}"
HBASE_TABLE="${HBASE_TABLE:-log_analytics:log_levels}"
HIVE_DB="${HIVE_DB:-analytics}"
HIVE_TABLE="${HIVE_TABLE:-log_level_counts}"
INTERVAL="${INTERVAL:-1.0}"

echo "[setup] Creating Hive tables..."
hive -f "${ROOT_DIR}/hive/create_tables.hql"

echo "[run] Starting Spark Structured Streaming job..."
spark-submit \
  --master local[*] \
  --deploy-mode client \
  "${ROOT_DIR}/spark/spark_streaming_job.py" \
  --bootstrap-servers "${BOOTSTRAP}" \
  --topic "${TOPIC}" \
  --checkpoint-location "${CHECKPOINT}" \
  --hbase-table "${HBASE_TABLE}" \
  --hive-database "${HIVE_DB}" \
  --hive-table "${HIVE_TABLE}" \
  --app-name "DistributedLogAnalytics" &

SPARK_PID=$!

echo "[run] Starting Kafka consumer (background)..."
python "${ROOT_DIR}/kafka/consumer.py" \
  --bootstrap-servers "${BOOTSTRAP}" \
  --topic "${TOPIC}" \
  --group-id "log-consumers" &

CONSUMER_PID=$!

echo "[run] Starting Kafka producer..."
python "${ROOT_DIR}/kafka/producer.py" \
  --bootstrap-servers "${BOOTSTRAP}" \
  --topic "${TOPIC}" \
  --interval "${INTERVAL}"

echo "[cleanup] Stopping background processes..."
kill "${SPARK_PID}" "${CONSUMER_PID}"
