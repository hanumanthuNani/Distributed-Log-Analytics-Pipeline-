#!/usr/bin/env python3
"""
Spark Structured Streaming job that reads logs from Kafka, parses JSON, aggregates
counts by log level, and writes results to Hive and a simulated HBase sink.
"""
import argparse
from typing import Any

from pyspark.sql import DataFrame, SparkSession, functions as F, types as T

LOG_SCHEMA = T.StructType(
    [
        T.StructField("timestamp", T.StringType(), False),
        T.StructField("service_name", T.StringType(), False),
        T.StructField("level", T.StringType(), False),
        T.StructField("message", T.StringType(), False),
        T.StructField("host", T.StringType(), False),
    ]
)


def create_spark(app_name: str) -> SparkSession:
    return (
        SparkSession.builder.appName(app_name)
        .enableHiveSupport()
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )


def parse_logs(raw_df: DataFrame) -> DataFrame:
    parsed = (
        raw_df.selectExpr("CAST(value AS STRING) as json_str")
        .select(F.from_json("json_str", LOG_SCHEMA).alias("log"))
        .select("log.*")
        .withColumn("event_time", F.to_timestamp("timestamp"))
        .dropna(subset=["event_time"])
    )
    return parsed


def aggregate_logs(parsed_df: DataFrame) -> DataFrame:
    return (
        parsed_df.groupBy(F.window("event_time", "1 minute"), parsed_df.level)
        .count()
        .withColumnRenamed("count", "total")
    )


def write_to_hbase(batch_df: DataFrame, hbase_table: str) -> None:
    materialized = (
        batch_df.select(
            F.date_format("window.start", "yyyyMMddHHmmss").alias("window_start"),
            F.date_format("window.end", "yyyyMMddHHmmss").alias("window_end"),
            "level",
            "total",
        )
        .collect()
    )
    for row in materialized:
        row_key = f"{row.window_start}_{row.level}"
        print(f"[hbase] table={hbase_table} put row_key={row_key} window_end={row.window_end} total={row.total}")


def write_to_hive(batch_df: DataFrame, spark: SparkSession, database: str, table: str) -> None:
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {database}")
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {database}.{table} (
            window_start timestamp,
            window_end timestamp,
            level string,
            total bigint
        )
        PARTITIONED BY (dt string)
        STORED AS PARQUET
        """
    )

    batch_df = batch_df.withColumn("dt", F.date_format("window.start", "yyyy-MM-dd"))
    (
        batch_df.selectExpr("window.start as window_start", "window.end as window_end", "level", "total", "dt")
        .write.mode("append")
        .insertInto(f"{database}.{table}", overwrite=False)
    )


def start_stream(
    bootstrap_servers: str,
    topic: str,
    checkpoint_location: str,
    hbase_table: str,
    hive_database: str,
    hive_table: str,
    app_name: str,
) -> None:
    spark = create_spark(app_name)
    raw_stream = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", bootstrap_servers)
        .option("subscribe", topic)
        .option("startingOffsets", "latest")
        .load()
    )

    parsed_stream = parse_logs(raw_stream)
    aggregated = aggregate_logs(parsed_stream)

    def sink_to_stores(batch_df: DataFrame, epoch_id: int) -> Any:
        write_to_hbase(batch_df, hbase_table)
        write_to_hive(batch_df, spark, hive_database, hive_table)

    query = (
        aggregated.writeStream.outputMode("update")
        .foreachBatch(sink_to_stores)
        .option("checkpointLocation", checkpoint_location)
        .start()
    )
    query.awaitTermination()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Spark Structured Streaming job for log analytics")
    parser.add_argument("--bootstrap-servers", default="localhost:9092", help="Kafka bootstrap servers")
    parser.add_argument("--topic", default="logs", help="Kafka topic name")
    parser.add_argument("--checkpoint-location", default="/tmp/spark-checkpoints/logs", help="Checkpoint directory")
    parser.add_argument("--hbase-table", default="log_analytics:log_levels", help="HBase table for aggregates")
    parser.add_argument("--hive-database", default="analytics", help="Hive database name")
    parser.add_argument("--hive-table", default="log_level_counts", help="Hive table name")
    parser.add_argument("--app-name", default="DistributedLogAnalytics", help="Spark application name")
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    start_stream(
        bootstrap_servers=args.bootstrap_servers,
        topic=args.topic,
        checkpoint_location=args.checkpoint_location,
        hbase_table=args.hbase_table,
        hive_database=args.hive_database,
        hive_table=args.hive_table,
        app_name=args.app_name,
    )
