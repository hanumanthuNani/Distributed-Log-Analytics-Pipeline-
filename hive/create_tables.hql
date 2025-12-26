CREATE DATABASE IF NOT EXISTS analytics;

USE analytics;

-- External table for raw log events
CREATE EXTERNAL TABLE IF NOT EXISTS raw_logs (
  timestamp STRING,
  service_name STRING,
  level STRING,
  message STRING,
  host STRING
)
PARTITIONED BY (dt STRING)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
STORED AS TEXTFILE
LOCATION '/warehouse/raw_logs';

-- Table for aggregated log levels
CREATE EXTERNAL TABLE IF NOT EXISTS log_level_counts (
  window_start TIMESTAMP,
  window_end TIMESTAMP,
  level STRING,
  total BIGINT
)
PARTITIONED BY (dt STRING)
STORED AS PARQUET
LOCATION '/warehouse/log_level_counts';

MSCK REPAIR TABLE raw_logs;
MSCK REPAIR TABLE log_level_counts;
