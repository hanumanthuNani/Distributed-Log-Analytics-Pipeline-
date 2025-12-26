#!/usr/bin/env python3
"""
Kafka log consumer that reads JSON log events and prints them.
"""
import argparse
import json

from kafka import KafkaConsumer


def consume(bootstrap_servers: str, topic: str, group_id: str) -> None:
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=bootstrap_servers,
        group_id=group_id,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        auto_offset_reset="latest",
        enable_auto_commit=True,
    )
    try:
        for message in consumer:
            print(f"[consumer] offset={message.offset} partition={message.partition} -> {message.value}")
    except KeyboardInterrupt:
        print("\n[consumer] stopping...")
    finally:
        consumer.close()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Kafka log consumer")
    parser.add_argument("--bootstrap-servers", default="localhost:9092", help="Kafka bootstrap servers")
    parser.add_argument("--topic", default="logs", help="Kafka topic name")
    parser.add_argument("--group-id", default="log-consumers", help="Consumer group id")
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    consume(args.bootstrap_servers, args.topic, args.group_id)
