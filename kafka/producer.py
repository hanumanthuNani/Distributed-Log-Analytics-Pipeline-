#!/usr/bin/env python3
"""
Kafka log producer that simulates application logs as JSON messages.
"""
import argparse
import json
import random
import socket
import time
from datetime import datetime

from kafka import KafkaProducer

LEVELS = ["INFO", "WARN", "ERROR", "DEBUG"]
SERVICES = ["auth-service", "payment-service", "order-service", "search-service"]
MESSAGES = [
    "User login successful",
    "Payment authorized",
    "Order placed",
    "Cache miss handled",
    "Downstream service latency warning",
    "Unhandled exception caught",
]


def build_producer(bootstrap_servers: str) -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        linger_ms=50,
    )


def generate_log() -> dict:
    return {
        "timestamp": datetime.utcnow().isoformat(timespec="seconds") + "Z",
        "service_name": random.choice(SERVICES),
        "level": random.choice(LEVELS),
        "message": random.choice(MESSAGES),
        "host": socket.gethostname(),
    }


def produce(bootstrap_servers: str, topic: str, interval: float) -> None:
    producer = build_producer(bootstrap_servers)
    try:
        while True:
            event = generate_log()
            producer.send(topic, value=event)
            producer.flush()
            print(f"[producer] sent -> {event}")
            time.sleep(interval)
    except KeyboardInterrupt:
        print("\n[producer] stopping...")
    finally:
        producer.close()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Kafka log producer simulator")
    parser.add_argument("--bootstrap-servers", default="localhost:9092", help="Kafka bootstrap servers")
    parser.add_argument("--topic", default="logs", help="Kafka topic name")
    parser.add_argument("--interval", type=float, default=1.0, help="Seconds between messages")
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    produce(args.bootstrap_servers, args.topic, args.interval)
