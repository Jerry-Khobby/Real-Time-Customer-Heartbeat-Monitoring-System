import json
import logging
import random
import signal
import sys
import time
from datetime import datetime
from typing import Dict

from kafka import KafkaProducer
from kafka.errors import KafkaError, NoBrokersAvailable
from config import (
    KAFKA_BROKER,
    KAFKA_TOPIC,
    NUM_PATIENTS,
    BASE_SLEEP_INTERVAL,
    HEART_RATE_MIN,
    HEART_RATE_MAX,
    ANOMALY_PROBABILITY,
    MAX_RETRIES,
    RETRY_BACKOFF_SECONDS,
)

from logging_config import setup_logging


# Logging Configuration
logger = setup_logging("heartbeat_producer")



#graceful shutting down 
running = True
def shutdown_handler(signum, frame):
    global running
    logger.info("Shutdown signal received. Stopping producer...")
    running = False


signal.signal(signal.SIGINT, shutdown_handler)
signal.signal(signal.SIGTERM, shutdown_handler)


# Heart Rate Logic
def generate_heart_rate() -> Dict[str, int | str]:
    if random.random() < ANOMALY_PROBABILITY:
        if random.choice(["high", "low"]) == "high":
            return {
                "heart_rate": random.randint(120, 160),
                "status": "tachycardia",
            }
        else:
            return {
                "heart_rate": random.randint(35, 50),
                "status": "bradycardia",
            }

    return {
        "heart_rate": random.randint(HEART_RATE_MIN, HEART_RATE_MAX),
        "status": "normal",
    }


def generate_patient_event(patient_id: int) -> Dict:
    heart_data = generate_heart_rate()

    return {
        "patient_id": patient_id,
        "timestamp": datetime.utcnow().isoformat(),
        "heart_rate": heart_data["heart_rate"],
        "status": heart_data["status"],
    }


def create_producer_with_retry() -> KafkaProducer:
    retries = 0

    while retries < MAX_RETRIES:
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BROKER,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                retries=5,
                linger_ms=10,
                acks="all",
            )
            logger.info("Connected to Kafka successfully.")
            return producer

        except NoBrokersAvailable:
            retries += 1
            backoff = RETRY_BACKOFF_SECONDS * retries
            logger.warning(
                f"Kafka not available. Retry {retries}/{MAX_RETRIES} in {backoff}s..."
            )
            time.sleep(backoff)

    logger.error("Failed to connect to Kafka after maximum retries.")
    sys.exit(1)


def send_with_retry(producer: KafkaProducer, event: Dict):
    attempt = 0

    while attempt < MAX_RETRIES:
        try:
            future = producer.send(KAFKA_TOPIC, value=event)
            record_metadata = future.get(timeout=10)

            logger.info(
                f"Sent → patient_id={event['patient_id']} "
                f"heart_rate={event['heart_rate']} "
                f"status={event['status']} "
                f"partition={record_metadata.partition}"
            )
            return

        except KafkaError as e:
            attempt += 1
            backoff = RETRY_BACKOFF_SECONDS * attempt
            logger.warning(
                f"Send failed (attempt {attempt}/{MAX_RETRIES}). "
                f"Retrying in {backoff}s... Error: {e}"
            )
            time.sleep(backoff)

    logger.error("Message permanently failed after max retries.")

def run_producer():
    logger.info("Starting Heartbeat Data Producer...")
    logger.info(f"Kafka Broker: {KAFKA_BROKER}")
    logger.info(f"Topic: {KAFKA_TOPIC}")
    logger.info(f"Simulating {NUM_PATIENTS} patients")

    producer = create_producer_with_retry()

    try:
        while running:
            patient_id = random.randint(1, NUM_PATIENTS)
            event = generate_patient_event(patient_id)

            send_with_retry(producer, event)

            jitter = random.uniform(0.5, 1.5)
            time.sleep(BASE_SLEEP_INTERVAL * jitter)

    except Exception as e:
        logger.exception(f"Unexpected error in producer loop: {e}")

    finally:
        logger.info("Flushing producer...")
        producer.flush()
        producer.close()
        logger.info("Producer closed gracefully.")

if __name__ == "__main__":
    run_producer()
