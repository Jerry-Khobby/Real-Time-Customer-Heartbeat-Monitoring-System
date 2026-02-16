import json
import logging
import signal
import sys
from typing import Dict
import psycopg2
from kafka import KafkaConsumer
from kafka.errors import KafkaError
from consumer.config import KAFKA_BROKER, KAFKA_TOPIC, DB_CONN_STRING
from producer.logging_config import setup_logging



# Logging Configuration
logger = setup_logging("heartbeat_consumer")


# Graceful Shutdown
running = True


def shutdown_handler(signum, frame):
    global running
    logger.info("Shutdown signal received. Stopping consumer...")
    running = False


signal.signal(signal.SIGINT, shutdown_handler)
signal.signal(signal.SIGTERM, shutdown_handler)



# Validation
def validate_message(data: Dict) -> bool:
    required_fields = {"patient_id", "timestamp", "heart_rate", "status"}
    if not required_fields.issubset(data.keys()):
        logger.warning(f"Invalid message structure: {data}")
        return False

    if not isinstance(data["heart_rate"], int):
        logger.warning(f"Invalid heart_rate type: {data}")
        return False

    return True



# Main Consumer Logic
def run_consumer():
    logger.info("Starting Kafka Consumer...")
    logger.info(f"Kafka Broker: {KAFKA_BROKER}")
    logger.info(f"Subscribed to topic: {KAFKA_TOPIC}")

    try:
        consumer = KafkaConsumer(
            KAFKA_TOPIC,
            bootstrap_servers=KAFKA_BROKER,
            value_deserializer=lambda m: json.loads(m.decode("utf-8")),
            auto_offset_reset="earliest",
            enable_auto_commit=True,
            group_id="heartbeat-consumer-group",
        )
    except KafkaError as e:
        logger.error(f"Failed to connect to Kafka: {e}")
        sys.exit(1)

    try:
        conn = psycopg2.connect(DB_CONN_STRING)
        cursor = conn.cursor()
        logger.info("Connected to PostgreSQL successfully.")
    except Exception as e:
        logger.error(f"Failed to connect to PostgreSQL: {e}")
        sys.exit(1)

    try:
        for message in consumer:
            if not running:
                break

            data = message.value

            if not validate_message(data):
                continue

            try:
                cursor.execute(
                    """
                    INSERT INTO heartbeats 
                    (patient_id, timestamp, heart_rate, status)
                    VALUES (%s, %s, %s, %s)
                    """,
                    (
                        data["patient_id"],
                        data["timestamp"],
                        data["heart_rate"],
                        data["status"],
                    ),
                )
                conn.commit()

                logger.info(
                    f"Inserted → patient_id={data['patient_id']} "
                    f"heart_rate={data['heart_rate']} "
                    f"status={data['status']}"
                )

            except Exception as db_error:
                conn.rollback()
                logger.error(f"Database insert failed: {db_error}")

    except Exception as e:
        logger.exception(f"Unexpected error in consumer loop: {e}")

    finally:
        logger.info("Closing consumer and database connection...")
        consumer.close()
        cursor.close()
        conn.close()
        logger.info("Consumer shut down gracefully.")


if __name__ == "__main__":
    run_consumer()
