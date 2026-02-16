import os




#setup log file

LOG_DIR = os.getenv("LOG_DIR", "/app/logs")
LOG_FILE = os.getenv("LOG_FILE", "heartbeat_producer.log")
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")


KAFKA_BROKER: str = os.getenv("KAFKA_BROKER", "kafka:9092")
KAFKA_TOPIC: str = os.getenv("KAFKA_TOPIC", "heartbeats")




NUM_PATIENTS: int = int(os.getenv("NUM_PATIENTS", 10))
BASE_SLEEP_INTERVAL: float = float(os.getenv("SLEEP_INTERVAL", 1.0))

HEART_RATE_MIN: int = 60
HEART_RATE_MAX: int = 100
ANOMALY_PROBABILITY: float = 0.05




MAX_RETRIES: int = int(os.getenv("MAX_RETRIES", 5))
RETRY_BACKOFF_SECONDS: float = float(os.getenv("RETRY_BACKOFF_SECONDS", 2))
