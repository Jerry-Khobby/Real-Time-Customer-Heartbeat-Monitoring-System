#!/bin/sh
# wait-for-kafka.sh

echo "Waiting for Kafka to be ready..."

# Wait until Kafka is reachable on port 9092
while ! nc -z kafka 9092; do
  echo "Kafka not ready yet, sleeping 2s..."
  sleep 2
done

echo "Kafka is up! Starting the producer..."
exec "$@"
