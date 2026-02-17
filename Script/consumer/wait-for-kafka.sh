#!/bin/sh

echo "Waiting for Kafka..."

while ! nc -z kafka 9092; do
  sleep 2
done

echo "Kafka is ready."

python consumer/kafka_consumer.py
