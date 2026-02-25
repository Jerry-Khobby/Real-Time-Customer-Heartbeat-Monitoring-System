FROM python:3.11-slim

WORKDIR /Script

# Install netcat for wait-for-kafka.sh
RUN apt-get update && \
  apt-get install -y --no-install-recommends netcat-openbsd && \
  apt-get clean && \
  rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Install PySpark explicitly (needed for tests)
RUN pip install --no-cache-dir pyspark

COPY Script/ .

RUN chmod +x /Script/producer/wait-for-kafka.sh

RUN chmod +x /Script/consumer/wait-for-kafka.sh

ENV PYTHONPATH=/Script
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

CMD ["python"]
