FROM python:3.11-slim

WORKDIR /app

# Install netcat for wait-for-kafka.sh
RUN apt-get update && \
  apt-get install -y --no-install-recommends netcat-openbsd && \
  apt-get clean && \
  rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app/ .

RUN chmod +x /app/producer/wait-for-kafka.sh

ENV PYTHONPATH=/app

CMD ["python"]
