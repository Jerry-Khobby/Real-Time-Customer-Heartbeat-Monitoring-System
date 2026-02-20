from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col,window
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, TimestampType
from producer.logging_config import setup_logging

HEART_RATE_MIN = 30
HEART_RATE_MAX = 200
VALID_STATUSES = ["normal", "tachycardia", "bradycardia"]

# Setup logging
logger = setup_logging("heartbeats_consumer")
logger.info("Starting Spark Streaming application...")

spark = SparkSession.builder \
    .appName("HeartbeatStreaming") \
    .config("spark.jars", "/opt/spark-apps/jars/postgresql-42.7.6.jar") \
    .getOrCreate()

# Set Spark log level
spark.sparkContext.setLogLevel("WARN")

logger.info("Spark session created successfully")

# Define schema
schema = StructType([
    StructField("patient_id", IntegerType(), nullable=False),
    StructField("timestamp", StringType(), nullable=False),
    StructField("heart_rate", IntegerType(), nullable=False),
    StructField("status", StringType(), nullable=False),
])

logger.info("Reading from Kafka topic: heartbeats")

# Read from Kafka
df = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "heartbeats") \
    .option("startingOffsets", "earliest") \
    .load()

# Parse JSON and cast timestamp
heartbeat_df = df.selectExpr("CAST(value AS STRING) as json") \
    .select(from_json("json", schema).alias("data")) \
    .select("data.*") \
    .withColumn("timestamp", col("timestamp").cast(TimestampType()))

# Validate
validated_df = heartbeat_df.filter(
    (col("patient_id").isNotNull()) &
    (col("timestamp").isNotNull()) &
    (col("heart_rate").between(HEART_RATE_MIN, HEART_RATE_MAX)) &
    (col("status").isin(VALID_STATUSES))
)

logger.info("Starting stream processing...")

# Apply watermark of 10 minutes on the event-time column "timestamp"
# and aggregate using 5-minute tumbling windows
watermarked_df = validated_df.withWatermark("timestamp","10 minutes")

# Perform aggregation on patient_id and 5-minute windows
windowed_df = watermarked_df.groupBy(
    "patient_id",
    window("timestamp", "5 minutes")
).count()

# Write to PostgreSQL
def write_to_postgres(batch_df, batch_id):
    if batch_df.count() == 0:
        logger.info(f"[Batch {batch_id}] No records to write")
        return
    
    try:
        batch_df.write \
            .format("jdbc") \
            .option("url", "jdbc:postgresql://postgres:5432/heartbeat_db") \
            .option("dbtable", "heartbeats") \
            .option("user", "postgres") \
            .option("password", "postgres") \
            .option("driver", "org.postgresql.Driver") \
            .mode("append") \
            .save()
        logger.info(f"[Batch {batch_id}] Successfully wrote {batch_df.count()} rows to PostgreSQL")
    except Exception as e:
        logger.error(f"[Batch {batch_id}] Failed to write to PostgreSQL: {e}")

windowed_df.writeStream \
    .foreachBatch(write_to_postgres) \
    .outputMode("update") \
    .start() \
    .awaitTermination()
