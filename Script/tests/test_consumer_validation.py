from pyspark.sql.functions import col

HEART_RATE_MIN = 30
HEART_RATE_MAX = 200
VALID_STATUSES = ["normal", "tachycardia", "bradycardia"]


def test_validation_filters_invalid_rows(spark):

    data = [
        (1, "2024-01-01T00:00:00", 80, "normal"),
        (2, "2024-01-01T00:00:00", 250, "normal"),
        (3, "2024-01-01T00:00:00", 70, "invalid_status"),
        (None, "2024-01-01T00:00:00", 70, "normal"),
    ]

    df = spark.createDataFrame(
        data,
        ["patient_id", "timestamp", "heart_rate", "status"]
    )

    validated_df = df.filter(
        (col("patient_id").isNotNull()) &
        (col("heart_rate").between(HEART_RATE_MIN, HEART_RATE_MAX)) &
        (col("status").isin(VALID_STATUSES))
    )

    results = validated_df.collect()

    assert len(results) == 1
    assert results[0]["patient_id"] == 1