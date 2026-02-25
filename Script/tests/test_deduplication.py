def test_drop_duplicates(spark):

    data = [
        (1, "2024-01-01T00:00:00", 80, "normal"),
        (1, "2024-01-01T00:00:00", 80, "normal"),
        (2, "2024-01-01T00:00:01", 90, "normal"),
    ]

    df = spark.createDataFrame(
        data,
        ["patient_id", "timestamp", "heart_rate", "status"]
    )

    deduped = df.dropDuplicates(["patient_id", "timestamp"])

    assert deduped.count() == 2