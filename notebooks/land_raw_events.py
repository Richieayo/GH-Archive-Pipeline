from pyspark.sql import functions as F

LANDING    = "/Volumes/github_events/raw/landing"
CHECKPOINT = "/Volumes/github_events/raw/checkpoints/raw_events"
TARGET     = "github_events.raw.raw_events"

raw = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "text")
    .load(LANDING)
)

events = (
    raw
    .withColumn("source_file", F.col("_metadata.file_name"))
    .withColumn("ingested_at", F.current_timestamp())
    .withColumn(
        "event_hour",
        F.to_timestamp(
            F.regexp_extract("source_file", r"(\d{4}-\d{2}-\d{2}-\d{1,2})", 1),
            "yyyy-MM-dd-H",
        ),
    )
    .withColumnRenamed("value", "raw_event")
    .select("raw_event", "event_hour", "source_file", "ingested_at")
)

query = (
    events.writeStream
    .format("delta")
    .option("checkpointLocation", CHECKPOINT)
    .partitionBy("event_hour")
    .trigger(availableNow=True)
    .toTable(TARGET)
)

query.awaitTermination()