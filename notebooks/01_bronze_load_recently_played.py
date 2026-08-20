# Databricks notebook source
raw_file_path = (
    "/Volumes/spotify_analytics/bronze/landing/"
    "recently_played_sample.json"
)

with open(raw_file_path, "r", encoding="utf-8") as file:
    raw_payload = file.read()

print(f"Payload size: {len(raw_payload):,} characters")
print(raw_payload[:500])

# COMMAND ----------

from uuid import uuid4

from pyspark.sql import functions as F

# COMMAND ----------

batch_id = str(uuid4())

source_endpoint = "/me/player/recently-played"

# COMMAND ----------

bronze_df = spark.createDataFrame(
    [
        (
            batch_id,
            source_endpoint,
            raw_payload,
        )
    ],
    [
        "batch_id",
        "source_endpoint",
        "payload",
    ],
)

# COMMAND ----------

bronze_df = (
    bronze_df
    .withColumn(
        "ingested_at",
        F.current_timestamp(),
    )
    .select(
        "batch_id",
        "ingested_at",
        "source_endpoint",
        "payload",
    )
)

# COMMAND ----------

display(bronze_df)

# COMMAND ----------

(
    bronze_df
    .write
    .format("delta")
    .mode("append")
    .saveAsTable(
        "spotify_analytics.bronze.spotify_recently_played_raw"
    )
)