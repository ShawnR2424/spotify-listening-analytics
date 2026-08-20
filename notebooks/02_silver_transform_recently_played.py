# Databricks notebook source
bronze_df = spark.table(
    "spotify_analytics.bronze.spotify_recently_played_raw"
)

display(bronze_df)

# COMMAND ----------

print(f"Bronze batches: {bronze_df.count()}")

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

# COMMAND ----------

artist_schema = StructType([
    StructField("id", StringType(), True),
    StructField("name", StringType(), True),
    StructField("uri", StringType(), True),
])

album_schema = StructType([
    StructField("id", StringType(), True),
    StructField("name", StringType(), True),
    StructField("release_date", StringType(), True),
    StructField("release_date_precision", StringType(), True),
])

track_schema = StructType([
    StructField("id", StringType(), True),
    StructField("uri", StringType(), True),
    StructField("name", StringType(), True),
    StructField("duration_ms", IntegerType(), True),
    StructField("explicit", BooleanType(), True),
    StructField("artists", ArrayType(artist_schema), True),
    StructField("album", album_schema, True),
])

context_schema = StructType([
    StructField("type", StringType(), True),
    StructField("uri", StringType(), True),
])

play_history_schema = StructType([
    StructField("played_at", StringType(), True),
    StructField("track", track_schema, True),
    StructField("context", context_schema, True),
])

spotify_response_schema = StructType([
    StructField(
        "items",
        ArrayType(play_history_schema),
        True,
    )
])

# COMMAND ----------

parsed_df = bronze_df.withColumn(
    "spotify_response",
    F.from_json(
        F.col("payload"),
        spotify_response_schema,
    ),
)

# COMMAND ----------

parsed_df.select(
    "batch_id",
    "spotify_response"
).printSchema()

# COMMAND ----------

events_df = (
    parsed_df
    .select(
        "batch_id",
        "ingested_at",
        F.explode(
            F.col("spotify_response.items")
        ).alias("item"),
    )
)

# COMMAND ----------

print(f"Listening events: {events_df.count()}")

# COMMAND ----------

display(
    events_df.select(
        "item.played_at",
        "item.track.name",
        "item.track.artists",
    )
)

# COMMAND ----------

silver_candidate_df = events_df.select(
    "batch_id",
    "ingested_at",

    F.to_timestamp(
        F.col("item.played_at")
    ).alias("played_at"),

    F.col(
        "item.track.id"
    ).alias("track_id"),

    F.col(
        "item.track.uri"
    ).alias("track_uri"),

    F.col(
        "item.track.name"
    ).alias("track_name"),

    F.col(
        "item.track.duration_ms"
    ).alias("duration_ms"),

    F.col(
        "item.track.explicit"
    ).alias("is_explicit"),

    F.col(
        "item.track.album.id"
    ).alias("album_id"),

    F.col(
        "item.track.album.name"
    ).alias("album_name"),

    F.col(
        "item.track.album.release_date"
    ).alias("album_release_date"),

    F.col(
        "item.track.artists"
    ).alias("artists"),

    F.col(
        "item.context.type"
    ).alias("context_type"),

    F.col(
        "item.context.uri"
    ).alias("context_uri"),
)

# COMMAND ----------

display(silver_candidate_df)

# COMMAND ----------

silver_candidate_df = silver_candidate_df.withColumn(
    "listening_event_id",
    F.sha2(
        F.concat_ws(
            "||",
            F.coalesce(
                F.col("track_id"),
                F.col("track_uri"),
                F.lit("unknown_track"),
            ),
            F.col("played_at").cast("string"),
        ),
        256,
    ),
)

# COMMAND ----------

silver_candidate_df = silver_candidate_df.select(
    "listening_event_id",
    "played_at",
    "track_id",
    "track_uri",
    "track_name",
    "duration_ms",
    "is_explicit",
    "album_id",
    "album_name",
    "album_release_date",
    "artists",
    "context_type",
    "context_uri",
    "batch_id",
    "ingested_at",
)

# COMMAND ----------

display(silver_candidate_df)

# COMMAND ----------

from pyspark.sql.window import Window

# COMMAND ----------

dedupe_window = (
    Window
    .partitionBy("listening_event_id")
    .orderBy(
        F.col("ingested_at").desc()
    )
)

# COMMAND ----------

silver_df = (
    silver_candidate_df
    .withColumn(
        "_row_number",
        F.row_number().over(dedupe_window),
    )
    .filter(
        F.col("_row_number") == 1
    )
    .drop("_row_number")
)

# COMMAND ----------

print(
    "Before deduplication:",
    silver_candidate_df.count()
)

print(
    "After deduplication:",
    silver_df.count()
)

# COMMAND ----------

(
    silver_df
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(
        "spotify_analytics.silver.listening_events"
    )
)