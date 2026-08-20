# Databricks notebook source
from pyspark.sql import functions as F
from pyspark.sql.window import Window

events_df = spark.table(
    "spotify_analytics.silver.listening_events"
)

display(events_df)

# COMMAND ----------

track_observations_df = (
    events_df
    .groupBy("track_id")
    .agg(
        F.min("ingested_at").alias("first_observed_at"),
        F.max("ingested_at").alias("last_observed_at"),
    )
)

# COMMAND ----------

track_window = (
    Window
    .partitionBy("track_id")
    .orderBy(F.col("ingested_at").desc())
)

latest_tracks_df = (
    events_df
    .withColumn(
        "_row_number",
        F.row_number().over(track_window),
    )
    .filter(F.col("_row_number") == 1)
    .select(
        "track_id",
        "track_uri",
        "track_name",
        "duration_ms",
        "is_explicit",
        "album_id",
    )
)

# COMMAND ----------

tracks_df = (
    latest_tracks_df
    .join(
        track_observations_df,
        on="track_id",
        how="left",
    )
)

# COMMAND ----------

display(tracks_df)

# COMMAND ----------

print("Listening events:", events_df.count())
print("Unique tracks:", tracks_df.count())

# COMMAND ----------

(
    tracks_df
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(
        "spotify_analytics.silver.tracks"
    )
)

# COMMAND ----------

album_window = (
    Window
    .partitionBy("album_id")
    .orderBy(F.col("ingested_at").desc())
)

# COMMAND ----------

latest_albums_df = (
    events_df
    .filter(F.col("album_id").isNotNull())
    .withColumn(
        "_row_number",
        F.row_number().over(album_window),
    )
    .filter(F.col("_row_number") == 1)
    .select(
        "album_id",
        "album_name",
        "album_release_date",
    )
)

# COMMAND ----------

album_observations_df = (
    events_df
    .filter(F.col("album_id").isNotNull())
    .groupBy("album_id")
    .agg(
        F.min("ingested_at").alias("first_observed_at"),
        F.max("ingested_at").alias("last_observed_at"),
    )
)

# COMMAND ----------

albums_df = (
    latest_albums_df
    .join(
        album_observations_df,
        on="album_id",
        how="left",
    )
)

# COMMAND ----------

display(albums_df)

# COMMAND ----------

(
    albums_df
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(
        "spotify_analytics.silver.albums"
    )
)

# COMMAND ----------

exploded_artists_df = (
    events_df
    .select(
        "track_id",
        "ingested_at",
        F.posexplode("artists").alias(
            "artist_position",
            "artist",
        ),
    )
)

# COMMAND ----------

display(
    exploded_artists_df.select(
        "track_id",
        "artist_position",
        "artist.id",
        "artist.name",
    )
)

# COMMAND ----------

artist_candidates_df = (
    exploded_artists_df
    .select(
        F.col("artist.id").alias("artist_id"),
        F.col("artist.name").alias("artist_name"),
        F.col("artist.uri").alias("artist_uri"),
        "ingested_at",
    )
    .filter(F.col("artist_id").isNotNull())
)

# COMMAND ----------

artist_observations_df = (
    artist_candidates_df
    .groupBy("artist_id")
    .agg(
        F.min("ingested_at").alias("first_observed_at"),
        F.max("ingested_at").alias("last_observed_at"),
    )
)

# COMMAND ----------

artist_window = (
    Window
    .partitionBy("artist_id")
    .orderBy(F.col("ingested_at").desc())
)

latest_artists_df = (
    artist_candidates_df
    .withColumn(
        "_row_number",
        F.row_number().over(artist_window),
    )
    .filter(F.col("_row_number") == 1)
    .select(
        "artist_id",
        "artist_name",
        "artist_uri",
    )
)

# COMMAND ----------

artists_df = (
    latest_artists_df
    .join(
        artist_observations_df,
        on="artist_id",
        how="left",
    )
)

# COMMAND ----------

display(artists_df)

# COMMAND ----------

(
    artists_df
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(
        "spotify_analytics.silver.artists"
    )
)

# COMMAND ----------

track_artists_df = (
    exploded_artists_df
    .select(
        "track_id",
        F.col("artist.id").alias("artist_id"),
        "artist_position",
    )
    .filter(
        F.col("track_id").isNotNull()
        & F.col("artist_id").isNotNull()
    )
    .dropDuplicates(
        ["track_id", "artist_id"]
    )
)

# COMMAND ----------

display(track_artists_df)

# COMMAND ----------

(
    track_artists_df
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(
        "spotify_analytics.silver.track_artists"
    )
)

# COMMAND ----------

