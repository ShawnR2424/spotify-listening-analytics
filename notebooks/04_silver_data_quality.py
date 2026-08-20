# Databricks notebook source
from pyspark.sql import functions as F

events_df = spark.table(
    "spotify_analytics.silver.listening_events"
)

tracks_df = spark.table(
    "spotify_analytics.silver.tracks"
)

albums_df = spark.table(
    "spotify_analytics.silver.albums"
)

artists_df = spark.table(
    "spotify_analytics.silver.artists"
)

track_artists_df = spark.table(
    "spotify_analytics.silver.track_artists"
)

# COMMAND ----------

quality_results = []


def record_check(
    check_name,
    severity,
    violation_count,
):
    status = "PASS" if violation_count == 0 else severity

    quality_results.append(
        {
            "check_name": check_name,
            "severity": severity,
            "status": status,
            "violation_count": violation_count,
        }
    )

# COMMAND ----------

null_event_ids = (
    events_df
    .filter(F.col("listening_event_id").isNull())
    .count()
)

record_check(
    "listening_event_id_not_null",
    "FAIL",
    null_event_ids,
)

# COMMAND ----------

duplicate_event_ids = (
    events_df
    .groupBy("listening_event_id")
    .count()
    .filter(F.col("count") > 1)
    .count()
)

record_check(
    "listening_event_id_unique",
    "FAIL",
    duplicate_event_ids,
)

# COMMAND ----------

null_track_ids = (
    tracks_df
    .filter(F.col("track_id").isNull())
    .count()
)

record_check(
    "track_id_not_null",
    "FAIL",
    null_track_ids,
)

# COMMAND ----------

duplicate_tracks = (
    tracks_df
    .groupBy("track_id")
    .count()
    .filter(F.col("count") > 1)
    .count()
)

record_check(
    "track_id_unique",
    "FAIL",
    duplicate_tracks,
)

# COMMAND ----------

null_artist_ids = (
    artists_df
    .filter(F.col("artist_id").isNull())
    .count()
)

record_check(
    "artist_id_not_null",
    "FAIL",
    null_artist_ids,
)

# COMMAND ----------

duplicate_artists = (
    artists_df
    .groupBy("artist_id")
    .count()
    .filter(F.col("count") > 1)
    .count()
)

record_check(
    "artist_id_unique",
    "FAIL",
    duplicate_artists,
)

# COMMAND ----------

null_album_ids = (
    albums_df
    .filter(F.col("album_id").isNull())
    .count()
)

record_check(
    "album_id_not_null",
    "FAIL",
    null_album_ids,
)

# COMMAND ----------

duplicate_albums = (
    albums_df
    .groupBy("album_id")
    .count()
    .filter(F.col("count") > 1)
    .count()
)

record_check(
    "album_id_unique",
    "FAIL",
    duplicate_albums,
)

# COMMAND ----------

duplicate_track_artists = (
    track_artists_df
    .groupBy(
        "track_id",
        "artist_id",
    )
    .count()
    .filter(F.col("count") > 1)
    .count()
)

record_check(
    "track_artist_relationship_unique",
    "FAIL",
    duplicate_track_artists,
)

# COMMAND ----------

orphan_event_tracks = (
    events_df
    .filter(F.col("track_id").isNotNull())
    .select("track_id")
    .distinct()
    .join(
        tracks_df.select("track_id"),
        on="track_id",
        how="left_anti",
    )
)

# COMMAND ----------

orphan_event_track_count = orphan_event_tracks.count()

record_check(
    "listening_events_have_valid_tracks",
    "FAIL",
    orphan_event_track_count,
)

# COMMAND ----------

orphan_albums = (
    tracks_df
    .filter(F.col("album_id").isNotNull())
    .select("album_id")
    .distinct()
    .join(
        albums_df.select("album_id"),
        on="album_id",
        how="left_anti",
    )
)

# COMMAND ----------

orphan_album_count = orphan_albums.count()

record_check(
    "tracks_have_valid_albums",
    "FAIL",
    orphan_album_count,
)

# COMMAND ----------

orphan_bridge_tracks = (
    track_artists_df
    .select("track_id")
    .distinct()
    .join(
        tracks_df.select("track_id"),
        on="track_id",
        how="left_anti",
    )
)

# COMMAND ----------

record_check(
    "track_artist_has_valid_track",
    "FAIL",
    orphan_bridge_tracks.count(),
)

# COMMAND ----------

orphan_bridge_artists = (
    track_artists_df
    .select("artist_id")
    .distinct()
    .join(
        artists_df.select("artist_id"),
        on="artist_id",
        how="left_anti",
    )
)

# COMMAND ----------

record_check(
    "track_artist_has_valid_artist",
    "FAIL",
    orphan_bridge_artists.count(),
)

# COMMAND ----------

invalid_duration = (
    tracks_df
    .filter(
        F.col("duration_ms").isNull()
        | (F.col("duration_ms") <= 0)
    )
    .count()
)

record_check(
    "track_duration_valid",
    "WARN",
    invalid_duration,
)

# COMMAND ----------

missing_played_at = (
    events_df
    .filter(F.col("played_at").isNull())
    .count()
)

record_check(
    "played_at_not_null",
    "FAIL",
    missing_played_at,
)

# COMMAND ----------

quality_df = spark.createDataFrame(
    quality_results
)

# COMMAND ----------

display(
    quality_df.orderBy(
        "status",
        "check_name",
    )
)

# COMMAND ----------

failed_checks = [
    result
    for result in quality_results
    if result["status"] == "FAIL"
]

# COMMAND ----------

failed_checks = [
    result
    for result in quality_results
    if result["status"] == "FAIL"
]

if failed_checks:
    failed_names = [
        result["check_name"]
        for result in failed_checks
    ]

    raise Exception(
        "Silver data quality checks failed: "
        + ", ".join(failed_names)
    )
else:
    print(
        "✅ All critical Silver data quality checks passed."
    )