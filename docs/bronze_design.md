# Bronze Layer Design

## Purpose

The Bronze layer preserves data received from the Spotify Web API
with minimal modification.

Raw API responses are retained so downstream datasets can be rebuilt
if transformation requirements or source schemas change.

## Recently Played

**Source endpoint:** `/me/player/recently-played`

**Bronze table:** `spotify_analytics.bronze.spotify_recently_played_raw`

**Grain:** One row per API extraction response.

### Columns

- `batch_id` — Unique identifier for each API extraction.
- `ingested_at` — Timestamp when the response was received.
- `source_endpoint` — Spotify API endpoint that produced the response.
- `payload` — Complete raw JSON response returned by Spotify.

## Design Decision

The Bronze layer does not flatten individual listening events.

Nested Spotify data including tracks, albums, artists, context,
and pagination metadata is preserved in the raw payload.

Flattening, typing, deduplication, and entity extraction are handled
downstream in the Silver layer.