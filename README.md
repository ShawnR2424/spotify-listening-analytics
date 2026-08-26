# Spotify Listening Analytics

An end-to-end analytics engineering project that collects personal Spotify listening activity and transforms raw API responses into tested, analytics-ready datasets using Python, Databricks, PySpark, Delta Lake, dbt, and Databricks Lakeflow Jobs.

The project demonstrates a modern analytics engineering workflow including incremental ingestion, medallion architecture, nested JSON processing, dimensional modeling, data quality validation, dbt testing, and workflow orchestration.

## Architecture

```text
Spotify Web API
       │
       ▼
Python / Spotipy
Incremental ingestion
       │
       ▼
Databricks Bronze
Raw API responses
       │
       ▼
PySpark
Parse • Explode • Deduplicate
       │
       ▼
Databricks Silver
Clean events and reusable entities
       │
       ▼
Silver Data Quality
Uniqueness • Completeness • Referential integrity
       │
       ▼
dbt
       │
       ▼
Databricks Gold
Facts • Dimensions • Bridge tables • Analytics marts
       │
       ▼
Analytics-ready datasets

## Data Model

The pipeline separates source-oriented Silver models from analytics-oriented Gold models.

### Silver Layer

The Silver layer represents cleaned and reusable Spotify entities.

| Table | Grain | Purpose |
|---|---|---|
| `listening_events` | One row per unique track play | Clean, deduplicated listening history |
| `tracks` | One row per Spotify track | Reusable track metadata |
| `albums` | One row per Spotify album | Reusable album metadata |
| `artists` | One row per Spotify artist | Reusable artist metadata |
| `track_artists` | One row per track-artist relationship | Resolves the many-to-many relationship between tracks and artists |

### Gold Layer

The Gold layer is modeled for analytical consumption using dbt.

| Model | Grain | Purpose |
|---|---|---|
| `fct_listening_events` | One row per listening event | Core listening activity fact table |
| `dim_tracks` | One row per track | Track-level descriptive attributes |
| `dim_albums` | One row per album | Album-level descriptive attributes |
| `dim_artists` | One row per artist | Artist-level descriptive attributes |
| `bridge_track_artists` | One row per track-artist relationship | Connects tracks to one or more artists |
| `mart_daily_listening` | One row per listening date | Business-facing daily listening metrics |

### Gold Relationships

```text
                         dim_albums
                             ▲
                             │ album_id
                             │
fct_listening_events ─────► dim_tracks
         │                   │
         │                   │ track_id
         │                   ▼
         │            bridge_track_artists
         │                   │
         │                   │ artist_id
         │                   ▼
         │              dim_artists
         │
         └── event-level listening activity


fct_listening_events
         │
         │ aggregated by date
         ▼
mart_daily_listening

## Key Engineering Decisions

### Preserve Raw Spotify Responses in Bronze

Spotify API responses are stored in Bronze as complete JSON payloads rather than immediately flattened.

Each Bronze record contains:

- `batch_id`
- `ingested_at`
- `source_endpoint`
- raw `payload`

This preserves the original source data and allows downstream models to be rebuilt if transformation requirements change.

---

### Explicit Grain at Every Layer

Each dataset has a clearly defined grain.

Examples:

- Bronze recently played data: one row per API extraction response
- Silver `listening_events`: one row per track play
- Silver `tracks`: one row per track
- Gold `fct_listening_events`: one row per listening event
- Gold `mart_daily_listening`: one row per listening date

Explicit grain definitions help prevent accidental duplication and incorrect aggregations.

---

### Nested JSON Processing with PySpark

Spotify returns nested structures containing tracks, albums, artists, and playback context.

PySpark is used to:

1. Parse raw JSON
2. Convert nested JSON into typed Spark structures
3. Explode the listening-event array
4. Flatten relevant attributes
5. Preserve multi-valued artist relationships

This converts semi-structured API data into reusable analytical entities.

---

### Deterministic Listening Event IDs

Spotify does not provide a unique identifier for each individual track play.

A deterministic `listening_event_id` is therefore generated from:

`track_id + played_at`

using SHA-256 hashing.

The same listening event produces the same identifier across ingestion runs, allowing duplicate events from overlapping API responses to be detected reliably.

---

### Overlapping API Pulls and Idempotency

Incremental ingestion uses the latest successfully processed `played_at` timestamp as its checkpoint.

API responses can still overlap between runs.

Rather than requiring Bronze to be duplicate-free:

- Bronze preserves every API extraction
- Silver generates deterministic event IDs
- Silver deduplicates listening events

This allows the pipeline to safely reprocess overlapping source data without multiplying analytical records.

---

### Many-to-Many Track and Artist Relationships

A Spotify track may contain multiple artists.

Directly joining artists into the listening-event fact table could create fan-out:

```text
1 listening event
      ↓
2 artists
      ↓
2 resulting rows