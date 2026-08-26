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