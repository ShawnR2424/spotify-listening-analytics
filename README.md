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