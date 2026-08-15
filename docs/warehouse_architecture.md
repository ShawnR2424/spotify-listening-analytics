# Architecture

## Overview

Spotify Listening Analytics uses a medallion lakehouse architecture
implemented in Databricks.

Data progresses through three layers:

### Bronze

Raw data collected from the Spotify Web API.

The Bronze layer preserves source data with minimal transformation and
adds ingestion metadata for traceability.

### Silver

Cleaned, standardized, deduplicated, and normalized Spotify data.

PySpark transformations convert raw Spotify responses into reusable
entities and event datasets.

### Gold

Analytics-ready models created using dbt.

The Gold layer contains dimensional models, fact tables, and analytical
marts designed to support listening behavior analysis.

## Data Flow

Spotify Web API
→ Python ingestion
→ Databricks Bronze
→ PySpark transformations
→ Databricks Silver
→ dbt transformations
→ Databricks Gold
→ Analytics and visualization
