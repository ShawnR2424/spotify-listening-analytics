# Pipeline Orchestration

## Overview

The Spotify Listening Analytics pipeline uses Databricks Lakeflow Jobs
to orchestrate the transformation, validation, and analytics modeling
stages of the data pipeline.

The workflow ensures that downstream analytical models are only built
after upstream transformations complete successfully and Silver-layer
data quality checks pass.

## Pipeline Flow

Spotify Web API
→ Python ingestion
→ Databricks Bronze
→ Silver transformation
→ Silver entity modeling
→ Silver data quality validation
→ dbt Gold models and marts

## Lakeflow Job

The Databricks workflow is named:

`Spotify Analytics Pipeline`

The orchestrated portion of the pipeline currently contains four tasks:

### 1. Silver Transform

**Task:** `silver_transform_recently_played`

**Source:** GitHub

**Notebook:** `notebooks/02_silver_transform_recently_played.py`

**Purpose:**

- Read raw Spotify API responses from Bronze
- Parse nested JSON
- Explode listening events
- Standardize data types
- Generate deterministic listening event IDs
- Deduplicate listening events

### 2. Silver Entity Modeling

**Task:** `silver_build_entities`

**Depends on:** `silver_transform_recently_played`

**Notebook:** `notebooks/03_silver_build_entities.py`

**Purpose:**

Build reusable Silver entities including:

- Tracks
- Albums
- Artists
- Track-artist relationships

### 3. Silver Data Quality

**Task:** `silver_data_quality`

**Depends on:** `silver_build_entities`

**Notebook:** `notebooks/04_silver_data_quality.py`

**Purpose:**

Validate the Silver layer before downstream analytical models are built.

Checks include:

- Primary key uniqueness
- Required-field completeness
- Referential integrity
- Duplicate relationships
- Business-rule validation

Critical quality failures raise an exception and cause the task to fail.

### 4. dbt Gold Build

**Task:** `dbt_gold`

**Depends on:** `silver_data_quality`

**Command:**

`dbt build --select gold`

**Project directory:** `dbt`

**Purpose:**

Build and test the Gold analytical layer, including:

- Fact tables
- Dimensions
- Bridge tables
- Business-facing marts
- dbt data tests

## Dependency Design

The workflow follows a sequential dependency graph:

    silver_transform_recently_played
                  ↓
          silver_build_entities
                  ↓
           silver_data_quality
                  ↓
               dbt_gold

Each downstream task is configured to run only when its dependency
succeeds.

This makes the Silver data-quality task a quality gate between the
data-processing layer and the business-facing Gold layer.

If a critical Silver validation fails:

    Silver Data Quality
            ↓
          FAILED
            ↓
    dbt Gold does not run

This prevents known invalid data from propagating into analytical
models and downstream reporting.

## Version Control

The Databricks notebook and dbt tasks use the project's GitHub
repository as their source.

Pipeline runs therefore execute version-controlled transformation
logic rather than separate copies of the code maintained only inside
the Databricks workspace.

## Current Ingestion Process

Spotify API ingestion is currently executed separately using:

`ingestion/ingest_recently_played.py`

The script performs incremental extraction from the Spotify Web API
and writes raw API responses to the Databricks Bronze layer.

The Databricks orchestration workflow currently begins after Bronze
data has been ingested.

## Future Improvements

Planned improvements include:

- Moving the orchestration definition into version-controlled
  Databricks configuration
- Scheduling the pipeline
- Automating Spotify ingestion
- Adding pipeline monitoring and notifications
- Adding retry and failure-handling policies