# HDA — Electricity Maps ETL Pipeline

A Python ETL project that ingests Electricity Maps market data for France and turns it into a Bronze → Silver → Gold medallion pipeline using Polars and Delta/Parquet storage.

## Overview

This repository downloads the raw hourly generation mix and cross-border flow payloads for a target zone, stores the immutable API responses in Bronze, normalizes them into typed Silver tables, and then computes business-level Gold aggregates such as daily net imports, daily net exports, and relative generation mix.

The implementation is designed to be simple, auditable, and idempotent: each ingestion is written as a new raw file, transformations are explicit, and downstream tables are built from the normalized silver layer rather than directly from the API JSON.

---

## Installation and environment setup

Requirements:

- Python 3.11+
- A valid Electricity Maps API key
- A local environment

Setup:

```bash
git clone https://github.com/TCACastelijns/hda-electricity-maps-etl.git
cd hda-electricity-maps-etl
python -m venv .venv
source .venv/bin/activate
pip install -e .
```

Set the API key:

```bash
cp .env.example .env
export ELECTRICITY_MAPS_API_KEY="<your-api-key>"
```

If you are using a different API base URL, set it in the configuration object or environment value used by the project.

---

## How to run the pipeline

The pipeline entrypoint is the `run_pipeline` function in the project’s orchestration layer. It performs the end-to-end flow for a single zone.

Example:

```python
from pathlib import Path
from hda_etl.config import Settings
from hda_etl.pipeline import run_pipeline

settings = Settings(
    api_key="your-api-key",
    base_url="https://api.electricitymaps.com/v4",
    request_timeout_seconds=30,
)

run_pipeline(
    zone="FR",
    data_root=Path("data"),
    settings=settings,
)
```

Equivalent CLI-style flow (depending on your project entrypoint):

```bash
source .venv/bin/activate
hda-etl run --data-root data --zone FR
```

The run sequence is:

1. Fetch raw market data for mix and flows.
2. Write immutable Bronze files with ingestion metadata embedded in the JSON.
3. Read Bronze JSON files and flatten nested payloads into Silver tables.
4. Write Silver tables to Delta and Parquet partitions.
5. Join zone metadata and compute daily Gold metrics.
6. Persist Gold results as Delta and Parquet outputs.

---

## Design decisions

### 1. Bronze is immutable

Raw API payloads are written once and never edited in-place. This preserves lineage and allows transformation replay if data corrections or business logic changes.

### 2. Silver normalizes the source payload

The API payloads for Electricity Maps are nested (`history`, `mix`, `import`, `export`, and storage sub-objects). Silver flattens and types these structures into stable, explicit tables.

### 3. Gold is business-oriented

Gold is not a copy of raw data. It is an aggregation layer meant for reporting and BI: daily import/export balances and relative electricity mix by source.

### 4. Partitioning uses business-relevant keys

- Bronze: ingestion timestamp (`year`, `month`, `day`)
- Silver: data timestamp (`year`, `month`, `day`)
- Gold: daily date (`date`)

This aligns with the ETL assignment and makes incremental refreshes easier to reason about.

### 5. Idempotency and deduplication

The project uses business keys to avoid duplicate logical rows when the same ingestion is re-run. This is especially important for incremental jobs.

---

## Silver schema descriptions

### Silver mix table

The flattened mix dataset contains one row per zone and timestamp with source-level generation and storage information.

Typical columns:

- `zone`: target zone, for example `FR`
- `datetime`: reference timestamp for the data
- `updatedAt`: last update timestamp from the API payload
- `ingestion_timestamp`: when the bronze file was ingested
- `temporalGranularity`: `hourly`
- `isEstimated`: boolean estimation flag
- `estimationMethod`: source estimation method
- `breakdownType`: schema type for the underlying mix breakdown
- generation/source columns such as `nuclear`, `wind`, `solar`, `hydro`, `gas`, etc.
- storage columns such as `hydro_storage_charge`, `hydro_storage_discharge`
- flow columns such as `flows_imports`, `flows_exports`
- partition columns: `year`, `month`, `day`

### Silver flow table

The flow table is normalized into a long format with one row per zone/timestamp/direction/counterparty pair.

Typical columns:

- `zone`: target zone
- `datetime`: reference data timestamp
- `updatedAt`: API update timestamp
- `ingestion_timestamp`: bronze ingestion timestamp
- `temporalGranularity`: usually `hourly`
- `direction`: `import` or `export`
- `flow_zone`: neighboring zone, such as `DE`
- `power_mw`: hourly power flow in MW
- partition columns: `year`, `month`, `day`

This schema makes downstream netting and aggregation much simpler because each row is already in a consistent long form.

---

## Gold schema descriptions

### Daily net import/export tables

The gold layer computes one row per target zone and date, grouped by counterparty zone.

Example fields:

- `zone`: target market zone
- `date`: daily logical date
- `source_zone` or `destination_zone`: counterpart zone involved in trade
- `imports_mwh` or `exports_mwh`: total daily net electricity amount in MWh
- `zone_name` / `source_zone_name`: human-readable metadata for enrichment
- `reference_datetimes`: ingestion timestamps used for provenance

The model does not double-count counter-flows. A single bilateral hourly pair can only contribute to one channel: net import or net export.

### Relative mix table

This table describes how much each generation source contributed to the total daily mix for a zone.

Typical fields:

- `zone`
- `date`
- `injection`-style percentage columns such as `nuclear_pct`, `wind_pct`, `solar_pct`
- `zone_name`
- `ingestion_timestamp`

The percentages are computed from the day’s aggregate source values and are rounded to a stable precision.

---

## Orchestration logic

The orchestration layer coordinates the full ETL flow in a single function: `run_pipeline`.

Responsibilities of the orchestration function:

1. Create a configured API client.
2. Download the mix and flow payloads for the target zone.
3. Persist raw payloads to Bronze using `write_raw_response`.
4. Read Bronze JSON files using the bronze reader.
5. Flatten nested structures into Silver tables with `flatten_mix` and `flatten_flows`.
6. Write Delta and Parquet outputs for both silver datasets.
7. Fetch zone metadata and enrich the result with `transform_metadata` and `join_metadata`.
8. Build Gold tables by day using `build_daily_net` and `build_daily_relative_mix`.
9. Persist Gold outputs with the shared storage utility.

This keeps the project modular while still making the full ETL run easy to reason about.

---

## Incremental ingestion logic

This project is designed for incremental ingestion and reruns.

Key ideas:

- Bronze is append-only; each response is saved under a unique timestamped file name.
- Silver and Gold rely on business keys and time-partition columns to deduplicate updates.
- Partial refreshes can reuse the same pattern by re-reading recent windows and writing new Bronze files for those windows.
- Since the project writes by partition (`year`, `month`, `day`), daily or hourly re-runs can be ingested without rewriting the full historical dataset.

Recommended operational pattern:

- keep a small overlap window during scheduled reruns, for example the last 24–48 hours,
- allow the bronze layer to accumulate new raw files,
- rebuild the impacted silver and gold partitions from the new data,
- use deduplication and partition pruning to maintain pipeline efficiency.

This gives you a safe, low-risk ingestion model for API data that may be revised retroactively.

---

## Data quality and reliability notes

The project includes explicit handling for:

- malformed or failed HTTP responses,
- non-200 API statuses,
- nested JSON flattening issues,
- and duplicate business rows when reprocessing data.

The design prefers explicit schema coercion and stable output columns, rather than dynamic typing that changes between runs.

---

## Testing

The project contains focused unit tests for the bronze writer, silver transformations, and gold netting logic.

Run tests with:

```bash
source .venv/bin/activate
pytest -q
```

---

## Why Polars instead of pandas or PySpark?

I chose Polars for this project because the workload is a medium-sized, single-zone ETL with nested JSON payloads and explicit schema transformation, not a cluster-scale distributed analytics problem.

### Why Polars fits this project well

- Fast local execution: Polars is highly optimized for columnar processing and performs very well for hourly and daily data in a local Python environment.
- Clear schema control: the project relies on strong typing and explicit column definitions, which maps naturally to Polars DataFrames.
- Natural handling of nested JSON: the Bronze-to-Silver step involves flattening nested payloads, which Polars handles elegantly with `unnest`, `explode`, and `struct` access.
- Lower operational overhead: unlike PySpark, there is no cluster or Spark-session setup cost for a project that runs in a standard Python environment.
- Better developer ergonomics for this use case: the transformation code remains readable and compact while still being fast enough for daily or multi-day refresh runs.

### Why not pandas?

Pandas is very good for quick analysis and notebook workflows, but for this kind of production ETL it is less attractive for a few reasons:

- It is less memory-efficient for larger tabular workloads than Polars.
- It is not as ergonomic for strongly typed, schema-first transformations across nested records.
- It can become slower and more cumbersome when the workflow includes repeated reshaping, filtering, and long format processing.

For an ETL that transforms nested API output into stable Silver tables and then aggregates into Gold tables, Polars provides a better speed-to-complexity trade-off.

### Why not PySpark?

PySpark would be a more appropriate choice if this project were scaling to:

- very large datasets,
- multi-node processing,
- distributed joins across many partitions,
- or a large enterprise data platform with cluster orchestration.

For this repository, PySpark would add operational complexity without a clear gain. The current workload is compact, consistent, and local. A single-node Polars pipeline is simpler to test, debug, and reason about while still matching the data engineering requirements of the assignment.

---

## Potential improvements

### Linting and static checks

The project already has a Ruff setup, but it is still close to a generic template. For a data pipeline, it is worth narrowing it to the rules that help with correctness, maintainability, and import hygiene.

This keeps the lint rules focused on the issues most relevant to code review and ETL maintainability: unused imports, unsafe patterns, unnecessary complexity, and docstring consistency without being overly noisy.

### Additional engineering recommendations

- Add a `pre-commit` hook to run `ruff check . --fix` and `pytest -q` before commits.
- Introduce stricter typing checks with `mypy` or Pyright for the transformation functions and settings objects.
- Add a small `schema` contract layer for Bronze/Silver payload validation so API drift is caught quickly.
- Add a manifest or checksum table for raw ingestions to make replay and lineage easier to audit.
- Expand the test suite with a real fixture-based end-to-end run for one representative multi-day batch.
- Add observability for retries, API rate-limit handling, and delayed data quality checks.
- Move the raw and processed data lake to S3-compatible storage, with Bronze/Silver/Gold partitions landing in separate AWS buckets or prefixes for production-scale persistence and access patterns.

These are the highest-value upgrades for a project that already has the core medallion flow working.

---

## Current CI pipeline

The project already includes a GitHub Actions workflow at [.github/workflows/ci.yml](.github/workflows/ci.yml). It is a simple but useful CI baseline for the ETL codebase.

The workflow currently runs on both push and pull_request events and executes the following steps:

```yaml
name: CI

on:
  push:
  pull_request:

jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with:
          python-version: "3.11"
      - run: python -m pip install --upgrade pip
      - run: pip install -e ".[dev]"
      - run: ruff format src tests
      - run: ruff check src tests
      - run: pytest -q
```

This gives the project a fast default safety net for:

- dependency installation problems,
- broken imports,
- failing unit tests,
- and formatting or lint regressions.

It is intentionally lightweight and suitable for a project of this size, while still helping catch the most common ETL issues before merge. 
