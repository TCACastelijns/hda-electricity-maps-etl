# HDA — Electricity Maps ETL Pipeline

A Python ETL project that ingests Electricity Maps market data for France and transforms it through a Bronze → Silver → Gold medallion architecture using Polars and Delta/Parquet storage.

## Key features

- Ingests Electricity Maps generation mix and cross-border flow data.
- Preserves raw API responses as immutable Bronze data.
- Normalizes nested API payloads into typed Silver tables.
- Produces business-oriented Gold aggregates for daily imports, exports, and relative generation mix.
- Uses Polars for local columnar transformations.
- Writes processed datasets to Delta and Parquet.
- Includes unit tests and GitHub Actions CI.
- Uses business keys to prevent duplicate logical rows during reprocessing.

## Architecture

```text
                    Electricity Maps API
                    generation mix + flows
                              │
                              ▼
                    ┌──────────────────┐
                    │ Bronze — Raw JSON│
                    │ Immutable payload│
                    │ + ingestion meta │
                    └────────┬─────────┘
                             │
                             ▼
                    ┌──────────────────┐
                    │ Silver — Normalized
                    │ Typed / flattened │
                    │ Delta + Parquet   │
                    └────────┬─────────┘
                             │
                             ▼
                    ┌──────────────────┐
                    │ Gold — Aggregates │
                    │ Daily imports     │
                    │ Daily exports     │
                    │ Relative mix     │
                    └──────────────────┘
```

The pipeline is designed to be simple, auditable, and replayable. Raw API responses are retained so downstream transformations can be rerun when transformation logic changes.

## Installation

### Requirements

- Python 3.11+
- A valid Electricity Maps API key
- A local Python environment

### Setup

```bash
git clone https://github.com/TCACastelijns/hda-electricity-maps-etl.git
cd hda-electricity-maps-etl

python3.11 -m venv .venv
source .venv/bin/activate

python -m pip install --upgrade pip setuptools wheel
python -m pip install -e ".[dev]"
```

Configure the API key using the project's supported configuration mechanism. For a local `.env` workflow:

```bash
cp .env.example .env
export ELECTRICITY_MAPS_API_KEY="<your-api-key>"
```

If a different Electricity Maps API base URL is required, configure it through the project's `Settings` object or the corresponding environment configuration.

## Quick start

The main orchestration entrypoint is `run_pipeline`.

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

If the installed CLI exposes the project entrypoint, the equivalent command is:

```bash
hda-etl run --data-root data --zone FR
```

The pipeline executes the following stages:

1. Fetch raw generation mix and cross-border flow data.
2. Write immutable Bronze JSON files with ingestion metadata.
3. Read Bronze JSON and flatten nested payloads.
4. Write normalized Silver data to Delta and Parquet.
5. Fetch zone metadata and enrich the normalized data.
6. Compute daily Gold aggregates.
7. Persist Gold results to Delta and Parquet.

## Example run

A representative France ingestion looks like:

```text
[INFO] 2026-10-03 11:39:17 - Connecting with ElectricityMapsClient.
[INFO] 2026-10-03 11:39:17 - Bronze data:
[INFO] 2026-10-03 11:39:17 - Fetch mix_payload and flow_payload JSON data
[INFO] 2026-10-03 11:39:17 - Loaded JSON to data/bronze/electricity_mix/year=2026/month=10/day=03/20261003T093917119234Z.json: 24 history records.
[INFO] 2026-10-03 11:39:17 - Loaded JSON to data/bronze/electricity_flows/year=2026/month=10/day=03/20261003T093917119234Z.json: 24 history records.
[INFO] 2026-10-03 11:39:17 - Silver data:
[INFO] 2026-10-03 11:39:17 - Flatten nested columns.
[INFO] 2026-10-03 11:39:22 - Loaded to data/silver/electricity_mix_delta: (24, 27)
[INFO] 2026-10-03 11:39:22 - Loaded to data/silver/electricity_flows_delta: (179, 11)
[INFO] 2026-10-03 11:39:22 - Gold data:
[INFO] 2026-10-03 11:39:22 - Loaded to data/gold/electricity_imports_daily_delta: (9, 7)
[INFO] 2026-10-03 11:39:22 - Loaded to data/gold/electricity_exports_daily_delta: (13, 7)
[INFO] 2026-10-03 11:39:22 - Loaded to data/gold/electricity_relative_mix_daily_delta: (2, 14)
```

The exact row counts depend on the API range and the hours returned by Electricity Maps. The Bronze → Silver → Gold progression remains the same.

## Data model

### Bronze

Bronze contains the original API responses and ingestion metadata.

Bronze data is partitioned by ingestion date:

```text
data/
└── bronze/
    ├── electricity_mix/
    │   └── year=YYYY/month=MM/day=DD/*.json
    └── electricity_flows/
        └── year=YYYY/month=MM/day=DD/*.json
```

Raw files are treated as immutable. This preserves source lineage and allows transformations to be replayed.

### Silver — electricity mix

The flattened mix dataset contains one row per target zone and timestamp with generation and storage information.

| Column | Description |
|---|---|
| `zone` | Target market zone, e.g. `FR` |
| `datetime` | Reference timestamp |
| `updatedAt` | Last update timestamp from the API |
| `ingestion_timestamp` | Bronze ingestion timestamp |
| `temporalGranularity` | Source temporal granularity, typically `hourly` |
| `isEstimated` | Source estimation flag |
| `estimationMethod` | Source estimation method |
| `breakdownType` | Schema type for the underlying mix breakdown |
| Generation columns | Sources such as `nuclear`, `wind`, `solar`, `hydro`, `gas`, etc. |
| Storage columns | Such as `hydro_storage_charge` and `hydro_storage_discharge` |
| Flow columns | Such as `flows_imports` and `flows_exports` |
| `year`, `month`, `day` | Data-date partition columns |

### Silver — electricity flows

The flow table uses a long format with one row per zone, timestamp, direction, and counterparty.

| Column | Description |
|---|---|
| `zone` | Target market zone |
| `datetime` | Reference timestamp |
| `updatedAt` | API update timestamp |
| `ingestion_timestamp` | Bronze ingestion timestamp |
| `temporalGranularity` | Usually `hourly` |
| `direction` | `import` or `export` |
| `flow_zone` | Neighboring/counterparty zone, e.g. `DE` |
| `power_mw` | Hourly power flow in MW |
| `year`, `month`, `day` | Data-date partition columns |

The long format simplifies downstream netting and aggregation.

### Gold — daily imports and exports

Gold contains daily business-level aggregates grouped by target zone and counterparty.

Typical fields include:

| Column | Description |
|---|---|
| `zone` | Target market zone |
| `date` | Daily logical date |
| `source_zone` / `destination_zone` | Counterparty zone |
| `imports_mwh` / `exports_mwh` | Daily electricity amount in MWh |
| `zone_name` / `source_zone_name` | Human-readable metadata |
| `reference_datetimes` | Provenance information |

The model is designed so bilateral counter-flows are netted rather than double-counted: a logical bilateral hourly pair contributes to either net imports or net exports.

### Gold — relative generation mix

The relative mix table describes the contribution of generation sources to the daily mix for a zone.

Typical fields include:

- `zone`
- `date`
- source percentage columns such as `nuclear_pct`, `wind_pct`, and `solar_pct`
- `zone_name`
- `ingestion_timestamp`

Percentages are calculated from the day's aggregate source values and rounded to a stable precision.

## Data processing and orchestration

The `run_pipeline` function coordinates the end-to-end ETL flow:

1. Create a configured Electricity Maps client.
2. Download generation mix and flow payloads.
3. Persist raw payloads with `write_raw_response`.
4. Read the Bronze JSON files.
5. Flatten nested payloads using `flatten_mix` and `flatten_flows`.
6. Persist Silver Delta and Parquet datasets.
7. Fetch and join zone metadata.
8. Build Gold datasets with `build_daily_net` and `build_daily_relative_mix`.
9. Persist Gold outputs with the shared storage utility.

Keeping these stages explicit makes the pipeline easier to test, debug, and replay.

## Design decisions

### Bronze is immutable

Raw API payloads are written as new ingestion records rather than edited in place. This preserves lineage and supports replay when source data or transformation logic changes.

### Silver normalizes the source

Electricity Maps returns nested structures including `history`, `mix`, `import`, `export`, and storage objects. Silver converts these structures into stable, typed tables suitable for downstream processing.

### Gold is business-oriented

Gold is an aggregation layer for reporting and BI rather than a copy of the source data. It exposes daily import/export balances and relative generation mix.

### Partitioning

The project uses partitions aligned with the semantics of each layer:

- **Bronze:** ingestion timestamp — `year`, `month`, `day`
- **Silver:** data timestamp — `year`, `month`, `day`
- **Gold:** logical daily date — `date`

### Idempotency and deduplication

The pipeline uses business keys to avoid duplicate logical rows when data is reprocessed. This is important for incremental runs and replay scenarios.

The exact business keys and conflict behavior should be kept aligned with the transformation implementation.

## Data quality and reliability

The pipeline includes explicit handling for:

- malformed or failed HTTP responses,
- non-200 API statuses,
- nested JSON flattening issues,
- duplicate business rows during reprocessing,
- explicit schema coercion and stable output columns.

For production use, useful additional checks include validation of required fields, expected temporal coverage, missing observations, and unexpected source-schema changes.

## Testing

Run the test suite with:

```bash
source .venv/bin/activate
pytest -q
```

The test suite contains focused tests for the Bronze writer, Silver transformations, and Gold netting logic.

A useful next step is to maintain deterministic fixtures for representative API responses so transformation tests can run without external API access. An end-to-end fixture-based test for a representative multi-day batch can then verify the complete Bronze → Silver → Gold flow.

## CI

The repository includes a GitHub Actions workflow at `.github/workflows/ci.yml`.

The CI pipeline runs on pushes and pull requests and provides checks for:

- dependency installation,
- formatting,
- Ruff linting,
- broken imports,
- and failing tests.

A CI configuration can use:

```yaml
- run: ruff format --check src tests
- run: ruff check src tests
- run: pytest -q
```

Using `ruff format --check` ensures CI verifies formatting without modifying the checkout.

## Why Polars?

Polars was selected for its columnar execution model, explicit schema handling, and suitability for local data transformation.

For this project, it provides a straightforward way to:

- flatten nested API payloads,
- enforce explicit schemas,
- reshape hourly observations,
- process long-form flow data,
- and calculate daily aggregates.

The workload is a single-zone ETL with hourly and daily data, so a local Polars pipeline avoids the operational complexity of a distributed processing framework.

### Why not PySpark?

PySpark becomes more compelling when the workload requires distributed processing, multi-node execution, very large datasets, or integration with a larger Spark-based platform.

For the current workload, those capabilities would introduce additional operational complexity without a clear requirement.

### Why not pandas?

Pandas would also be capable of handling a pipeline of this scale. Polars was chosen here because its columnar execution model, schema-oriented transformations, and nested-data operations fit the project's implementation style.

The choice is therefore specific to this workload rather than a claim that Polars is universally preferable to pandas.

## Potential improvements

The core ETL flow is already implemented. Possible next steps include:

### Developer tooling

- Add a `pre-commit` configuration for formatting, linting, and tests.
- Add stricter type checking with mypy or Pyright.
- Narrow Ruff rules to the checks most useful for this project.

### Data contracts and lineage

- Add a schema/contract layer for validating Bronze and Silver payloads.
- Add manifests or checksums for raw ingestions.
- Record source/API version information where available.
- Add explicit data-quality checks for missing or unexpected hourly observations.

### Reliability and observability

- Add retry and backoff handling for transient API failures.
- Add API rate-limit handling.
- Add metrics for ingestion duration, row counts, rejected records, and missing periods.
- Add fixture-based end-to-end tests.

### Production storage

For a production deployment, the local data lake could be moved to S3-compatible object storage, with Bronze, Silver, and Gold datasets separated by prefixes or buckets according to the required access and retention model.