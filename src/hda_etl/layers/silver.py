import json
import logging
from pathlib import Path
from typing import Any

import polars as pl

from hda_etl.schema import EXPECTED_DTYPES_FLOWS, EXPECTED_DTYPES_MIX

logger = logging.getLogger(__name__)


def transform_str_to_datetime(
    df: pl.DataFrame, cols: list[str] | None = None
) -> pl.DataFrame:
    """Parse ISO8601 timestamp strings into UTC Polars Datetime columns.

    Args:
        df: Input Polars DataFrame containing string timestamp columns.
        cols: List of column names to parse. If ``None``, defaults to
            ["datetime", "updatedAt", "ingestion_timestamp"].

    Returns:
        A new Polars DataFrame where each column in ``cols`` has been
        converted from an ISO8601 string (with trailing ``Z``) to a
        timezone-aware ``Datetime(time_zone="UTC")`` column.

    """
    if cols is None:
        cols = ["datetime", "updatedAt", "ingestion_timestamp"]

    return df.with_columns(
        pl.col(c)
        .str.strptime(pl.Datetime(time_zone="UTC"), "%Y-%m-%dT%H:%M:%S.%3fZ")
        .alias(c)
        for c in cols
    )


def add_date_partition_cols(df: pl.DataFrame) -> pl.DataFrame:
    """Add partitioning columns `year`, `month`, and `day`.

    The columns are derived from the existing ``datetime`` column and are
    convenient for downstream partitioned writes and comparisons.

    Returns:
        DataFrame with new integer columns: ``year``, ``month``, ``day``.

    """
    return df.with_columns(
        [
            pl.col("datetime").dt.year().alias("year"),
            pl.col("datetime").dt.month().alias("month"),
            pl.col("datetime").dt.day().alias("day"),
        ]
    )


def cast_to_explicit_dtypes(
    df: pl.DataFrame, expected_dtypes: dict[str, Any]
) -> pl.DataFrame:
    """Cast columns to explicit dtypes.

    Args:
        df: Input Polars DataFrame.
        expected_dtypes: Dictionary mapping column names to Polars dtypes.

    Returns:
        A new Polars DataFrame where each column in ``expected_dtypes`` is
        present and cast to the specified dtype.

    """
    missing_columns = [
        (name, dtype)
        for name, dtype in expected_dtypes.items()
        if name not in df.columns
    ]
    if missing_columns:
        df = df.with_columns(
            [pl.lit(None, dtype=dtype).alias(name) for name, dtype in missing_columns]
        )
    return df.select(
        pl.col(name).cast(dtype) for name, dtype in expected_dtypes.items()
    )


def flatten_mix(payloads: pl.DataFrame) -> pl.DataFrame:
    """Flatten a 'mix' payload DataFrame into a typed, wide layout.

    This function expects a DataFrame whose rows are the raw JSON payloads
    (as returned by :func:`read_bronze_json`). It explodes the ``history``
    array, unnests the nested ``mix`` struct, normalizes storage and flows
    fields into top-level columns, and enforces a stable set of dtypes.

    Args:
        payloads: Polars DataFrame containing the raw payload objects.

    Returns:
        A Polars DataFrame with one row per ``zone``/``datetime`` and the
        expected typed columns (suitable for downstream aggregation).

    """
    if payloads.is_empty():
        return pl.DataFrame(schema=EXPECTED_DTYPES_MIX)

    return (
        payloads.explode("history", empty_as_null=True)
        .unnest("history")
        .unnest("mix")
        .pipe(
            transform_str_to_datetime, ["datetime", "updatedAt", "ingestion_timestamp"]
        )
        .pipe(add_date_partition_cols)
        .with_columns(
            pl.col("hydro storage")
            .struct.field("charge")
            .alias("hydro_storage_charge"),
            pl.col("hydro storage")
            .struct.field("discharge")
            .alias("hydro_storage_discharge"),
            pl.col("battery storage")
            .struct.field("charge")
            .alias("battery_storage_charge"),
            pl.col("battery storage")
            .struct.field("discharge")
            .alias("battery_storage_discharge"),
            pl.col("flows").struct.field("imports").alias("flows_imports"),
            pl.col("flows").struct.field("exports").alias("flows_exports"),
        )
        .pipe(cast_to_explicit_dtypes, EXPECTED_DTYPES_MIX)
        .unique(subset=["zone", "datetime"], keep="last")
        .sort(["zone", "datetime"])
    )


def flatten_flows(payloads: pl.DataFrame) -> pl.DataFrame:
    """Flatten a 'flows' payload DataFrame into a typed, wide layout.

    Explodes the ``history`` array and unnests ``import``/``export``
    sub-structures (using ``separator="_"`` to avoid name collisions).
    Dynamically discovers import/export columns, casts them to float, and
    returns a stable, typed DataFrame with partition columns.

    Args:
        payloads: Polars DataFrame containing the raw flows payloads.

    Returns:
        A Polars DataFrame with import/export columns and date partition
        columns added and typed.

    """
    if payloads.is_empty():
        return pl.DataFrame(schema=EXPECTED_DTYPES_FLOWS)

    wide_df = (
        payloads.explode("history", empty_as_null=True)
        .unnest("history")
        .unnest("import", separator="_")
        .unnest("export", separator="_")
    )
    import_cols = [c for c in wide_df.columns if c.startswith("import_")]
    export_cols = [c for c in wide_df.columns if c.startswith("export_")]

    long_df = (
        wide_df.unpivot(
            index=[
                "zone",
                "datetime",
                "updatedAt",
                "ingestion_timestamp",
                "temporalGranularity",
            ],
            on=[*import_cols, *export_cols],
            value_name="power_mw",
        )
        .drop_nulls(subset=["power_mw"])
        .with_columns(
            pl.col("variable").str.split("_").list.get(0).alias("direction"),
            pl.col("variable").str.split("_").list.get(1).alias("flow_zone"),
        )
    )
    return (
        long_df.pipe(
            transform_str_to_datetime, ["datetime", "updatedAt", "ingestion_timestamp"]
        )
        .pipe(add_date_partition_cols)
        .pipe(cast_to_explicit_dtypes, EXPECTED_DTYPES_FLOWS)
        .unique(subset=["zone", "datetime", "direction", "flow_zone"], keep="last")
        .sort(["zone", "datetime", "direction", "flow_zone"])
    )


def read_bronze_json(bronze_root: Path, data_type: str) -> pl.DataFrame:
    """Read bronze JSON payload files into a Polars DataFrame.

    This will search recursively under ``bronze_root / data_type`` and
    collect all ``*.json`` payload files while skipping metadata files
    ending with ``.metadata.json``. Each JSON file is loaded and appended
    as an individual payload object; the returned Polars DataFrame uses
    ``strict=False`` to allow heterogeneous row structures.

    Args:
        bronze_root: Root directory that contains the bronze partitions.
        data_type: Subdirectory name (e.g., ``"electricity_mix"`` or
            ``"electricity_flows"``) to read.

    Returns:
        A Polars DataFrame constructed from the JSON payload objects. If no
        payload files are found, an empty DataFrame is returned.

    """
    bronze_path = bronze_root / data_type
    payloads = []
    logger.debug(f"Reading bronze JSON files from {bronze_path}")
    # Support partitioned layout (year/month/day/*.json) by searching recursively.
    json_files = sorted(bronze_path.rglob("*.json"))
    if not json_files:
        logger.warning(f"No bronze JSON files found under {bronze_path}")
        return pl.DataFrame([])

    logger.debug(f"Found {len(json_files)} bronze JSON files")
    for file in json_files:
        with open(file) as f:
            payload = json.load(f)
            payloads.append(payload)

    return pl.DataFrame(payloads, strict=False)
