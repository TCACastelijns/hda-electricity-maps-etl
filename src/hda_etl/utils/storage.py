import logging
from pathlib import Path

import polars as pl

logger = logging.getLogger(__name__)


def write_delta_and_parquet(
    df: pl.DataFrame,
    delta_path: Path,
    parquet_root: Path,
    partition_cols: list[str],
) -> None:
    """Write a Polars DataFrame as both Delta and partitioned Parquet outputs.

    Args:
        df: DataFrame to persist.
        delta_path: Target directory for the Delta table.
        parquet_root: Root directory for the partitioned Parquet files.
        partition_cols: Column names used to partition both outputs.

    """
    if df.is_empty():
        logger.info("DataFrame is empty. Skipping write operations.")
        return
    delta_path.parent.mkdir(parents=True, exist_ok=True)
    parquet_root.mkdir(parents=True, exist_ok=True)

    # Delta Lake table.
    df.write_delta(
        target=str(delta_path),
        mode="append",
        delta_write_options={"partition_by": partition_cols},
    )

    # Hive-style partitioned Parquet files.
    df.write_parquet(
        str(parquet_root / "data.parquet"),
        use_pyarrow=True,
        pyarrow_options={"partition_cols": partition_cols},
    )
    logger.info(f"Loaded to {delta_path}: {df.shape}")
