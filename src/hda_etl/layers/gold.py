import polars as pl

from hda_etl.schema import MIX_SOURCES


def transform_metadata(meta_zone_payload: dict) -> pl.DataFrame:
    """Convert the raw zone metadata payload into a standard lookup table.

    Args:
        meta_zone_payload: Raw metadata dictionary returned by the API, keyed by
            zone code.

    Returns:
        A DataFrame with the columns ``zone`` and ``zoneName``.

    """
    # There are also metadatafields about tier, access and isCommerciallyAvailable,
    # but these are not relevant for the ETL pipeline.
    metadata_cols = ["zone", "zoneName"]
    return (
        pl.DataFrame(meta_zone_payload)
        .transpose(include_header=True, header_name="zone")
        .unnest()
        .select(*metadata_cols)
    )


def join_metadata(
    df: pl.DataFrame, meta_zone: pl.DataFrame, zone_col: str = "zone"
) -> pl.DataFrame:
    """Join a DataFrame with zone metadata.

    Args:
        df: Input DataFrame containing a column with zone identifiers.
        meta_zone: Metadata DataFrame with columns `zone` and `zoneName`.
        zone_col: Name of the column in `df` that contains zone identifiers.

    Returns:
        A new DataFrame with the metadata joined on the specified zone column.

    """
    return df.join(meta_zone, left_on=zone_col, right_on="zone", how="left").rename(
        {"zoneName": f"{zone_col}_name"}
    )


def build_daily_net(
    flows: pl.DataFrame,
    meta_zone: pl.DataFrame,
    target_zone: str = "FR",
    direction: str = "import",
) -> pl.DataFrame:
    """Aggregate a target zone's daily import or export flow totals.

    Args:
        flows: Long-form flow DataFrame with columns such as ``zone``,
            ``datetime``, ``direction``, ``flow_zone``, ``power_mw``, and
            ``ingestion_timestamp``.
        meta_zone: Zone metadata lookup table with ``zone`` and ``zoneName``.
        target_zone: Zone to aggregate, defaulting to "FR".
        direction: Either "import" or "export"; determines the aggregation mode.

    Returns:
        A DataFrame with one row per target-zone/day/counterparty aggregate,
        including the respective zone-name metadata and the ingestion timestamp
        list used in the gold-layer reference values.

    """
    if flows is None or flows.is_empty():
        return pl.DataFrame()

    # Since the flows data has hourly granularity, each record represents the average
    # MW over the hour. To compute MWh, we can multiply by 1 hour, or just sum the MW
    # values over the day, since each is already an average over an hour.
    flow_zone_col = "source_zone" if direction == "import" else "destination_zone"
    return (
        flows.filter(
            (pl.col("zone") == target_zone) & (pl.col("direction") == direction)
        )
        .with_columns(pl.col("datetime").dt.date().alias("date"))
        .group_by(["zone", "date", "flow_zone", "ingestion_timestamp"])
        .agg(
            pl.col("power_mw").sum().alias("net_mwh"),
        )
        .with_columns(pl.col("net_mwh").alias(f"{direction}s_mwh"))
        .select(
            [
                "zone",
                "date",
                pl.col("flow_zone").alias(flow_zone_col),
                f"{direction}s_mwh",
                "ingestion_timestamp",
            ]
        )
        .pipe(join_metadata, meta_zone, zone_col=flow_zone_col)
        .pipe(join_metadata, meta_zone, zone_col="zone")
    )


def build_daily_relative_mix(
    mix: pl.DataFrame, meta_zone: pl.DataFrame
) -> pl.DataFrame:
    """Compute percentage contribution per energy source for each zone/day.

    Args:
        mix: Flattened silver mix DataFrame containing source columns such as
            ``nuclear``, ``wind``, and ``solar`` plus ``datetime`` and
            ``ingestion_timestamp``.
        meta_zone: Zone metadata lookup table with ``zone`` and ``zoneName``.

    Returns:
        A DataFrame with percentage shares for each source, joined to zone names.

    """
    return (
        mix.with_columns(pl.col("datetime").dt.date().alias("date"))
        .group_by(["zone", "date", "ingestion_timestamp"])
        .agg(
            *[pl.sum(c).alias(c) for c in MIX_SOURCES],
        )
        .with_columns(
            pl.sum_horizontal(*MIX_SOURCES).alias("total_mw"),
        )
        .with_columns(
            **{
                f"{c}_pct": (pl.col(c) / pl.col("total_mw") * 100).round(2)
                for c in MIX_SOURCES
            }
        )
        .pipe(join_metadata, meta_zone, zone_col="zone")
        .select(
            [
                "zone",
                *[f"{c}_pct" for c in MIX_SOURCES],
                "zone_name",
                "date",
                pl.col("ingestion_timestamp"),
            ]
        )
    )
