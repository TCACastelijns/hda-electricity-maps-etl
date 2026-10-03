import logging
from datetime import datetime
from pathlib import Path

from .config import Settings
from .layers.bronze import write_raw_response
from .layers.gold import build_daily_net, build_daily_relative_mix, transform_metadata
from .layers.silver import flatten_flows, flatten_mix, read_bronze_json
from .utils.api_client import ElectricityMapsClient
from .utils.storage import write_delta_and_parquet

logger = logging.getLogger(__name__)


def run_pipeline(
    zone: str,
    data_root: Path,
    settings: Settings,
    ingestion_timestamp: datetime | None = None,
) -> None:
    """Run the full bronze-to-gold ETL for a target zone.

    Args:
        zone: The Electricity Maps zone to fetch data for, such as "FR".
        data_root: Root directory used for bronze, silver, and gold outputs.
        settings: API configuration including credentials and request settings.
        ingestion_timestamp: Optional timestamp to stamp the bronze writes with;
            if omitted, the current UTC time is used.

    Fetches raw mix and flow payloads, writes them to bronze storage,
    flattens them into the silver layer, and aggregates the resulting data
    into daily import/export and relative mix gold tables.

    """
    logger.info("Connecting with ElectricityMapsClient.")
    client = ElectricityMapsClient(
        api_key=settings.api_key,
        base_url=settings.base_url,
        timeout_seconds=settings.request_timeout_seconds,
    )

    bronze_root = data_root / "bronze"
    silver_root = data_root / "silver"
    gold_root = data_root / "gold"

    logger.info("Bronze data:")
    logger.info("Fetch mix_payload and flow_payload JSON data")
    mix_payload, mix_url = client.fetch_mix(zone)
    flow_payload, flow_url = client.fetch_flows(zone)
    for payload, name, url in [
        (mix_payload, "electricity_mix", mix_url),
        (flow_payload, "electricity_flows", flow_url),
    ]:
        write_raw_response(
            bronze_root,
            name,
            payload,
            url,
            ingestion_timestamp,
        )

    logger.info("Silver data:")
    mix_df, flows_df = (
        read_bronze_json(bronze_root, f"electricity_{name}")
        for name in ["mix", "flows"]
    )

    logger.info("Flatten nested columns.")
    mix = flatten_mix(mix_df)
    flows = flatten_flows(flows_df)

    for df, name in [(mix, "mix"), (flows, "flows")]:
        write_delta_and_parquet(
            df,
            silver_root / f"electricity_{name}_delta",
            silver_root / f"electricity_{name}_parquet",
            ["year", "month", "day"],
        )

    logger.info("Gold data:")
    meta_zone_payload, _ = client.fetch_zone_metadata()
    meta_zone = transform_metadata(meta_zone_payload)

    imports, exports = (
        build_daily_net(flows, meta_zone, target_zone=zone, direction=direction)
        for direction in ["import", "export"]
    )
    relative_mix = build_daily_relative_mix(mix, meta_zone)
    for df, name in [
        (imports, "imports"),
        (exports, "exports"),
        (relative_mix, "relative_mix"),
    ]:
        write_delta_and_parquet(
            df,
            gold_root / f"electricity_{name}_daily_delta",
            gold_root / f"electricity_{name}_daily_parquet",
            ["date"],
        )
