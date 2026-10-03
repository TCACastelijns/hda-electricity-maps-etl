import json
import logging
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)


def _partition_path(root: Path, ingestion_ts: datetime) -> Path:
    """Build the bronze partition path for a UTC ingestion timestamp.

    Args:
        root: Base path for the bronze dataset.
        ingestion_ts: Timestamp used to derive the year/month/day partition.

    Returns:
        A Path under root partitioned by year, month, and day.

    """
    ts = ingestion_ts.astimezone(UTC)
    return root / f"year={ts.year:04d}" / f"month={ts.month:02d}" / f"day={ts.day:02d}"


def write_raw_response(
    root: Path,
    signal: str,
    payload: dict[str, Any],
    source_url: str,
    ingestion_timestamp: datetime | None = None,
) -> Path:
    """Write a raw API payload to bronze storage with embedded metadata.

    Args:
        root: Root directory for bronze data.
        signal: Logical data signal name, e.g. "electricity_mix".
        payload: Raw JSON payload returned by the API.
        source_url: Original API URL used to fetch the payload.
        ingestion_timestamp: Optional UTC timestamp for the ingestion event;
            defaults to the current UTC time.

    Returns:
        The path to the JSON file written to disk.

    """
    ingestion_timestamp = ingestion_timestamp or datetime.now(UTC)
    directory = _partition_path(root / signal, ingestion_timestamp)
    directory.mkdir(parents=True, exist_ok=True)

    # To keep it consistent with the other timestamps in data.
    ingestion_timestamp_non_offset = ingestion_timestamp.replace(tzinfo=None)
    payload["ingestion_timestamp"] = (
        ingestion_timestamp_non_offset.isoformat(timespec="milliseconds") + "Z"
    )
    payload["source_url"] = source_url

    raw_bytes = json.dumps(payload, separators=(",", ":"), ensure_ascii=False).encode()
    filename = f"{ingestion_timestamp.strftime('%Y%m%dT%H%M%S%fZ')}"
    json_path = directory / f"{filename}.json"

    json_path.write_bytes(raw_bytes)
    logger.info(
        f"Loaded JSON to {json_path}: {len(payload['history'])} history records."
    )

    return json_path
