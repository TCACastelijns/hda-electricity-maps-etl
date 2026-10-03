import argparse
from datetime import UTC, datetime
from pathlib import Path

from hda_etl.config import Settings
from hda_etl.pipeline import run_pipeline
from hda_etl.utils.logging_utils import configure_logging


def main() -> None:
    configure_logging()
    parser = argparse.ArgumentParser(
        description="Electricity Maps FR Bronze-Silver-Gold ETL"
    )
    sub = parser.add_subparsers(dest="command", required=True)

    run = sub.add_parser("run", help="Run the end-to-end pipeline")
    run.add_argument("--data-root", default="data", type=Path)
    run.add_argument("--zone", default="FR")
    run.add_argument("--base-url", default=None)

    args = parser.parse_args()

    settings = Settings.from_env()
    settings = Settings(
        api_key=settings.api_key,
        base_url=args.base_url or settings.base_url,
        zone=args.zone,
        request_timeout_seconds=settings.request_timeout_seconds,
        max_retries=settings.max_retries,
        retry_backoff_seconds=settings.retry_backoff_seconds,
    )
    run_pipeline(
        zone=args.zone,
        data_root=args.data_root,
        settings=settings,
        ingestion_timestamp=datetime.now(UTC),
    )
