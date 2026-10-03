import logging
import sys

LOG_FORMAT = "[%(levelname)s] %(asctime)s - %(message)s"


def configure_logging(level: int = logging.INFO) -> None:
    """Configure the project-wide logging format and output stream.

    Args:
        level: The minimum logging level to emit, such as ``logging.INFO`` or
            ``logging.DEBUG``.

    This helper is used at application startup so ETL steps emit consistent,
    timestamped log messages to stdout. In this project it supports the bronze,
    silver, and gold processing stages, making it easier to trace ingestion,
    transformation, and export progress during a full pipeline run.

    """
    logging.basicConfig(
        level=level,
        format=LOG_FORMAT,
        datefmt="%Y-%m-%d %H:%M:%S",
        stream=sys.stdout,
        force=True,
    )
