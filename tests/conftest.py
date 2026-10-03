import pytest

from hda_etl.utils.logging_utils import configure_logging


@pytest.fixture(autouse=True)
def configure_test_logging():
    configure_logging()
