from datetime import UTC, date, datetime

import polars as pl
import pytest
from polars.testing import assert_frame_equal

from hda_etl.layers.gold import build_daily_net

INGESTION_TS = datetime(2026, 9, 1, 1, tzinfo=UTC)


@pytest.fixture
def flows():
    return pl.DataFrame(
        [
            {
                "zone": "FR",
                "datetime": datetime(2026, 9, 1, 0, tzinfo=UTC),
                "direction": "import",
                "flow_zone": "DE",
                "power_mw": 100.0,
                "ingestion_timestamp": INGESTION_TS,
            },
            {
                "zone": "FR",
                "datetime": datetime(2026, 9, 1, 1, tzinfo=UTC),
                "direction": "import",
                "flow_zone": "DE",
                "power_mw": 50.0,
                "ingestion_timestamp": INGESTION_TS,
            },
            {
                "zone": "FR",
                "datetime": datetime(2026, 9, 1, 0, tzinfo=UTC),
                "direction": "export",
                "flow_zone": "DE",
                "power_mw": 25.0,
                "ingestion_timestamp": INGESTION_TS,
            },
        ]
    )


@pytest.fixture
def meta_zone():
    return pl.DataFrame(
        {
            "zone": ["FR", "DE"],
            "zoneName": ["France", "Germany"],
        }
    )


@pytest.mark.parametrize(
    ("direction", "expected_mwh"),
    [
        (
            "import",
            150.0,
        ),
        ("export", 25.0),
    ],
)
def test_build_daily_net(flows, meta_zone, direction, expected_mwh):
    result = build_daily_net(flows, meta_zone, target_zone="FR", direction=direction)
    prefix = "source" if direction == "import" else "destination"
    expected = pl.DataFrame(
        {
            "zone": ["FR"],
            "date": [date(2026, 9, 1)],
            f"{prefix}_zone": ["DE"],
            f"{direction}s_mwh": [expected_mwh],
            "ingestion_timestamp": [INGESTION_TS],
            f"{prefix}_zone_name": ["Germany"],
            "zone_name": ["France"],
        },
    )
    assert_frame_equal(result, expected)
