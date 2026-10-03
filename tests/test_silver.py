from datetime import UTC, datetime

import polars as pl
from polars.testing import assert_frame_equal

from hda_etl.layers.silver import (
    add_date_partition_cols,
    flatten_flows,
    flatten_mix,
    transform_str_to_datetime,
)
from hda_etl.schema import EXPECTED_DTYPES_FLOWS, EXPECTED_DTYPES_MIX


def test_transform_str_to_datetime_and_partition_cols():
    df = pl.DataFrame(
        {
            "datetime": ["2026-09-01T00:00:00.000Z"],
            "updatedAt": ["2026-09-01T00:15:00.000Z"],
            "ingestion_timestamp": ["2026-09-01T01:00:00.000Z"],
        }
    )

    transformed = transform_str_to_datetime(
        df, ["datetime", "updatedAt", "ingestion_timestamp"]
    )
    expected = pl.DataFrame(
        {
            "datetime": [datetime(2026, 9, 1, 0, tzinfo=UTC)],
            "updatedAt": [datetime(2026, 9, 1, 0, 15, tzinfo=UTC)],
            "ingestion_timestamp": [datetime(2026, 9, 1, 1, tzinfo=UTC)],
        }
    )
    assert_frame_equal(transformed, expected)

    partitioned = add_date_partition_cols(transformed)
    expected_partitioned = pl.DataFrame(
        [
            (
                datetime(2026, 9, 1, 0, tzinfo=UTC),
                datetime(2026, 9, 1, 0, 15, tzinfo=UTC),
                datetime(2026, 9, 1, 1, tzinfo=UTC),
                2026,
                9,
                1,
            )
        ],
        schema={
            "datetime": pl.Datetime(time_zone="UTC"),
            "updatedAt": pl.Datetime(time_zone="UTC"),
            "ingestion_timestamp": pl.Datetime(time_zone="UTC"),
            "year": pl.Int32,
            "month": pl.Int8,
            "day": pl.Int8,
        },
        orient="row",
    )
    assert_frame_equal(partitioned, expected_partitioned)


def test_flatten_mix_matches_expected_schema():
    payloads = pl.DataFrame(
        [
            {
                "zone": "FR",
                "temporalGranularity": "hourly",
                "history": [
                    {
                        "datetime": "2026-09-01T00:00:00.000Z",
                        "updatedAt": "2026-09-01T00:15:00.000Z",
                        "ingestion_timestamp": "2026-09-01T01:00:00.000Z",
                        "mix": {
                            "nuclear": 35.0,
                            "wind": 10.0,
                            "solar": 7.5,
                            "hydro storage": {"charge": 2.0, "discharge": 1.5},
                            "battery storage": {"charge": 0.5, "discharge": 0.25},
                            "flows": {"imports": 11.0, "exports": 4.0},
                            "isEstimated": False,
                            "estimationMethod": "default",
                            "breakdownType": "total",
                        },
                    }
                ],
            }
        ]
    )

    result = flatten_mix(payloads)

    expected = pl.DataFrame(
        {
            "zone": ["FR"],
            "datetime": [datetime(2026, 9, 1, 0, tzinfo=UTC)],
            "updatedAt": [datetime(2026, 9, 1, 0, 15, tzinfo=UTC)],
            "ingestion_timestamp": [datetime(2026, 9, 1, 1, tzinfo=UTC)],
            "isEstimated": False,
            "estimationMethod": "default",
            "breakdownType": "total",
            "temporalGranularity": ["hourly"],
            **{
                k: [None]
                for k in [
                    "oil",
                    "hydro",
                    "gas",
                    "biomass",
                    "geothermal",
                    "unknown",
                    "coal",
                ]
            },
            "nuclear": [35.0],
            "wind": [10.0],
            "solar": [7.5],
            "hydro_storage_charge": [2.0],
            "hydro_storage_discharge": [1.5],
            "battery_storage_charge": [0.5],
            "battery_storage_discharge": [0.25],
            "flows_imports": [11.0],
            "flows_exports": [4.0],
            "year": [2026],
            "month": [9],
            "day": [1],
        },
        schema=EXPECTED_DTYPES_MIX,
    )
    assert_frame_equal(result, expected)


def test_flatten_flows_matches_expected_schema():
    payloads = pl.DataFrame(
        [
            {
                "zone": "FR",
                "temporalGranularity": "hourly",
                "history": [
                    {
                        "datetime": "2026-09-01T00:00:00.000Z",
                        "updatedAt": "2026-09-01T00:15:00.000Z",
                        "ingestion_timestamp": "2026-09-01T01:00:00.000Z",
                        "import": {"DE": 10.0},
                        "export": {"DE": 3.0},
                    }
                ],
            }
        ]
    )

    result = flatten_flows(payloads)
    expected = pl.DataFrame(
        {
            "zone": ["FR", "FR"],
            "datetime": [
                datetime(2026, 9, 1, 0, tzinfo=UTC),
                datetime(2026, 9, 1, 0, tzinfo=UTC),
            ],
            "updatedAt": [
                datetime(2026, 9, 1, 0, 15, tzinfo=UTC),
                datetime(2026, 9, 1, 0, 15, tzinfo=UTC),
            ],
            "ingestion_timestamp": [
                datetime(2026, 9, 1, 1, tzinfo=UTC),
                datetime(2026, 9, 1, 1, tzinfo=UTC),
            ],
            "temporalGranularity": ["hourly", "hourly"],
            "direction": ["export", "import"],
            "flow_zone": ["DE", "DE"],
            "power_mw": [3.0, 10.0],
            "year": [2026, 2026],
            "month": [9, 9],
            "day": [1, 1],
        },
        schema=EXPECTED_DTYPES_FLOWS,
    )
    assert_frame_equal(result, expected)
