import json
from datetime import UTC, datetime

from hda_etl.layers.bronze import write_raw_response


def test_bronze_embeds_metadata_in_raw_payload(tmp_path, caplog):
    payload = {
        "zone": "FR",
        "temporalGranularity": "hourly",
        "unit": "MW",
        "history": [{"datetime": "2026-09-01T00:00:00.000Z", "import": {"DE": 10}}],
    }

    path = write_raw_response(
        tmp_path,
        "electricity_flows",
        payload,
        "https://example.test/v4/electricity-flows/past-range?zone=FR",
        datetime(2026, 9, 1, 1, tzinfo=UTC),
    )

    raw = json.loads(path.read_text())
    assert raw["zone"] == payload["zone"]
    assert raw["source_url"].startswith("https://example.test")
    assert raw["ingestion_timestamp"] == "2026-09-01T01:00:00.000Z"
    assert caplog.messages[-1].endswith(
        "electricity_flows/year=2026/month=09/day=01/20260901T010000000000Z.json: "
        "1 history records."
    )
