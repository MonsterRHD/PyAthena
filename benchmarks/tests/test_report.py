import csv
import json

import pytest
from pyathena_bench.report import report, summarize


def test_report_recovers_an_interrupted_trial_without_claiming_success(tmp_path):
    event = {
        "event": "trial_start",
        "trial": "interrupted",
        "case_id": "case",
        "scale": "small",
        "warmup": False,
    }
    (tmp_path / "events.jsonl").write_text(json.dumps(event) + "\n")
    (tmp_path / "trials.jsonl").write_text('{"trial":')
    with pytest.warns(UserWarning, match="incomplete final record"):
        report(tmp_path)
    with (tmp_path / "summary.csv").open() as stream:
        row = next(csv.DictReader(stream))
    assert row["failed_trials"] == "1"
    assert row["successful_trials"] == "0"
    assert row["median_seconds"] == ""


def test_summary_excludes_failures_and_warmups_but_reports_them():
    base = {
        "case_id": "test",
        "scale": "small",
        "status": "ok",
        "warmup": False,
        "queries": [{"total_seconds": 2}],
        "successful_queries_per_second": 0.5,
        "rss_peak_bytes": 100,
        "max_threads": 3,
        "loop_lag_seconds": [0.01],
    }
    trials = [
        base,
        {**base, "warmup": True, "queries": [{"total_seconds": 999}]},
        {**base, "status": "row_count_mismatch", "queries": [{"total_seconds": 0.01}]},
        {
            "case_id": "unsupported",
            "scale": "small",
            "status": "unsupported",
            "reason": "Nested CSV",
        },
    ]
    rows = summarize(trials)
    assert rows[0]["median_seconds"] == 2
    assert rows[0]["successful_trials"] == rows[0]["failed_trials"] == 1
    assert rows[1]["unsupported"]
    assert rows[1]["median_seconds"] is None
