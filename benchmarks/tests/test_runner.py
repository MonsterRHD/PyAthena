import json
import time
from dataclasses import replace

from pyathena_bench.config import Settings
from pyathena_bench.runner import Observer, supervise


def successful_worker(pipe, payload):
    pipe.send({"event": "ready"})
    pipe.recv()
    time.sleep(0.05)
    pipe.send({"event": "result", "result": {"status": "ok"}})
    pipe.close()


def waiting_worker(pipe, payload):
    time.sleep(30)


def test_trial_uses_child_process_and_external_memory_samples(tmp_path):
    result = supervise(
        {"trial": "test"},
        tmp_path / "events.jsonl",
        replace(Settings(), timeout_seconds=20),
        successful_worker,
    )
    assert result["status"] == "ok"
    assert result["rss_peak_bytes"] >= result["rss_baseline_bytes"] > 0
    assert result["resource_samples"]
    events = [json.loads(line) for line in (tmp_path / "events.jsonl").read_text().splitlines()]
    assert events[0]["event"] == "ready"


def test_timeout_does_not_become_a_successful_timing(tmp_path):
    result = supervise(
        {"trial": "test"},
        tmp_path / "events.jsonl",
        replace(Settings(), timeout_seconds=0.1),
        waiting_worker,
    )
    assert result["status"] == "timeout"


def test_observer_keeps_first_completion_and_query_id():
    events = []

    class Pipe:
        def send(self, value):
            events.append(value)

    observer = Observer(Pipe())
    context = {}
    observer.before_start(
        {"QueryString": "SELECT 1", "ResultConfiguration": {"OutputLocation": "s3://out/"}}, context
    )
    observer.started({"QueryExecutionId": "q"}, context)
    response = {
        "QueryExecution": {
            "QueryExecutionId": "q",
            "Status": {"State": "SUCCEEDED"},
            "Statistics": {"DataScannedInBytes": 123},
        }
    }
    observer.polled(response)
    observer.polled(response)
    assert [e["event"] for e in events] == ["query", "athena"]
    assert events[0]["output"] == "s3://out/"
    assert events[1]["statistics"]["DataScannedInBytes"] == 123
