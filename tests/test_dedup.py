import json
from datetime import datetime, timedelta

from labs.dedup import run_dedup


def _write_events(directory, name, events):
    directory.mkdir(parents=True, exist_ok=True)
    with open(directory / name, "w") as f:
        for e in events:
            f.write(json.dumps(e) + "\n")


def _events(start, n, prefix="e"):
    return [
        {
            "event_id": f"{prefix}{i}",
            "user_id": f"u{i % 3}",
            "event_time": (start + timedelta(minutes=i)).isoformat(),
            "amount": 10.0,
        }
        for i in range(n)
    ]


def test_replaying_the_same_hour_twice_adds_no_duplicates(spark, tmp_path):
    src, out, ckpt = tmp_path / "in", str(tmp_path / "out"), str(tmp_path / "ckpt")
    hour = _events(datetime(2026, 10, 1, 12, 0), 60)

    # The batch arrives once with an in-batch retry of 10 events...
    _write_events(src, "batch-1.json", hour + hour[:10])
    run_dedup(spark, str(src), out, ckpt)
    assert spark.read.format("delta").load(out).count() == 60

    # ...then the producer replays the whole hour as a new file.
    _write_events(src, "batch-1-replay.json", hour)
    run_dedup(spark, str(src), out, ckpt)

    result = spark.read.format("delta").load(out)
    assert result.count() == 60
    assert result.select("event_id").distinct().count() == 60


def test_new_events_still_flow_through(spark, tmp_path):
    src, out, ckpt = tmp_path / "in", str(tmp_path / "out"), str(tmp_path / "ckpt")
    _write_events(src, "a.json", _events(datetime(2026, 10, 1, 12, 0), 5, "a"))
    run_dedup(spark, str(src), out, ckpt)
    _write_events(src, "b.json", _events(datetime(2026, 10, 1, 12, 30), 5, "b"))
    run_dedup(spark, str(src), out, ckpt)

    assert spark.read.format("delta").load(out).count() == 10
