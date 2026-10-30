from datetime import date

import pytest
from pyspark.sql import functions as F

from labs.backfill import backfill

START, END = date(2026, 1, 1), date(2026, 1, 10)


def _extract(spark):
    def extract(day):
        n = 5 + day.day  # a different row count per day
        return spark.range(n).select(F.col("id").alias("order_id"))

    return extract


def _counts(spark, path):
    rows = spark.read.format("delta").load(path).groupBy("event_date").count().collect()
    return {r.event_date: r["count"] for r in rows}


def test_running_the_whole_range_twice_changes_nothing(spark, tmp_path):
    target, ledger = str(tmp_path / "orders"), str(tmp_path / "ledger")
    backfill(spark, _extract(spark), target, ledger, START, END)
    first = _counts(spark, target)

    backfill(spark, _extract(spark), target, ledger, START, END, resume=False)

    assert _counts(spark, target) == first
    assert first == {date(2026, 1, d): 5 + d for d in range(1, 11)}


def test_a_killed_run_resumes_from_the_first_gap(spark, tmp_path):
    target, ledger = str(tmp_path / "orders"), str(tmp_path / "ledger")
    extract = _extract(spark)

    def flaky(day):
        if day == date(2026, 1, 6):
            raise RuntimeError("cluster lost")
        return extract(day)

    with pytest.raises(RuntimeError):
        backfill(spark, flaky, target, ledger, START, END)
    assert len(_counts(spark, target)) == 5

    processed = backfill(spark, extract, target, ledger, START, END)

    assert processed == [date(2026, 1, d) for d in range(6, 11)]
    assert _counts(spark, target) == {date(2026, 1, d): 5 + d for d in range(1, 11)}
