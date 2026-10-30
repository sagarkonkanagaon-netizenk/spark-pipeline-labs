"""LAB 04: a backfill you can run twice.

Each unit of work is one day, written as an overwrite of exactly that day with
Delta's replaceWhere. Finished days are recorded in a small Delta ledger after a
row-count audit, so a killed job resumes from the first gap and a full rerun
leaves the target identical.
"""

from collections.abc import Callable, Iterator
from datetime import date, timedelta

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


def days(start: date, end: date) -> Iterator[date]:
    d = start
    while d <= end:
        yield d
        d += timedelta(days=1)


def _done_days(spark: SparkSession, ledger_path: str) -> set[date]:
    if not DeltaTable.isDeltaTable(spark, ledger_path):
        return set()
    return {r.day for r in spark.read.format("delta").load(ledger_path).collect()}


def load_day(
    spark: SparkSession,
    extract: Callable[[date], DataFrame],
    target_path: str,
    day: date,
    partition_col: str = "event_date",
) -> int:
    d = day.isoformat()
    df = extract(day).withColumn(partition_col, F.lit(d).cast("date"))
    expected = df.count()
    (
        df.write.format("delta")
        .mode("overwrite")
        .option("replaceWhere", f"{partition_col} = '{d}'")
        .partitionBy(partition_col)
        .save(target_path)
    )
    written = spark.read.format("delta").load(target_path).where(f"{partition_col} = '{d}'").count()
    if written != expected:
        raise RuntimeError(f"{d}: expected {expected} rows, found {written}")
    return written


def backfill(
    spark: SparkSession,
    extract: Callable[[date], DataFrame],
    target_path: str,
    ledger_path: str,
    start: date,
    end: date,
    resume: bool = True,
) -> list[date]:
    """Load every day in [start, end]. Returns the days processed in this run."""
    done = _done_days(spark, ledger_path) if resume else set()
    processed = []
    for day in days(start, end):
        if day in done:
            continue
        load_day(spark, extract, target_path, day)
        spark.createDataFrame([(day,)], "day date").write.format("delta").mode("append").save(
            ledger_path
        )
        processed.append(day)
    return processed
