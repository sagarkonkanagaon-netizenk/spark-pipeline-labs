"""Run every lab end to end on generated data and print what each one proves.

python -m labs.demo            # all labs
python -m labs.demo scd2       # one lab
"""

import argparse
import json
import tempfile
from datetime import date, datetime, timedelta
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from labs.backfill import backfill
from labs.dedup import run_dedup
from labs.salting import add_salt, find_hot_keys, salted_join
from labs.scd2 import apply_scd2
from labs.spark import get_spark


def _title(text: str) -> None:
    print(f"\n{'=' * 72}\n{text}\n{'=' * 72}")


def demo_dedup(spark: SparkSession, work: Path) -> None:
    _title("LAB 01 · Streaming dedup within a watermark")
    src = work / "dedup_in"
    src.mkdir()
    start = datetime(2026, 10, 1, 12, 0)
    hour = [
        {
            "event_id": f"e{i}",
            "user_id": f"u{i % 5}",
            "event_time": (start + timedelta(minutes=i)).isoformat(),
            "amount": 9.99,
        }
        for i in range(60)
    ]
    out, ckpt = str(work / "dedup_out"), str(work / "dedup_ckpt")
    (src / "batch.json").write_text("".join(json.dumps(e) + "\n" for e in hour + hour[:15]))
    run_dedup(spark, str(src), out, ckpt)
    (src / "replay.json").write_text("".join(json.dumps(e) + "\n" for e in hour))
    run_dedup(spark, str(src), out, ckpt)
    rows = spark.read.format("delta").load(out).count()
    print(f"input rows: {len(hour) * 2 + 15}  (60 events + 15 retries + full replay)")
    print(f"output rows: {rows}  → duplicates: {rows - 60}")


def demo_scd2(spark: SparkSession, work: Path) -> None:
    _title("LAB 02 · SCD Type 2 in one MERGE")
    path = str(work / "dim_customer")
    schema = "customer_id string, city string, tier string, updated_at timestamp"
    t0, t1 = datetime(2026, 1, 1), datetime(2026, 6, 1)
    apply_scd2(
        spark,
        path,
        spark.createDataFrame([("c1", "Pune", "gold", t0), ("c2", "Delhi", "silver", t0)], schema),
        "customer_id",
        ["city", "tier"],
    )
    batch = spark.createDataFrame([("c1", "Bengaluru", "gold", t1)], schema)
    apply_scd2(spark, path, batch, "customer_id", ["city", "tier"])
    apply_scd2(spark, path, batch, "customer_id", ["city", "tier"])  # rerun: no-op
    (
        spark.read.format("delta")
        .load(path)
        .select("customer_id", "city", "valid_from", "valid_to", "is_current")
        .orderBy("customer_id", "valid_from")
        .show(truncate=False)
    )


def demo_salting(spark: SparkSession, work: Path) -> None:
    _title("LAB 03 · Salting a skewed join")
    facts = spark.range(100_000).select(
        F.when(F.col("id") < 90_000, F.lit("c_hot"))
        .otherwise(F.concat(F.lit("c_"), (F.col("id") % 50).cast("string")))
        .alias("customer_id"),
        F.col("id").alias("order_id"),
    )
    dims = spark.createDataFrame(
        [("c_hot", "Hot Seller")] + [(f"c_{i}", f"Seller {i}") for i in range(50)],
        "customer_id string, name string",
    )
    hot = find_hot_keys(facts, "customer_id", share=0.2)
    print(f"hot keys: {hot}")
    buckets = 16
    spread = (
        add_salt(facts, "customer_id", hot, buckets)
        .where(F.col("customer_id").isin(hot))
        .groupBy("_salt")
        .count()
        .agg(F.count("*").alias("buckets"), F.max("count").alias("largest"))
        .first()
    )
    assert spread is not None
    print(
        f"hot key rows: 90,000 in 1 task  →  {spread['buckets']} tasks, "
        f"largest {spread['largest']:,}"
    )
    joined = salted_join(facts, dims, "customer_id", hot, buckets)
    print(f"joined rows: {joined.count():,} (same as the plain join)")


def demo_backfill(spark: SparkSession, work: Path) -> None:
    _title("LAB 04 · A backfill you can run twice")
    target, ledger = str(work / "orders"), str(work / "ledger")
    start, end = date(2026, 1, 1), date(2026, 1, 14)

    def extract(day: date):
        return spark.range(100 + day.day).select(F.col("id").alias("order_id"))

    def flaky(day: date):
        if day == date(2026, 1, 9):
            raise RuntimeError("executor lost")
        return extract(day)

    try:
        backfill(spark, flaky, target, ledger, start, end)
    except RuntimeError as err:
        print(f"run 1 failed on 2026-01-09: {err}")
    resumed = backfill(spark, extract, target, ledger, start, end)
    print(f"run 2 resumed and loaded {len(resumed)} days: {resumed[0]} → {resumed[-1]}")
    total = spark.read.format("delta").load(target).count()
    backfill(spark, extract, target, ledger, start, end, resume=False)
    again = spark.read.format("delta").load(target).count()
    print(f"rows after resume: {total:,}   after full rerun: {again:,}   (identical)")


DEMOS = {
    "dedup": demo_dedup,
    "scd2": demo_scd2,
    "salting": demo_salting,
    "backfill": demo_backfill,
}


def main() -> None:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawTextHelpFormatter
    )
    parser.add_argument(
        "labs", nargs="*", metavar="LAB", help=f"any of {', '.join(DEMOS)} (default: all)"
    )
    args = parser.parse_args()
    unknown = [name for name in args.labs if name not in DEMOS]
    if unknown:
        parser.error(f"unknown lab(s): {', '.join(unknown)}")

    spark = get_spark("labs-demo")
    spark.sparkContext.setLogLevel("ERROR")
    with tempfile.TemporaryDirectory() as tmp:
        for name in args.labs or DEMOS:
            work = Path(tmp) / name
            work.mkdir()
            DEMOS[name](spark, work)
    spark.stop()


if __name__ == "__main__":
    main()
