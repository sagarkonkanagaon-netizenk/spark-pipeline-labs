# spark-pipeline-labs

[![tests](https://github.com/sagarkonkanagaon-netizenk/spark-pipeline-labs/actions/workflows/ci.yml/badge.svg)](https://github.com/sagarkonkanagaon-netizenk/spark-pipeline-labs/actions/workflows/ci.yml)

Four small, runnable PySpark + Delta Lake labs, each on a problem that breaks real
pipelines. Every lab has tests that prove the property it claims, and they run on
every push.

These are the code behind the mini projects on my portfolio,
[The Data Foundry](https://data-foundry-sand.vercel.app).

| Lab | Problem | Property the tests prove |
|---|---|---|
| [01 · Streaming dedup](labs/dedup.py) | At-least-once sources redeliver events, sometimes hours late | Replaying the same hour twice adds **0** duplicates |
| [02 · SCD Type 2](labs/scd2.py) | Overwriting a dimension rewrites last quarter's reports | One current row per key, full history, **rerun is a no-op** |
| [03 · Skew salting](labs/salting.py) | One hot key sends most of a join to a single task | Same result as the plain join, hot key spread over **N** tasks |
| [04 · Idempotent backfill](labs/backfill.py) | A failed backfill restarted from the top double-loads history | Run the range twice → identical table; a killed run **resumes from the gap** |

## Run it

Needs Python 3.10+ and Java 17+.

```bash
python -m venv .venv && source .venv/bin/activate
pip install -e ".[dev]"
pytest
```

The first run downloads the Delta Lake jars. Everything runs on a local Spark session
(`labs/spark.py`), no cluster needed.

## How each lab works

**01 · Streaming dedup.** `dropDuplicatesWithinWatermark` on a stable `event_id`, so
Spark keeps only a bounded window of ids in state. The stream runs with
`trigger(availableNow=True)` into Delta, and the checkpoint is what makes a replay safe.

**02 · SCD Type 2.** Hash the tracked columns, keep the latest change per key, and do
it all in a single `MERGE`. Changed keys are staged twice: once to expire the current
row, once with a `NULL` merge key so they fall through to the insert.

**03 · Skew salting.** Find keys that own a large share of rows, then salt only those:
random bucket `0..N-1` on the large side, replicate N times on the small side. Other
keys keep salt `0`, so the small side barely grows.

**04 · Idempotent backfill.** One day per unit of work, written with Delta
`replaceWhere` so a rerun replaces instead of appending. A row-count audit runs before
a day is recorded in a ledger, and the next run skips finished days.

## Stack

PySpark 4.0 · Delta Lake 4.0 · pytest · GitHub Actions

---

More short notes in [til](https://github.com/sagarkonkanagaon-netizenk/til).
