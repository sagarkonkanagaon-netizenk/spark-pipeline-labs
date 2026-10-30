# spark-pipeline-labs

[![ci](https://github.com/sagarkonkanagaon-netizenk/spark-pipeline-labs/actions/workflows/ci.yml/badge.svg)](https://github.com/sagarkonkanagaon-netizenk/spark-pipeline-labs/actions/workflows/ci.yml)
![python](https://img.shields.io/badge/python-3.10%20|%203.11%20|%203.12-3776AB)
![spark](https://img.shields.io/badge/spark-4.0-E25A1C)
![delta](https://img.shields.io/badge/delta%20lake-4.0-00ADD4)
[![license](https://img.shields.io/badge/license-MIT-green)](LICENSE)

**Production data-engineering patterns as runnable, tested PySpark + Delta Lake code.**

Each lab takes a problem that breaks real pipelines, fixes it with the pattern used in
production, and ships a test that **proves** the fix holds: no duplicates after a replay,
a merge that's safe to rerun, a backfill that resumes from where it died.

These are the working code behind the mini projects on
[The Data Foundry](https://data-foundry-sand.vercel.app).

## Labs

| | Lab | Problem | Property the tests prove |
|---|---|---|---|
| 01 | [Streaming dedup](labs/dedup.py) | At-least-once sources redeliver events, sometimes hours late | Replaying the same hour twice adds **0** duplicates |
| 02 | [SCD Type 2](labs/scd2.py) | Overwriting a dimension rewrites last quarter's reports | One current row per key, full history, **rerun is a no-op** |
| 03 | [Skew salting](labs/salting.py) | One hot key sends most of a join to a single task | Same rows as the plain join, hot key spread over **N** tasks |
| 04 | [Idempotent backfill](labs/backfill.py) | A failed backfill restarted from the top double-loads history | Full rerun → identical table; a killed run **resumes from the gap** |

How each one works, with diagrams: [docs/architecture.md](docs/architecture.md).

## Quickstart

Needs Python 3.10+ and Java 17+.

```bash
git clone https://github.com/sagarkonkanagaon-netizenk/spark-pipeline-labs
cd spark-pipeline-labs
python -m venv .venv && source .venv/bin/activate
make install
make demo           # run every lab end to end
make check          # lint, type check and tests, exactly what CI runs
```

Or with Docker, no local Java needed:

```bash
make docker
```

### What the demo prints

```text
LAB 01 · Streaming dedup within a watermark
input rows: 135  (60 events + 15 retries + full replay)
output rows: 60  → duplicates: 0

LAB 02 · SCD Type 2 in one MERGE
|c1 |Pune     |2026-01-01 00:00:00|2026-06-01 00:00:00|false|
|c1 |Bengaluru|2026-06-01 00:00:00|NULL               |true |
|c2 |Delhi    |2026-01-01 00:00:00|NULL               |true |

LAB 03 · Salting a skewed join
hot key rows: 90,000 in 1 task  →  16 tasks, largest 5,706
joined rows: 100,000 (same as the plain join)

LAB 04 · A backfill you can run twice
run 1 failed on 2026-01-09: executor lost
run 2 resumed and loaded 6 days: 2026-01-09 → 2026-01-14
rows after resume: 1,505   after full rerun: 1,505   (identical)
```

## Project layout

```text
labs/            one module per lab, plus spark.py (session) and demo.py (CLI)
tests/           one test file per lab, each asserting the lab's property
docs/            architecture and per-lab diagrams
.github/         CI (lint · types · Python 3.10-3.12 · Docker), templates, Dependabot
```

## Engineering standards

- **Every claim is a test.** 98% coverage, and CI fails if a property breaks.
- **Idempotent by design.** Every lab can be rerun on the same input with no change.
- **Quality gates in CI:** `ruff` lint and format, `mypy`, a test matrix across three
  Python versions, a demo smoke test and a Docker build.
- **Reproducible:** pinned Spark and Delta versions, Docker image, `make` targets.

## Roadmap

- [ ] Lab 05: schema drift detection and quarantine
- [ ] Lab 06: data-quality contracts that fail the job, not the dashboard
- [ ] Lab 07: Airflow DAG that orchestrates labs 01, 02 and 04

Ideas welcome: [open a lab request](https://github.com/sagarkonkanagaon-netizenk/spark-pipeline-labs/issues/new/choose).

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md). Please report security issues privately as
described in [SECURITY.md](SECURITY.md).

## License

[MIT](LICENSE) © 2026 Sagar Konkanagaon
