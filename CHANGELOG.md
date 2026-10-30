# Changelog

All notable changes to this project are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and the project uses
[Semantic Versioning](https://semver.org/).

## [0.1.0] - 2026-10-30

### Added

- Lab 01: streaming dedup within an event-time watermark into Delta.
- Lab 02: SCD Type 2 dimension from a CDC feed in a single `MERGE`.
- Lab 03: salting only the hot keys of a skewed join.
- Lab 04: idempotent, resumable day-by-day backfill with `replaceWhere` and a row-count audit.
- `spark-labs` demo CLI that runs every lab end to end.
- CI: ruff, mypy, tests on Python 3.10 to 3.12 with coverage, demo smoke test and Docker build.

[0.1.0]: https://github.com/sagarkonkanagaon-netizenk/spark-pipeline-labs/releases/tag/v0.1.0
