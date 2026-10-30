# Architecture

Each lab is a small, pure module in `labs/` that takes DataFrames or paths in and
writes Delta tables out. Nothing depends on a cluster, a metastore or cloud storage,
so the same code runs in tests, in the demo and inside a real job.

```mermaid
flowchart LR
    subgraph labs
        S[spark.py<br/>local session + Delta]
        D[dedup.py]
        C[scd2.py]
        K[salting.py]
        B[backfill.py]
    end
    T[tests/] --> labs
    M[demo.py<br/>spark-labs CLI] --> labs
    CI[GitHub Actions<br/>lint · types · 3 Pythons · Docker] --> T
```

## Design rules

1. **Every lab proves one property, and a test asserts it.** "No duplicates after a
   replay", "rerun is a no-op", "same rows as the plain join", "identical after a full
   rerun". If the property breaks, CI goes red.
2. **Idempotent by construction.** Re-running any lab with the same input leaves the
   output unchanged. That is the property production pipelines lose first.
3. **Paths, not catalogs.** Delta tables are addressed by path so the labs run on a
   laptop. Swapping to `saveAsTable` / Unity Catalog is a one-line change.
4. **Small, typed, linted.** `ruff` and `mypy` run before tests in CI.

## Lab 01 · Streaming dedup

```mermaid
flowchart LR
    A[JSON event files<br/>at-least-once] --> W[withWatermark<br/>event_time, 2h]
    W --> X[dropDuplicatesWithinWatermark<br/>event_id]
    X --> O[(Delta: clean events)]
    X -. state .-> CK[(checkpoint)]
```

State holds only ids inside the watermark. The checkpoint records processed files and
dedup state, so replaying an input file is safe.

## Lab 02 · SCD Type 2

```mermaid
flowchart LR
    CDC[CDC batch] --> L[latest change per key]
    L --> H[hash tracked columns]
    H --> S{changed?}
    S -- yes --> E[stage twice:<br/>key → expire<br/>NULL key → insert]
    S -- no --> N[stage once → no-op]
    E --> M[[single MERGE]]
    N --> M
    M --> DIM[(dim_customer<br/>valid_from · valid_to · is_current)]
```

## Lab 03 · Skew salting

```mermaid
flowchart LR
    F[(facts: 90% one key)] --> HK[find_hot_keys]
    HK --> SF[hot rows: salt 0..N-1<br/>others: salt 0]
    DM[(dims)] --> SD[hot rows × N<br/>others: salt 0]
    SF --> J[[join on key + salt]]
    SD --> J
    J --> R[same rows as plain join,<br/>hot key over N tasks]
```

## Lab 04 · Idempotent backfill

```mermaid
flowchart LR
    P[date range] --> G{in ledger?}
    G -- yes --> SKIP[skip]
    G -- no --> X[extract day]
    X --> W[overwrite with<br/>replaceWhere day]
    W --> A{row-count audit}
    A -- ok --> LG[(ledger: day done)]
    A -- mismatch --> FAIL[raise, day not recorded]
```
