# Technical Reflection

## Modularity

Splitting the pipeline into `extract`, `transform` (`staging` and `curated`), `load`, `validate`,
and `benchmark` meant every stage could be reasoned about, tested, and fixed on its own. This paid
off directly during development: three real bugs surfaced during testing (a cursor closed too
early in `upsert_curated`, a stale partition tree in `cmd_load_partition`, and a partition
directory that accumulated files instead of being replaced), and in each case the fix was
contained to a single function because the responsibilities were already separated. None of the
fixes touched `extract`, `staging`, or `curated`, since those modules had no reason to be involved.

The `src.cli` layer stayed thin by design: every command handler is a few lines that call into a
module and pass through a `run_id`, with no business logic of its own. That is also what let the
exact same tested code run three different ways in this lab: directly on the command line, inside
a Docker container, and as a task inside an Airflow DAG, without any of the underlying logic being
duplicated or rewritten for each context.

## Idempotency

Idempotency was the single most load bearing property in this pipeline. `extract_sources` hash
verifies its copy so re-extracting the same run id is always safe. `build_staging` and
`build_curated` are pure functions of the raw snapshot and run id, so re-running `transform`
produces the same output. `upsert_curated` uses `record_hash`, deliberately excluding volatile
columns like `pipeline_run_id` and `processed_at_utc`, so a row is only rewritten when its actual
business content changed.

This was proven directly, not just claimed. Loading the same 49897 curated rows a second time
reported zero rows written, and `COUNT(*)` still equaled `COUNT(DISTINCT order_id)` afterward. The
same property carried into Airflow: clearing and re-running a failed task after restoring the
missing source file completed cleanly, with the same row count and no duplicates, because the
whole DAG shares one `pipeline_run_id` and every stage underneath it is safe to repeat.

## Storage trade offs

The four format benchmark made the trade offs concrete rather than theoretical. Parquet was
roughly a third the size of CSV and about twenty five times faster on both full and filtered
reads, because it is columnar and compresses repeated values well. CSV and JSON Lines were larger
and slower on every read, but they remain the easiest formats for a human or another system to
read line by line without special tooling. PostgreSQL was not the fastest on a raw full table
read, but it is the only one of the four that is a live, concurrent, queryable system rather than
a static file, and its filtered query used its own query engine instead of a full scan followed by
an in memory filter.

The conclusion drawn from this was not that one format is universally best, but that the choice
depends on the workload: Parquet for analytical batch reads, PostgreSQL for live queries and
concurrent access, and CSV or JSON Lines mainly as interchange formats for systems that expect
plain text.

## Orchestration versus business logic

Keeping Airflow strictly as a scheduler, and never as a place where business rules live, was
enforced everywhere in the DAG. Every task is a `BashOperator` that calls
`python -m src.cli <command>`, so the DAG file contains no joins, no validation rules, and no
calculations. The one piece of real logic inside the DAG is choosing which CLI command to run
based on the `run_mode` parameter, which is a routing decision, not a business rule.

This separation is what made the Section 11 integrated test and the Goal 4 evidence consistent
with each other: the same `run-all`, `load`, `validate`, and `load-partition` behavior that was
verified manually on the command line is exactly what Airflow triggers, so there was never a
second, DAG specific implementation of the pipeline that could drift out of sync with the one
tested locally.
