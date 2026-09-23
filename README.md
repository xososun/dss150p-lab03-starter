# DSS150P Laboratory 3 Starter Repository

This repository supports Module 2: Pipeline Construction, Storage, and Orchestration.
It is intentionally incomplete. Students must implement the marked TODOs and document their decisions.

## Main progression
- Goal 1: reproducible environment, modularization, Git, Docker, configuration
- Goal 2: raw -> staging -> curated transformations; audit/error handling; rerun-safe loading
- Goal 3: CSV/JSON/Parquet/PostgreSQL comparison; partitioning; selected-partition load
- Goal 4: Apache Airflow DAG for extract -> transform -> load -> validate

Start with `DSS150P_Laboratory_Activity_3.pdf`.

## Recommended commands
```bash
cp .env.example .env
python -m venv .venv
# activate .venv then:
pip install -r requirements.txt
python -m src.cli validate-env
```
The provided `.env.example` uses `POSTGRES_HOST=localhost` for host-side commands. Docker Compose overrides the application containers to use the service hostname `postgres`.

Docker/PostgreSQL:
```bash
docker compose up -d postgres
docker compose run --rm pipeline python -m src.cli validate-env
```

Airflow in Goal 4:
```bash
docker compose -f docker-compose.yml -f docker-compose.airflow.yml up airflow-init
docker compose -f docker-compose.yml -f docker-compose.airflow.yml up -d airflow-webserver airflow-scheduler
```
Airflow UI: http://localhost:8080 (training credentials: admin/admin; change if reused outside the lab).

---
## FAQ
### Python Version for this Project
```bash
python --version
Python 3.14.6
```
The initial requirements.txt has been modified to match with my machine. Libraries versions might differ.

### Why should .venv not be committed to Git?
It contains compiled binaries, dynamic link libraries, and executable scripts specific to
individual operating system and CPU architecture. Thus, committing it breaks
functionality across different platforms. It could also bloat repo size and slow down
Git operations.

### How configuration is separated from code
Settings.yml stores non-secret, environment-independent defaults, such as paths, quality rules, and benchmark settings.

.env manages environment-specific values and secrets, including database hosts and credentials. This file stays git-ignored and never enters source control.

src/config.py acts as the single source of truth for application settings. It reads settings.yml and .env, exposing values through objects like SETTINGS, DB, and path_for(). No other module reads os.environ or the YAML file directly.

Docker Compose overrides values dynamically at runtime (for instance, setting POSTGRES_HOST=postgres). Because load_dotenv() preserves existing environment variables, container orchestration requires zero code modifications.

### Explain how partitioning can reduce unnecessary I/O when queries filter on partition keys.
Partitioning splits a large database table into smaller, independent physical units based on a specific column, called the partition key.

When you run a query filtering on that key, the query optimizer performs partition pruning (or partition elimination). The engine reads the filter condition in your WHERE clause, calculates which specific partitions contain matching rows, and skips reading all other partitions entirely.

### Why do I need to set PIPELINE_RUN_ID?

Each CLI command generates a new run_id unless one is provided, since separate invocations are separate processes (this mirrors how Airflow tasks run as separate processes too). To chain commands against the same data, set PIPELINE_RUN_ID once before running them, or pass --run-id explicitly to each command.

---
## AI Tool Usage Disclosure
Generative AI (Claude) was used during this lab as a debugging, design-review, and writing-refinement aid, consistent with the course's AI use policy.

How it was used:

* Reviewing my own module stubs and explaining what each TODO required before I implemented it.
* Drafting implementations for src/extract/files.py, src/transform/staging.py, src/transform/curated.py, src/load/postgres.py, src/validate/quality.py, src/benchmark/storage.py, src/cli.py wiring, and dags/dss150p_pipeline.py, which I then ran against the real project data, reviewed, and am able to explain and defend.
* Diagnosing real runtime errors I encountered (e.g. a cursor-closed bug in the PostgreSQL loader, a stale-partition bug in the partition loader, a partition-file-duplication bug, and an Airflow run_id sanitization issue) by reading my actual tracebacks and command output, not by guessing.
* Explaining configuration/security tradeoffs (e.g. removing a hard-coded database password default) and Airflow/DAG design choices (retries, timeouts, catchup, run_id propagation).
* Helping draft the Goal 3 storage-format analysis and the Goal 4 backfill explanation, which I reviewed and wrote in my own words based on my own benchmark numbers.

What I did myself:

* Ran every command against my own environment and database; all counts, timings, screenshots, and logs in this submission are from my own machine, not fabricated or copied from the AI.
* Reviewed, tested, and can explain every line of AI-assisted code, including the bugs found and why the fixes work.
* Made the design decisions the lab required judgment on (e.g. how run_id is shared across CLI invocations vs. Airflow tasks; how curated columns map to the PostgreSQL schema; dedupe-before-validate ordering in staging).

I understand I may be asked to explain, modify, or reproduce any part of this pipeline's behavior during validation.