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
## Deliverables

```bash
python --version
Python 3.14.6
```
#### Why should .venv not be committed to Git?
It contains compiled binaries, dynamic link libraries, and executable scripts specific to
individual operating system and CPU architecture. Thus, committing it breaks
functionality across different platforms. It could also bloat repo size and slow down
Git operations.

#### How configuration is separated from code
Settings.yml stores non-secret, environment-independent defaults, such as paths, quality rules, and benchmark settings.

.env manages environment-specific values and secrets, including database hosts and credentials. This file stays git-ignored and never enters source control.

src/config.py acts as the single source of truth for application settings. It reads settings.yml and .env, exposing values through objects like SETTINGS, DB, and path_for(). No other module reads os.environ or the YAML file directly.

Docker Compose overrides values dynamically at runtime (for instance, setting POSTGRES_HOST=postgres). Because load_dotenv() preserves existing environment variables, container orchestration requires zero code modifications.
