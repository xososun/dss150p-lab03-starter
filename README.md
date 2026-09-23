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
### 7.1 Task A
```bash
python --version
Python 3.14.6

pip list
Package         Version
--------------- -----------
numpy           2.5.3
packaging       26.3
pandas          3.0.6
pip             26.2.1
psycopg         3.3.6
psycopg-binary  3.3.6
pyarrow         25.0.1
python-dateutil 2.9.0.post0
python-dotenv   1.2.3
PyYAML          6.0.3
setuptools      84.0.0
six             1.17.0
tzdata          2026.4
wheel           0.48.0
```
#### Why should .venv not be committed to Git?
It contains compiled binaries, dynamic link libraries, and executable scripts specific to
individual operating system and CPU architecture. Thus, committing it breaks
functionality across different platforms. It could also bloat repo size and slow down
Git operations.

