# Run Evidence

## Week 4
- Python version: Python 3.14.16
- Git status/log evidence: 
```bash 
$ git status --short
 M .idea/dss150p-lab03-starter.iml
 M README.md
 M requirements.txt
 M templates/run_evidence_template.md
```
- Docker image/container evidence:
```bash
Task 7.4 Docker Compose Status.png
```
- External configuration evidence:
```bash
Task 7.3.png
```

## Week 5
- Raw row counts: Customers 3003, Products 601, Orders 50005
- Staging row counts: Customers 3000, Products 599, Orders 49998
- Curated row counts: 49897
- Quarantine row counts: 101
- First load affected rows: 2506
- Second rerun affected rows / evidence of idempotency: 2506

## Week 6
- Benchmark table attached: yes
- Partition selected: 2026-01
- Partition row count: 2506
- PostgreSQL verification query:

``
docker exec -it dss150p-postgres psql -U dss150p -d dss150p -c "SELECT * FROM audit.partition_loads;"
``

partition_key |         loaded_at_utc         | row_count | pipeline_run_id
--- | --- | --- | --- |
 2026-01       | 2026-09-23 03:57:59.290723+00 |      2506 | goal3_run1


## Week 7
- DAG ID: dss150p_sales_pipeline
- Schedule: 0 2 * * *
- Parameters used: Year "2026", Month "1", Run Mode "full" and "partition"
- Successful run ID: manual__2026-09-23T0714350000
- Deliberate failure run ID: manual__2026-09-23T0719390000
- Retry/failure-handling evidence: `extraction fail file not found.png`
- Final recovery run ID: manual__2026-09-23T0719390000 (4th attempt)
