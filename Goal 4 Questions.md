### Which steps in ETL and validate are safe to rerun and why?

Extract is safe to rerun as it re-copies from an untouched source and verifies via hash,
and reusing the same run_id just overwrites an identical snapshot.

Transform is safe. Staging and curated are pure functions of the raw snapshot and produce
deterministic output for the same run_id.

Load is safe by construction. The record_hash-gated WHERE clause in the UPSERT means
a retry that resubmits identical rows writes nothing.

Validate is safe as it is read-only.

So the whole DAG is idempotent per run_id, which is exactly why Airflow's Clear (rerun within the same run) works correctly here rather than needing manual cleanup.