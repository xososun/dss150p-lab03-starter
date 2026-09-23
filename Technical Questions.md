# DSS150P Laboratory Activity 3

### 1. Why is record_hash useful for rerun safe loading, and which columns should not be included in it?

record_hash gives every curated row a single deterministic fingerprint of its business content. The PostgreSQL upsert uses this fingerprint in the ON CONFLICT clause: an incoming row only overwrites the existing row when the hash actually changed. When the same run is executed again with unchanged source data, every hash matches what is already stored, so the update is skipped entirely. This is what let the pipeline load 49897 rows once and then report zero rows written on a second, identical run, with no duplicate order_id values ever created.

Columns that change purely because the pipeline ran again, rather than because the underlying business data changed, must be excluded from the hash. In this pipeline that means pipeline_run_id and processed_at_utc, and record_hash itself. If either of those two were included, every single run would produce a brand new hash for every row even when nothing about the order actually changed, which would defeat the entire purpose of rerun safety and force PostgreSQL to rewrite the full table on every load.

### 2. Why should raw data usually be preserved even when staging/curated outputs are sufficient for analytics?

Staging and curated data are the result of a chain of decisions: which duplicate business key wins, which records get quarantined, how prices and discounts get combined into monetary measures. If a rule in that chain turns out to be wrong, or a new business requirement appears later, the only way to correct the curated output is to reprocess from an unmodified source. Without a preserved raw snapshot, that source is gone, and any fix has to be reconstructed from whatever downstream data happens to still exist, which is often incomplete or already distorted.

In this pipeline the raw layer also gives every run a fixed, reproducible starting point. extract_sources copies the three source files into a run specific folder and verifies the copy with a hash check, so staging and curated can always be rebuilt from that exact snapshot later, independent of whatever the live source files look like by then.

### 3. What is the difference between a data quality rejection and a system exception?

A data quality rejection means the pipeline is working correctly and has found a record that fails a business rule, such as an order with a negative price or a status outside the allowed set. That record is not an error in the code. It is routed to quarantine along with a specific reason, and the rest of the run continues normally.

A system exception means the pipeline itself cannot proceed: a required source file is missing, a database connection fails, or a bug produces an error the code did not anticipate. In this project those are raised as a PipelineStageError that names the stage and run id involved, and the whole stage stops rather than silently continuing. The distinction matters because quarantine is a normal, expected outcome that should never crash the run, while a system exception must always be visible and must always stop the run, since continuing past it could silently produce wrong or incomplete results.

### 4. Why might Parquet outperform CSV for selected analytical workloads even if both contain the same rows?

Parquet stores data by column rather than by row, and it compresses each column on its own. Columns with a small number of repeated values, such as status or category name, compress extremely well this way, and an analytical query that only needs a few columns can read just those columns instead of scanning every field in every row. CSV has no such structure. It is plain text, uninterpreted, so every read has to scan the entire file and parse every column for every row even when most of the columns are not needed.

The benchmark in this lab measured this directly. Parquet was roughly five megabytes against about eighteen megabytes for CSV, and both the full read and the filtered read were on the order of twenty times faster for Parquet. Both formats held the exact same forty nine thousand eight hundred ninety seven rows, so the difference comes entirely from the storage layout, not from the data itself.

### 5. Why is a DAG that contains all transformation logic directly considered harder to maintain?

If business rules live inside the DAG file itself, testing them requires running Airflow, which is slow and awkward compared to testing a plain Python function directly. The logic also becomes tied to one orchestration tool, so switching schedulers later would mean rewriting the actual business rules rather than just changing how they are triggered. It also becomes very easy for the same logic to be duplicated between a manual command line run and a scheduled DAG run, and the two copies can quietly drift apart over time.

In this project the DAG only calls python dash m src dot cli followed by extract, transform, load, or validate. All of the actual business logic lives in the src package, so the exact same tested code runs whether it is triggered manually from a terminal or automatically by Airflow, and there is only ever one place to fix a rule.

### 6. How do retries interact with idempotency? Give an example where retries without idempotency cause damage.

A retry means the same task is executed again after a failure, usually because a transient problem interrupted it partway through. If that task is idempotent, meaning running it twice with the same input produces the same end state as running it once, then retries are safe and simply repeat work that either did nothing new or produced the same correct result. If the task is not idempotent, a retry can repeat an effect that should only have happened once.

The clearest example is the load stage in this pipeline. Because it uses an upsert with a conflict target and skips rows whose record_hash has not changed, retrying it after a network interruption simply resubmits the same rows and changes nothing further. Now imagine the same stage had instead used a plain insert of every curated row on every run, with no conflict handling at all. A retry after a partial failure would insert the already loaded rows a second time, creating duplicate order_id values and silently doubling reported sales figures for every analyst querying that table.

### 7. What trade off is introduced by partitioning too aggressively?

Partitioning is meant to let a query skip reading data it does not need, but each partition carries its own fixed overhead: a directory, at least one file, and an entry in whatever system is tracking partitions. If the partition key has very high cardinality, the dataset ends up split into an enormous number of very small partitions, and the overhead of opening and tracking all of them can outweigh whatever I O was saved by skipping unrelated ones. A query that touches many partitions at once can end up slower than an equivalent query against a single unpartitioned file.

This pipeline partitions by order_year and order_month, which produced twenty one partitions from about fifty thousand orders, each holding a reasonably sized chunk of data. If it had partitioned by order_id instead, the result would have been close to fifty thousand partitions with only one or two rows each, which would be a clear example of over partitioning with no real benefit.

### 8. How would you adapt the pipeline if the source became an API or database instead of local files?

The change would be isolated almost entirely to the extract module. Instead of copying three local files into a run specific raw folder, extract_sources would call the API or query the source database and write the results into that same raw folder in a stable format such as Parquet or JSON Lines, preserving the same contract of one immutable snapshot per run id. Everything downstream, staging, curated, load, and validate, reads from that raw snapshot and would not need to change at all, since none of those modules know or care where the raw files originally came from.

A few new concerns would appear inside extract itself: authentication and credentials handled through config and dot env rather than hard coded, network failures and rate limits needing retry handling, and a decision about incremental extraction, such as only pulling records updated since the last successful run, versus pulling a full snapshot every time. The quarantine and record_hash design would not need to change, since both already operate on the content of the data rather than on how it arrived.
