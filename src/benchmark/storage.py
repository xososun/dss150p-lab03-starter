"""Format materialization and timing. No production business logic here:
the curated dataset is taken as given and only re-shaped/measured/timed."""
import json
import logging
import os
import platform
import shutil
import statistics
import time
from pathlib import Path

import pandas as pd
import psycopg2

from src.config import SETTINGS, db_connection_params

log = logging.getLogger(__name__)


def _timed(fn, repeats: int):
    """Run fn() `repeats` times, discarding nothing, and return (median_seconds, last_result)."""
    times, result = [], None
    for _ in range(repeats):
        start = time.perf_counter()
        result = fn()
        times.append(time.perf_counter() - start)
    return statistics.median(times), result


def _hardware_context() -> dict:
    return {
        'python_version': platform.python_version(),
        'platform': platform.platform(),
        'processor': platform.processor() or 'unknown',
        'cpu_count': os.cpu_count(),
    }


# --------------------------------------------------------------------------- #
# CSV / JSON Lines / Parquet
# --------------------------------------------------------------------------- #
def _benchmark_file_format(df, fmt: str, path: Path, repeats: int, status_filter: str) -> dict:
    writers = {
        'csv': lambda: df.to_csv(path, index=False),
        'jsonl': lambda: df.to_json(path, orient='records', lines=True, date_format='iso'),
        'parquet': lambda: df.to_parquet(path, index=False, compression='snappy'),
    }
    readers = {
        'csv': lambda: pd.read_csv(path),
        'jsonl': lambda: pd.read_json(path, orient='records', lines=True),
        'parquet': lambda: pd.read_parquet(path),
    }

    write_start = time.perf_counter()
    writers[fmt]()
    write_time = time.perf_counter() - write_start
    size_bytes = path.stat().st_size

    full_median, full_result = _timed(readers[fmt], repeats)

    def _filtered():
        data = readers[fmt]()
        return data[data['status'] == status_filter]

    filtered_median, filtered_result = _timed(_filtered, repeats)

    return {
        'format': fmt,
        'size_bytes': size_bytes,
        'write_time_s': round(write_time, 4),
        'full_read_median_s': round(full_median, 4),
        'filtered_read_median_s': round(filtered_median, 4),
        'full_row_count': len(full_result),
        'filtered_row_count': len(filtered_result),
    }


# --------------------------------------------------------------------------- #
# PostgreSQL
# --------------------------------------------------------------------------- #
def _benchmark_postgres(repeats: int, status_filter: str) -> dict:
    conn = psycopg2.connect(**db_connection_params())
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT pg_total_relation_size('curated.sales_order_lines')")
            size_bytes = cur.fetchone()[0]  # table + indexes; a single file-size number doesn't apply to a server

        def _full():
            with conn.cursor() as cur:
                cur.execute('SELECT * FROM curated.sales_order_lines')
                return cur.fetchall()

        def _filtered():
            with conn.cursor() as cur:
                cur.execute(
                    'SELECT * FROM curated.sales_order_lines WHERE status = %s', (status_filter,)
                )
                return cur.fetchall()

        full_median, full_result = _timed(_full, repeats)
        filtered_median, filtered_result = _timed(_filtered, repeats)
    finally:
        conn.close()

    return {
        'format': 'postgresql',
        'size_bytes': size_bytes,
        'write_time_s': None,  # already loaded by `load`; not re-measured here
        'full_read_median_s': round(full_median, 4),
        'filtered_read_median_s': round(filtered_median, 4),
        'full_row_count': len(full_result),
        'filtered_row_count': len(filtered_result),
    }


def run_benchmark(curated_path, output_dir, repeats: int = 5):
    """Compare the same logical dataset in CSV, JSON Lines, Parquet, and PostgreSQL.

    Capture:
    - storage/file size where applicable
    - write time
    - full-read time
    - filtered-read/query time
    - row count

    Use multiple repetitions and report a median for read/query timing.

    Requires `load` to have already loaded the same curated data into PostgreSQL
    (the DB is measured/queried as-is, not re-written here). Writes
    benchmark_results.csv and benchmark_context.json under output_dir.
    """
    curated_path = Path(curated_path)
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    df = pd.read_parquet(curated_path)
    status_filter = SETTINGS['storage_benchmark']['filter_status']

    rows = [
        _benchmark_file_format(df, 'csv', output_dir / 'sales_order_lines.csv', repeats, status_filter),
        _benchmark_file_format(df, 'jsonl', output_dir / 'sales_order_lines.jsonl', repeats, status_filter),
        _benchmark_file_format(df, 'parquet', output_dir / 'sales_order_lines.parquet', repeats, status_filter),
        _benchmark_postgres(repeats, status_filter),
    ]
    results = pd.DataFrame(rows)

    results_path = output_dir / 'benchmark_results.csv'
    results.to_csv(results_path, index=False)
    with (output_dir / 'benchmark_context.json').open('w', encoding='utf-8') as f:
        json.dump({'repeats': repeats, 'status_filter': status_filter, **_hardware_context()}, f, indent=2)

    log.info('benchmark: results written to %s', results_path)
    return results


# --------------------------------------------------------------------------- #
# Partitioning
# --------------------------------------------------------------------------- #
def write_partitioned_parquet(df, output_dir):
    """Write Parquet partitioned by order_year/order_month.

    output_dir becomes the partition root (e.g. data/partitioned/), containing
    order_year=YYYY/order_month=M/ subfolders, per settings.storage_benchmark
    .partition_columns. Not run-scoped: this represents the current full curated
    dataset, and re-running it overwrites the existing partitioned tree.
    """
    output_dir = Path(output_dir)
    partition_cols = SETTINGS['storage_benchmark']['partition_columns']

    # to_parquet(..., partition_cols=...) ADDS a new file into each partition folder
    # on every call rather than replacing it. Since this dataset is rebuilt on every
    # run (see cli.cmd_load_partition), the old tree must be cleared first or repeated
    # runs silently double-count rows the next time a partition is read back.
    if output_dir.exists():
        shutil.rmtree(output_dir)

    df = df.copy()
    df['order_year'] = df['order_timestamp'].dt.year
    df['order_month'] = df['order_timestamp'].dt.month

    df.to_parquet(output_dir, index=False, partition_cols=partition_cols, compression='snappy')
    log.info('partitioned parquet written to %s (%d row(s), by %s)',
              output_dir, len(df), partition_cols)
    return output_dir


def read_partition(partition_dir, year: int, month: int) -> pd.DataFrame:
    """Read only the selected year/month partition, verifying every row matches.

    Added beyond the original stub: src.cli needs a read side to hand rows to
    src.load.postgres.load_partition without re-deriving order_year/order_month
    from the full curated set on every partition load.
    """
    partition_dir = Path(partition_dir)
    df = pd.read_parquet(
        partition_dir,
        filters=[('order_year', '=', year), ('order_month', '=', month)],
    )
    if len(df) and not ((df['order_year'] == year) & (df['order_month'] == month)).all():
        raise AssertionError('partition read returned rows outside the requested year/month')
    return df.drop(columns=['order_year', 'order_month'])