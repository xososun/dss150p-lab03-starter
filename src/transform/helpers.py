"""Shared helpers for the transform stage: quarantine records and Parquet output."""
import json
from pathlib import Path

import numpy as np
import pandas as pd

# One quarantine schema for every stage, so validation and reporting can read them all.
QUARANTINE_COLUMNS = [
    'source', 'record_key', 'reason', 'raw_record', 'pipeline_run_id', 'quarantined_at_utc',
]


def empty_quarantine() -> pd.DataFrame:
    return pd.DataFrame(columns=QUARANTINE_COLUMNS)


def reasons_for(index, failures: dict) -> pd.Series:
    """Combine rule results into one reason string per row.

    failures maps a reason label to a boolean Series (True = the row breaks that rule).
    A row breaking several rules gets all labels joined by ';'. Passing rows get ''.
    """
    reasons = pd.Series('', index=index, dtype=object)
    for label, mask in failures.items():
        flags = mask.fillna(False).to_numpy(dtype=bool)
        reasons = reasons + np.where(flags, label + ';', '')
    return reasons.str.rstrip(';')


def to_quarantine(df, reasons, source, key_col, run_id, quarantined_at) -> pd.DataFrame:
    """Turn the rows that have a non-empty reason into quarantine records."""
    bad = reasons != ''
    rows = df[bad]
    if rows.empty:
        return empty_quarantine()
    payload = rows.astype(object).where(rows.notna(), None)
    raw = [json.dumps(rec, default=str, sort_keys=True) for rec in payload.to_dict('records')]
    return pd.DataFrame({
        'source': source,
        'record_key': rows[key_col].to_numpy(dtype=object),
        'reason': reasons[bad].to_numpy(),
        'raw_record': raw,
        'pipeline_run_id': run_id,
        'quarantined_at_utc': quarantined_at,
    })


def combine_quarantine(frames) -> pd.DataFrame:
    frames = [f for f in frames if not f.empty]
    return pd.concat(frames, ignore_index=True) if frames else empty_quarantine()


def write_parquet(df: pd.DataFrame, path: Path) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(path, index=False)
    return path
