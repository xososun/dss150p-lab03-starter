import json
import logging
from pathlib import Path

import pandas as pd

from src.config import SETTINGS, path_for
from src.transform.helpers import (
    combine_quarantine, reasons_for, to_quarantine, write_parquet,
)

log = logging.getLogger(__name__)


def _to_utc(series: pd.Series) -> pd.Series:
    """Parse ISO-8601 timestamps as UTC; anything unparseable becomes NaT."""
    return pd.to_datetime(series, utc=True, errors='coerce', format='ISO8601')


def _clean_text(df: pd.DataFrame) -> pd.DataFrame:
    """Trim every text column; empty strings become missing values."""
    for col in df.columns:
        if df[col].dtype == object or pd.api.types.is_string_dtype(df[col]):
            trimmed = df[col].str.strip()
            df[col] = trimmed.mask(trimmed == '')
    return df


def _resolve(df, source, key, run_id, staged_at, dataset_rules):
    """Shared staging flow for one dataset. Returns (clean_df, quarantine_df, stats).

    1. Quarantine rows that cannot take part in deduplication (no key / bad updated_at).
    2. Keep the latest version per business key (greatest updated_at; on a tie the row
       that appears later in the source file wins, so the result is deterministic).
    3. Apply the dataset's validity rules to the surviving latest versions and
       quarantine violators. A newer invalid version is NOT replaced by an older valid one.
    """
    df = df.copy()
    df['_src_row'] = range(len(df))
    n_raw = len(df)

    pre = reasons_for(df.index, {
        f'missing_{key}': df[key].isna(),
        'invalid_updated_at': df['updated_at'].isna(),
    })
    q_pre = to_quarantine(df, pre, source, key, run_id, staged_at)
    usable = df[pre == '']

    ordered = usable.sort_values([key, 'updated_at', '_src_row'])
    latest = ordered.drop_duplicates(key, keep='last')
    superseded = len(usable) - len(latest)

    post = reasons_for(latest.index, dataset_rules(latest))
    q_post = to_quarantine(latest, post, source, key, run_id, staged_at)
    good = latest[post == ''].drop(columns='_src_row').sort_values(key).reset_index(drop=True)

    stats = {'raw': n_raw, 'superseded_versions': superseded,
             'quarantined': len(q_pre) + len(q_post), 'staged': len(good)}
    log.info('staging %-9s %s', source, stats)
    return good, combine_quarantine([q_pre, q_post]), stats


def _stamp(df, run_id, staged_at):
    df['pipeline_run_id'] = run_id
    df['staged_at_utc'] = staged_at
    return df


def _stage_customers(raw_dir, run_id, staged_at):
    df = pd.read_csv(raw_dir / 'customers.csv', dtype=str, keep_default_na=False, na_values=[''])
    df = _clean_text(df)
    df['created_at'] = _to_utc(df['created_at'])
    df['updated_at'] = _to_utc(df['updated_at'])

    def rules(d):
        return {'invalid_created_at': d['created_at'].isna()}

    good, quarantine, stats = _resolve(df, 'customers', 'customer_id', run_id, staged_at, rules)
    good['email'] = good['email'].str.lower()
    good['email_missing'] = good['email'].isna()  # missing email stays a visible quality condition
    good['city'] = good['city'].str.replace(r'\s+', ' ', regex=True).str.title()
    return _stamp(good, run_id, staged_at), quarantine, stats


def _stage_products(raw_dir, run_id, staged_at):
    with (raw_dir / 'products.json').open(encoding='utf-8') as f:
        df = pd.json_normalize(json.load(f))
    df = df.rename(columns={'category.name': 'category_name',
                            'category.department': 'category_department'})
    for col in ('category_name', 'category_department'):
        if col not in df.columns:
            df[col] = pd.NA
    df = _clean_text(df)
    df['unit_price'] = pd.to_numeric(df['unit_price'], errors='coerce')
    df['updated_at'] = _to_utc(df['updated_at'])

    def rules(d):
        return {'invalid_unit_price': d['unit_price'].isna(),
                'negative_unit_price': d['unit_price'] < 0}

    good, quarantine, stats = _resolve(df, 'products', 'product_id', run_id, staged_at, rules)
    return _stamp(good, run_id, staged_at), quarantine, stats


def _stage_orders(raw_dir, run_id, staged_at):
    df = pd.read_csv(raw_dir / 'orders.csv', dtype=str, keep_default_na=False, na_values=[''])
    df = _clean_text(df)
    df['status'] = df['status'].str.upper()
    df['order_timestamp'] = _to_utc(df['order_timestamp'])
    df['updated_at'] = _to_utc(df['updated_at'])
    for col in ('quantity', 'unit_price', 'discount_pct'):
        df[col] = pd.to_numeric(df[col], errors='coerce')

    quality = SETTINGS['quality']

    def rules(d):
        qty = d['quantity']
        return {
            'quantity_not_integer': qty.isna() | (qty % 1 != 0),
            'quantity_out_of_range': (qty < quality['min_quantity']) | (qty > quality['max_quantity']),
            'invalid_status': ~d['status'].isin(quality['allowed_order_statuses']),
            'invalid_order_timestamp': d['order_timestamp'].isna(),
            'invalid_unit_price': d['unit_price'].isna() | (d['unit_price'] < 0),
            'invalid_discount_pct': d['discount_pct'].isna() | (d['discount_pct'] < 0) | (d['discount_pct'] > 1),
        }

    good, quarantine, stats = _resolve(df, 'orders', 'order_id', run_id, staged_at, rules)
    good['quantity'] = good['quantity'].astype('int64')  # validated as whole numbers above
    return _stamp(good, run_id, staged_at), quarantine, stats


def build_staging(raw_dir, run_id: str):
    """Create cleaned, typed staging datasets.

    Required rules:
    - Deduplicate by business key, keeping greatest updated_at.
    - Parse timestamps as UTC.
    - Normalize emails/cities and flatten product.category.
    - Validate order quantity/status and product price.
    - Add pipeline_run_id and staged_at_utc audit columns.
    - Write invalid records to data/quarantine/ with a reason.

    Return a dict of staging DataFrames and a quarantine DataFrame.
    """
    raw_dir = Path(raw_dir)
    if not raw_dir.is_dir():
        raise FileNotFoundError(
            f'Raw snapshot not found: {raw_dir}. Run extract with the same run id '
            f'first (or use run-all).'
        )

    staged_at = pd.Timestamp.now(tz='UTC')
    staging, quarantines = {}, []
    for name, stage_fn in (('customers', _stage_customers),
                           ('products', _stage_products),
                           ('orders', _stage_orders)):
        staging[name], quarantine, _ = stage_fn(raw_dir, run_id, staged_at)
        quarantines.append(quarantine)

    quarantine = combine_quarantine(quarantines)

    staging_dir = path_for('staging_dir') / f'run_id={run_id}'
    for name, df in staging.items():
        write_parquet(df, staging_dir / f'{name}.parquet')
    write_parquet(quarantine, path_for('quarantine_dir') / f'run_id={run_id}' / 'staging_quarantine.parquet')
    log.info('staging written to %s; %d record(s) quarantined', staging_dir, len(quarantine))

    return staging, quarantine