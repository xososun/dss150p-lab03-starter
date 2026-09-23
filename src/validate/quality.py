"""Contract assertions over an already-built curated dataset.

This module only checks; it must not repair, drop, or otherwise mutate rows
(that belongs to src.transform). Every check appends a human-readable message,
so all problems are reported in one pass instead of stopping at the first one.
"""
from src.config import SETTINGS

REQUIRED_AUDIT_FIELDS = ['pipeline_run_id', 'processed_at_utc', 'record_hash']
AMOUNT_COLUMNS = ['gross_amount', 'discount_amount', 'net_amount']


def validate_curated(df) -> list[str]:
    """Return a list of human-readable validation errors.

    Minimum checks: order_id uniqueness/non-null, quantity range,
    nonnegative amounts, allowed statuses, required audit fields.
    An empty list means the curated dataset passed every check.
    """
    errors = []
    quality = SETTINGS['quality']

    if df.empty:
        errors.append('curated dataset is empty')
        return errors

    def add(mask, message):
        n = int(mask.sum())
        if n:
            examples = df.loc[mask, 'order_id'].head(5).tolist() if 'order_id' in df else []
            errors.append(f'{message}: {n} row(s), e.g. {examples}')

    if 'order_id' not in df.columns:
        errors.append("required column 'order_id' is missing")
    else:
        add(df['order_id'].isna(), 'order_id is null')
        dup = df['order_id'].duplicated(keep=False) & df['order_id'].notna()
        add(dup, 'order_id is duplicated')

    if 'quantity' in df.columns:
        bad_qty = (df['quantity'] < quality['min_quantity']) | (df['quantity'] > quality['max_quantity'])
        add(bad_qty.fillna(True), f"quantity outside [{quality['min_quantity']}, {quality['max_quantity']}]")
    else:
        errors.append("required column 'quantity' is missing")

    for col in AMOUNT_COLUMNS:
        if col in df.columns:
            add(df[col].isna() | (df[col] < 0), f'{col} is missing or negative')
        else:
            errors.append(f"required column '{col}' is missing")

    if 'status' in df.columns:
        add(~df['status'].isin(quality['allowed_order_statuses']), 'status not in allowed set')
    else:
        errors.append("required column 'status' is missing")

    for col in REQUIRED_AUDIT_FIELDS:
        if col in df.columns:
            add(df[col].isna() | (df[col] == ''), f'{col} is missing')
        else:
            errors.append(f"required audit column '{col}' is missing")

    return errors