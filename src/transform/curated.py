import logging

import pandas as pd

from src.common.audit import record_hash
from src.config import path_for
from src.transform.helpers import reasons_for, to_quarantine, write_parquet

log = logging.getLogger(__name__)

CUSTOMER_COLUMNS = ['customer_id', 'first_name', 'last_name', 'email', 'email_missing',
                    'city', 'customer_tier']
PRODUCT_COLUMNS = {'product_id': 'product_id', 'name': 'product_name', 'brand': 'brand',
                   'category_name': 'category_name',
                   'category_department': 'category_department',
                   'unit_price': 'product_list_price', 'active': 'product_active'}

BUSINESS_COLUMNS = [
    'order_id', 'customer_id', 'product_id', 'order_timestamp', 'quantity', 'unit_price',
    'discount_pct', 'status',
    'first_name', 'last_name', 'email', 'email_missing', 'city', 'customer_tier',
    'product_name', 'brand', 'category_name', 'category_department',
    'product_list_price', 'product_active',
    'gross_amount', 'discount_amount', 'net_amount', 'source_updated_at',
]
AUDIT_COLUMNS = ['pipeline_run_id', 'processed_at_utc', 'record_hash']
CURATED_COLUMNS = BUSINESS_COLUMNS + AUDIT_COLUMNS

# The hash covers business content only. pipeline_run_id and processed_at_utc change on
# every run, so including them would make every rerun look like a change.
HASH_COLUMNS = BUSINESS_COLUMNS


def _row_hashes(curated: pd.DataFrame) -> list:
    """Deterministic hash per row: same business content -> same hash, on any run."""
    frame = curated[HASH_COLUMNS].copy()
    for col in ('order_timestamp', 'source_updated_at'):
        frame[col] = frame[col].dt.strftime('%Y-%m-%dT%H:%M:%S%z')  # stable text form
    frame = frame.astype(object).where(frame.notna(), None)
    return [record_hash(rec, HASH_COLUMNS) for rec in frame.to_dict('records')]


def build_curated(staging: dict, run_id: str):
    """Join staging orders/customers/products and create analysis-ready sales rows.

    Required columns include gross_amount, discount_amount, net_amount,
    processed_at_utc, pipeline_run_id, and record_hash.

    Orphan customer/product references must be quarantined, not silently dropped.

    Returns (curated_df, quarantine_df) and writes both under data/curated/ and
    data/quarantine/ for this run.
    """
    orders, customers, products = staging['orders'], staging['customers'], staging['products']
    processed_at = pd.Timestamp.now(tz='UTC')

    # Orphans: the order references a customer/product that is not among the VALID staged
    # records (missing from the source, or itself quarantined in staging).
    reasons = reasons_for(orders.index, {
        'orphan_customer': ~orders['customer_id'].isin(customers['customer_id']),
        'orphan_product': ~orders['product_id'].isin(products['product_id']),
    })
    quarantine = to_quarantine(orders, reasons, 'orders', 'order_id', run_id, processed_at)

    product_dim = products[list(PRODUCT_COLUMNS)].rename(columns=PRODUCT_COLUMNS)
    lines = (
        orders[reasons == '']
        .merge(customers[CUSTOMER_COLUMNS], on='customer_id', how='left', validate='many_to_one')
        .merge(product_dim, on='product_id', how='left', validate='many_to_one')
    )

    # Amounts use the price actually charged on the order (unit_price on the order line),
    # not the product's current list price (kept as product_list_price).
    lines['gross_amount'] = (lines['quantity'] * lines['unit_price']).round(2)
    lines['discount_amount'] = (lines['gross_amount'] * lines['discount_pct']).round(2)
    lines['net_amount'] = (lines['gross_amount'] - lines['discount_amount']).round(2)
    lines['source_updated_at'] = lines['updated_at']

    lines = lines.sort_values('order_id').reset_index(drop=True)
    lines['record_hash'] = _row_hashes(lines)
    lines['pipeline_run_id'] = run_id
    lines['processed_at_utc'] = processed_at
    curated = lines[CURATED_COLUMNS]

    write_parquet(curated, path_for('curated_dir') / f'run_id={run_id}' / 'sales_order_lines.parquet')
    write_parquet(quarantine, path_for('quarantine_dir') / f'run_id={run_id}' / 'curated_quarantine.parquet')
    log.info('curated: %d line(s) built, %d order(s) quarantined as orphans', len(curated), len(quarantine))
    return curated, quarantine