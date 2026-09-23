from pathlib import Path
import hashlib
import logging
import re
import shutil
from src.config import path_for

log = logging.getLogger(__name__)

# Source snapshot contents (fixed by the lab). Kept here because they describe
# what to acquire, not how the data is transformed.
SOURCE_FILES = ('customers.csv', 'products.json', 'orders.csv')

# run_id becomes a folder name (data/raw/run_id=<run_id>), so keep it filesystem-safe.
# This also catches raw Airflow run ids ("manual__2026-09-21T02:00:00+00:00").
_SAFE_RUN_ID = re.compile(r'[A-Za-z0-9_.\-]+')


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open('rb') as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b''):
            digest.update(chunk)
    return digest.hexdigest()


def extract_sources(run_id: str) -> Path:
    """Copy immutable source snapshots into a run-specific raw directory.

    TODO:
    1. Create data/raw/run_id=<run_id>/.
    2. Copy customers.csv, products.json, and orders.csv from data/source/.
    3. Return the run-specific raw path.
    4. Do not modify source files in place.
    """
    if not _SAFE_RUN_ID.fullmatch(run_id or ''):
        raise ValueError(
            f'run_id {run_id!r} is not filesystem-safe; use letters, digits, '
            "'_', '-' and '.' only."
        )

    source_dir = path_for('source_dir')

    # Check everything first so a missing file never leaves a half-built snapshot.
    missing = [name for name in SOURCE_FILES if not (source_dir / name).is_file()]
    if missing:
        raise FileNotFoundError(
            f'Missing source file(s) in {source_dir}: {", ".join(missing)}'
        )

    raw_dir = path_for('raw_dir') / f'run_id={run_id}'
    # exist_ok: a retried task (same run_id) simply rewrites the identical snapshot.
    raw_dir.mkdir(parents=True, exist_ok=True)

    for name in SOURCE_FILES:
        src, dst = source_dir / name, raw_dir / name
        shutil.copy2(src, dst)  # byte-for-byte copy; the source is only read
        if _sha256(src) != _sha256(dst):
            raise RuntimeError(f'Snapshot copy of {name} differs from source')
        log.info('raw snapshot: %s -> %s (%d bytes)', src, dst, dst.stat().st_size)

    return raw_dir