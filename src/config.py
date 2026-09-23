from pathlib import Path
import os
import yaml
from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parents[1]
load_dotenv(PROJECT_ROOT / '.env')

with (PROJECT_ROOT / 'config' / 'settings.yml').open(encoding='utf-8') as f:
    SETTINGS = yaml.safe_load(f)

DB = {
    'host': os.getenv('POSTGRES_HOST', 'localhost'),
    'port': int(os.getenv('POSTGRES_PORT', '5432')),
    'dbname': os.getenv('POSTGRES_DB', 'dss150p'),
    'user': os.getenv('POSTGRES_USER', 'dss150p'),
    # No committed default: the password must come from .env or the environment.
    'password': os.getenv('POSTGRES_PASSWORD'),
}

def path_for(key: str) -> Path:
    return PROJECT_ROOT / SETTINGS['pipeline'][key]


def db_connection_params() -> dict:
    """Connection settings for code that actually opens a DB connection (src/load, src/validate).
    Fails with a clear message if the password is missing, instead of at import time,
    so commands that never touch the database (e.g. validate-env) still work."""
    if not DB['password']:
        raise RuntimeError(
            'POSTGRES_PASSWORD is not set. Copy .env.example to .env and set it '
            '(or pass it through Docker Compose).'
        )
    return dict(DB)