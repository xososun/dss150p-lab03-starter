import pandas as pd
from src.config import path_for
from src.validate.quality import validate_curated

path = path_for('curated_dir') / 'run_id=verify1' / 'sales_order_lines.parquet'
df = pd.read_parquet(path)

broken = df.copy()
broken.loc[broken.index[0], 'quantity'] = 999          # out of range
broken.loc[broken.index[1], 'status'] = 'NOT_A_STATUS'  # not allowed
broken = pd.concat([broken, broken.iloc[[0]]])          # duplicate order_id

errors = validate_curated(broken)
if not errors:
    print("no errors found — something's wrong with the test")
for e in errors:
    print(e)