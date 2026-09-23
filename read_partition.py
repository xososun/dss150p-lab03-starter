from src.benchmark.storage import read_partition
from src.config import path_for
df = read_partition(path_for('partition_dir'), 2026, 1)
print(len(df), "rows for 2026-01")