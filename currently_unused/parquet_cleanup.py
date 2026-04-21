import shutil
import os

TARGET = "runs"          # or "parquet" if you use that folder

if os.path.isdir(TARGET):
    shutil.rmtree(TARGET, ignore_errors=True)
    os.makedirs(TARGET, exist_ok=True)
print(f"cleaned {TARGET}")