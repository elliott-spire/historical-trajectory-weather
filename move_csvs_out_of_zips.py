from pathlib import Path
import os

for path in Path("ZIPs").rglob("*.csv"):
    filepath = str(path)
    csv = filepath.split("/")[-1]
    os.rename(filepath, csv)
