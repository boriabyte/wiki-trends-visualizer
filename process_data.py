import os
import re
import gzip
from pathlib import Path
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

IN_DIR = Path("data/raw/gz files/january")
OUT_DIR = Path("data/processed/pageviews_hourly")

PROJECTS = {"en", "en.m"} 

FILENAME_RE = re.compile(r"pageviews-(\d{4})(\d{2})(\d{2})-(\d{2})\d{4}\.gz$")

def parse_one_gz_to_parquet(gz_path: Path, batch_rows: int = 500_000) -> None:
    m = FILENAME_RE.match(gz_path.name)
    if not m:
        raise ValueError(f"Unexpected filename format: {gz_path.name}")

    yyyy, mm, dd, hh = m.groups()
    dt = f"{yyyy}-{mm}-{dd}"

    out_dir = OUT_DIR / f"dt={dt}" / f"hour={hh}"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_file = out_dir / f"part-{gz_path.stem}.parquet"

    # skip if already processed
    if out_file.exists():
        return

    rows = []
    total_kept = 0

    with gzip.open(gz_path, "rt", encoding="utf-8", errors="replace") as f:
        for line in f:
            parts = line.rstrip("\n").split(" ")
            if len(parts) < 3:
                continue

            project = parts[0]
            if project not in PROJECTS:
                continue

            title = parts[1]
            try:
                views = int(parts[2])
            except ValueError:
                continue

            if ":" in title and not title.startswith("Category:"):
                continue

            rows.append((dt, int(hh), project, title, views))
            total_kept += 1

            if len(rows) >= batch_rows:
                df = pd.DataFrame(rows, columns=["dt", "hour", "project", "title", "views"])
                table = pa.Table.from_pandas(df, preserve_index=False)
                pq.write_table(table, out_file, compression="zstd")
                rows.clear() 
                break

    if rows:
        df = pd.DataFrame(rows, columns=["dt", "hour", "project", "title", "views"])
        table = pa.Table.from_pandas(df, preserve_index=False)
        pq.write_table(table, out_file, compression="zstd")

    if total_kept == 0 and not out_file.exists():
        df = pd.DataFrame([], columns=["dt", "hour", "project", "title", "views"])
        table = pa.Table.from_pandas(df, preserve_index=False)
        pq.write_table(table, out_file, compression="zstd")

def process_data():
    gz_files = sorted(IN_DIR.glob("*.gz"))
    print(f"Found {len(gz_files)} gz files")

    for i, gz in enumerate(gz_files, 1):
        parse_one_gz_to_parquet(gz)
        if i % 20 == 0:
            print(f"Processed {i}/{len(gz_files)}")

