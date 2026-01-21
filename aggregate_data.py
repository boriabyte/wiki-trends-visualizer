from pathlib import Path
import duckdb

HOURLY_GLOB = "data/processed/pageviews_hourly/dt=*/hour=*/part-*.parquet"
DAILY_OUT_DIR = Path("data/aggregates/pageviews_daily")
DAILY_OUT_DIR.mkdir(parents=True, exist_ok=True)

def aggregate_data():
    print("Starting daily aggregation...")
    print(f"Reading hourly files via glob:\n  {HOURLY_GLOB}")
    print(f"Writing output to:\n  {DAILY_OUT_DIR.resolve()}")

    con = duckdb.connect()

    row_count = con.execute(
        f"SELECT COUNT(*) FROM read_parquet('{HOURLY_GLOB}')"
    ).fetchone()[0]

    print(f"Total rows found in hourly parquet: {row_count:,}")
    if row_count == 0:
        con.close()
        raise RuntimeError("No hourly rows found. Check HOURLY_GLOB.")

    con.execute(f"""
        COPY (
            SELECT
                dt,
                project,
                title,
                SUM(views) AS views
            FROM read_parquet('{HOURLY_GLOB}')
            GROUP BY dt, project, title
        )
        TO '{DAILY_OUT_DIR.as_posix()}'
        (FORMAT PARQUET, PARTITION_BY (dt), COMPRESSION ZSTD, OVERWRITE 1);
    """)

    con.close()

    out_files = list(DAILY_OUT_DIR.rglob("*.parquet"))
    print(f"Done. Parquet files written: {len(out_files)}")
    if out_files:
        print("Example output file:")
        print(f"  {out_files[0].resolve()}")


#if __name__ == "__main__":
#    aggregate_hourly_to_daily()
