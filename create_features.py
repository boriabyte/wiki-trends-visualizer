from pathlib import Path
import duckdb

DAILY_GLOB = "data/aggregates/pageviews_daily/dt=*/data_*.parquet"
FEAT_OUT_DIR = Path("data/aggregates/pageviews_daily_features")
FEAT_OUT_DIR.mkdir(parents=True, exist_ok=True)

def build_features():
    con = duckdb.connect()

    rows = con.execute(
        f"SELECT COUNT(*) FROM read_parquet('{DAILY_GLOB}')"
    ).fetchone()[0]
    if rows == 0:
        con.close()
        raise RuntimeError("No daily data found. Run aggregation first.")

    con.execute(f"""
        COPY (
            WITH base AS (
                SELECT dt, project, title, views
                FROM read_parquet('{DAILY_GLOB}')
            ),
            w AS (
                SELECT
                    dt,
                    project,
                    title,
                    views,
                    LAG(views) OVER (PARTITION BY project, title ORDER BY dt) AS views_prev,
                    AVG(views) OVER (
                        PARTITION BY project, title
                        ORDER BY dt
                        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
                    ) AS ma7,
                    STDDEV_SAMP(views) OVER (
                        PARTITION BY project, title
                        ORDER BY dt
                        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
                    ) AS sd7
                FROM base
            )
            SELECT
                dt,
                project,
                title,
                views,
                views_prev,
                views - views_prev AS delta,
                CASE
                    WHEN views_prev IS NULL OR views_prev = 0 THEN NULL
                    ELSE (views - views_prev) * 1.0 / views_prev
                END AS pct_change,
                ma7,
                CASE
                    WHEN sd7 IS NULL OR sd7 = 0 THEN NULL
                    ELSE (views - ma7) * 1.0 / sd7
                END AS z7
            FROM w
        )
        TO '{FEAT_OUT_DIR.as_posix()}'
        (FORMAT PARQUET, PARTITION_BY (dt), COMPRESSION ZSTD, OVERWRITE 1);
    """)

    con.close()
    print("Daily features written to:", FEAT_OUT_DIR)
