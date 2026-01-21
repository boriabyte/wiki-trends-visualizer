from pathlib import Path
import duckdb

FEATURES_GLOB = "data/aggregates/pageviews_daily_features/dt=*/data_*.parquet"

TREND_OUT_DIR = Path("data/aggregates/pageviews_daily_trending")
TREND_OUT_DIR.mkdir(parents=True, exist_ok=True)

MIN_VIEWS_TODAY = 0
MIN_MA7 = 100
MIN_PREV = 50

def build_trends():
    con = duckdb.connect()

    rows = con.execute(
        f"SELECT COUNT(*) FROM read_parquet('{FEATURES_GLOB}')"
    ).fetchone()[0]
    if rows == 0:
        con.close()
        raise RuntimeError("No feature data found. Run build_features first.")

    con.execute(f"""
        COPY (
            SELECT
                *,
                (LN(views + 1) * 0.5) +
                (LN(GREATEST(delta, 0) + 1) * 1.0) +
                COALESCE(z7, 0) AS up_score,

                (LN(views + 1) * 0.5) +
                (LN(GREATEST(-delta, 0) + 1) * 1.0) +
                COALESCE(z7, 0) AS down_score,

                (LN(views + 1) * 0.5) +
                (SIGN(delta) * LN(ABS(delta) + 1) * 1.0) +
                COALESCE(z7, 0) AS trend_score
            FROM read_parquet('{FEATURES_GLOB}')
            WHERE
                title NOT LIKE '%:%'
                AND title IS NOT NULL
                AND title <> '-'
                AND title <> ''
                AND views >= {MIN_VIEWS_TODAY}
                AND ma7 >= {MIN_MA7}
                AND (views_prev IS NULL OR views_prev >= {MIN_PREV})
        )
        TO '{TREND_OUT_DIR.as_posix()}'
        (FORMAT PARQUET, PARTITION_BY (dt), COMPRESSION ZSTD, OVERWRITE 1);
    """)

    con.close()
    print("Done. Trending dataset written to:", TREND_OUT_DIR)
