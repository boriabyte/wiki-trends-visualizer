import streamlit as st
import duckdb
import pandas as pd

try:
    import altair as alt
    _HAS_ALTAIR = True
except Exception:
    _HAS_ALTAIR = False

from topic_series import build_topic_series

TREND_GLOB = "data/aggregates/pageviews_daily_trending/**/*.parquet"
DAILY_GLOB = "data/aggregates/pageviews_daily/**/*.parquet"
HOURLY_GLOB = "data/processed/pageviews_hourly/**/*.parquet"

# Display-name mapping (UI only)
DISPLAY_RENAME = {
    "dt": "Date",
    "title": "Title",
    "views": "Views",
    "delta": "Daily change",
    "up_score": "Upward trend score",
    "down_score": "Downward trend score",
    "trend_score": "Overall trend score",
}

st.set_page_config(layout="wide")
st.title("Wikipedia Trends Dashboard")

con = duckdb.connect()

# -----------------------------
# Helpers
# -----------------------------
def get_available_trending_dates() -> list[str]:
    df = con.execute(
        f"""
        SELECT DISTINCT dt
        FROM read_parquet('{TREND_GLOB}')
        ORDER BY dt DESC
        """
    ).df()
    return df["dt"].astype(str).tolist()

def get_latest_trending_date() -> str | None:
    df = con.execute(
        f"""
        SELECT MAX(dt) AS latest_dt
        FROM read_parquet('{TREND_GLOB}')
        """
    ).df()
    if df.empty or pd.isna(df.loc[0, "latest_dt"]):
        return None
    return str(df.loc[0, "latest_dt"])

def pretty_title(t: str) -> str:
    return t.replace("_", " ")

def make_bar_chart(df: pd.DataFrame, x_col: str, y_col: str, tooltip_cols: list[str], x_title: str, y_title: str):
    if not _HAS_ALTAIR or df.empty:
        return None
    return (
        alt.Chart(df)
        .mark_bar()
        .encode(
            x=alt.X(f"{x_col}:Q", title=x_title),
            y=alt.Y(f"{y_col}:N", sort="-x", title=y_title),
            tooltip=tooltip_cols,
        )
    )

def make_line_chart(df: pd.DataFrame, x_col: str, y_col: str, tooltip_cols: list[str], x_title: str, y_title: str):
    if not _HAS_ALTAIR or df.empty:
        return None
    return (
        alt.Chart(df)
        .mark_line()
        .encode(
            x=alt.X(f"{x_col}:T", title=x_title),
            y=alt.Y(f"{y_col}:Q", title=y_title),
            tooltip=tooltip_cols,
        )
    )

def display_table(df: pd.DataFrame) -> pd.DataFrame:
    # Rename columns for display only; keep raw df for charts/calcs
    out = df.copy()
    out = out.rename(columns={k: v for k, v in DISPLAY_RENAME.items() if k in out.columns})
    return out

# -----------------------------
# Date selector
# -----------------------------
dates = get_available_trending_dates()
if not dates:
    st.error("No trending data found. Check TREND_GLOB and confirm your trending parquet exists.")
    st.stop()

latest_dt = get_latest_trending_date()
default_index = dates.index(latest_dt) if latest_dt in dates else 0
selected_dt = st.selectbox("Select date", dates, index=default_index)

# -----------------------------
# Trending panels (Up / Down)
# -----------------------------
st.subheader("Trending (for selected date)")

col_up, col_down = st.columns(2)

with col_up:
    st.markdown("### Trending Up (increasing attention)")

    df_up = con.execute(
        f"""
        SELECT dt, title, views, delta, up_score, down_score
        FROM read_parquet('{TREND_GLOB}')
        WHERE dt = ?
        ORDER BY up_score DESC
        LIMIT 20
        """,
        [selected_dt],
    ).df()

    if not df_up.empty:
        # RAW for charts
        df_up_chart = df_up.copy()
        df_up_chart["title"] = df_up_chart["title"].map(pretty_title)
        for c in ["views", "delta", "up_score", "down_score"]:
            if c in df_up_chart.columns:
                df_up_chart[c] = pd.to_numeric(df_up_chart[c], errors="coerce")

        # Pretty for table
        df_up_disp = display_table(df_up_chart)

        st.dataframe(df_up_disp, use_container_width=True, hide_index=True)

        chart = make_bar_chart(
            df_up_chart,  # IMPORTANT: keep raw col names for plotting
            x_col="views",
            y_col="title",
            tooltip_cols=["title", "views", "delta", "up_score"],
            x_title="Views",
            y_title="Title",
        )
        if chart is not None:
            st.altair_chart(chart, use_container_width=True)
        else:
            st.bar_chart(df_up_chart.set_index("title")["views"])
    else:
        st.info("No rows for this date under current thresholds.")

with col_down:
    st.markdown("### Trending Down (declining attention)")

    df_down = con.execute(
        f"""
        SELECT dt, title, views, delta, up_score, down_score
        FROM read_parquet('{TREND_GLOB}')
        WHERE dt = ?
          AND delta < 0
        ORDER BY down_score DESC
        LIMIT 20
        """,
        [selected_dt],
    ).df()

    if not df_down.empty:
        # RAW for charts
        df_down_chart = df_down.copy()
        df_down_chart["title"] = df_down_chart["title"].map(pretty_title)
        for c in ["views", "delta", "up_score", "down_score"]:
            if c in df_down_chart.columns:
                df_down_chart[c] = pd.to_numeric(df_down_chart[c], errors="coerce")

        # Pretty for table
        df_down_disp = display_table(df_down_chart)

        st.dataframe(df_down_disp, use_container_width=True, hide_index=True)

        chart = make_bar_chart(
            df_down_chart,  # IMPORTANT: keep raw col names for plotting
            x_col="down_score",
            y_col="title",
            tooltip_cols=["title", "views", "delta", "down_score"],
            x_title="Downward trend score",
            y_title="Title",
        )
        if chart is not None:
            st.altair_chart(chart, use_container_width=True)
        else:
            st.bar_chart(df_down_chart.set_index("title")["down_score"])
    else:
        st.info("No negative-delta rows for this date under current thresholds.")

st.divider()

# -----------------------------
# Explore section (Title search + Topic canonicalization)
# -----------------------------
st.subheader("Explore")

tab1, tab2 = st.tabs(["Article (dataset search)", "Topic (Wikidata canonicalization)"])

# --- Tab 1: Article explorer ---
with tab1:
    st.markdown("Search for an article title in your dataset, then view daily and hourly series.")
    q = st.text_input("Search title (spaces ok). Example: New York City", key="article_search")

    selected_title = None
    if q:
        candidates = con.execute(
            f"""
            SELECT title, SUM(views) AS total_views
            FROM read_parquet('{DAILY_GLOB}')
            WHERE title ILIKE '%' || REPLACE(?, ' ', '_') || '%'
            GROUP BY title
            ORDER BY total_views DESC
            LIMIT 50
            """,
            [q],
        ).df()

        if candidates.empty:
            st.warning("No matches found. Try fewer characters.")
        else:
            candidates["label"] = (
                candidates["title"].map(pretty_title)
                + "  ("
                + candidates["total_views"].fillna(0).astype(int).astype(str)
                + " views)"
            )
            label = st.selectbox("Select a title", candidates["label"].tolist(), key="article_select")
            selected_title = candidates.loc[candidates["label"] == label, "title"].iloc[0]

    if selected_title:
        st.write(f"Selected title: `{selected_title}`")

        # Daily series (all available dates)
        df_ts = con.execute(
            f"""
            SELECT dt, SUM(views) AS views
            FROM read_parquet('{DAILY_GLOB}')
            WHERE title = ?
            GROUP BY dt
            ORDER BY dt
            """,
            [selected_title],
        ).df()

        if df_ts.empty:
            st.warning("No daily data found for this title.")
        else:
            if _HAS_ALTAIR:
                chart = make_line_chart(
                    df_ts,
                    x_col="dt",
                    y_col="views",
                    tooltip_cols=["dt", "views"],
                    x_title="Date",
                    y_title="Daily views",
                )
                st.altair_chart(chart, use_container_width=True)
            else:
                st.line_chart(df_ts.set_index("dt")["views"])

        # Hourly series (for selected date)
        df_hr = con.execute(
            f"""
            SELECT hour, SUM(views) AS views
            FROM read_parquet('{HOURLY_GLOB}')
            WHERE dt = ?
              AND title = ?
            GROUP BY hour
            ORDER BY hour
            """,
            [selected_dt, selected_title],
        ).df()

        st.markdown(f"#### Hourly views on {selected_dt}")
        if df_hr.empty:
            st.info("No hourly rows found for this title on the selected date.")
        else:
            if _HAS_ALTAIR:
                chart = (
                    alt.Chart(df_hr)
                    .mark_bar()
                    .encode(
                        x=alt.X("hour:O", title="Hour (0-23)"),
                        y=alt.Y("views:Q", title="Views"),
                        tooltip=["hour", "views"],
                    )
                )
                st.altair_chart(chart, use_container_width=True)
            else:
                st.bar_chart(df_hr.set_index("hour")["views"])

# --- Tab 2: Topic explorer via Wikidata canonicalization ---
with tab2:
    st.markdown(
        "Resolves your query to a Wikidata entity (QID), gets the canonical enwiki title, "
        "fetches enwiki redirects, and sums pageviews across any of those titles that exist in your dataset."
    )

    topic_q = st.text_input("Topic (e.g., Romania, Greenland, Golden Globe Awards)", key="topic_search")

    projects = st.multiselect(
        "Projects to include",
        ["en", "en.m"],
        default=["en"],
        help="Include `en.m` if you ingested mobile project data; it will be merged with desktop.",
        key="topic_projects",
    )

    if topic_q:
        series_df, meta = build_topic_series(con, topic_q, projects=tuple(projects))

        if series_df is None:
            st.error(meta.get("error", "Unknown error"))
            st.json(meta)
        else:
            st.write(f"QID: `{meta['qid']}`")
            st.write(f"Canonical enwiki title: `{meta['canonical_title']}`")
            st.write(
                f"Redirects found: {meta['redirect_count']}; matched titles in your dataset: {meta['matched_titles_count']}"
            )

            with st.expander("Matched titles (sample)"):
                st.write([pretty_title(t) for t in meta["matched_titles_sample"]])

            st.markdown("#### Topic daily views (canonical + redirects)")
            if _HAS_ALTAIR:
                chart = make_line_chart(
                    series_df,
                    x_col="dt",
                    y_col="views_topic",
                    tooltip_cols=["dt", "views_topic"],
                    x_title="Date",
                    y_title="Topic views",
                )
                st.altair_chart(chart, use_container_width=True)
            else:
                st.line_chart(series_df.set_index("dt")["views_topic"])

con.close()
