import duckdb
from canonicalize_topic import (
    wikidata_search_qid,
    wikidata_get_enwiki_title,
    enwiki_get_redirect_titles,
    normalize_to_dump_title,
)

DAILY_GLOB = "data/aggregates/pageviews_daily/**/*.parquet"

def build_topic_series(con: duckdb.DuckDBPyConnection, query: str, projects=("en",)):
    candidates = wikidata_search_qid(query, limit=5)
    if not candidates:
        return None, {"error": "No Wikidata matches"}

    qid = candidates[0]["qid"]
    canonical = wikidata_get_enwiki_title(qid)
    if not canonical:
        return None, {"error": "No enwiki sitelink", "qid": qid}

    redirects = enwiki_get_redirect_titles(canonical)
    titles = [canonical] + redirects
    titles = [normalize_to_dump_title(t) for t in titles]

    existing = con.execute(
        f"""
        SELECT title
        FROM read_parquet('{DAILY_GLOB}')
        WHERE project IN (SELECT * FROM UNNEST(?))
          AND title IN (SELECT * FROM UNNEST(?))
        GROUP BY title
        """,
        [list(projects), titles],
    ).df()

    matched_titles = existing["title"].tolist()
    if not matched_titles:
        return None, {
            "error": "Canonical + redirects not found in dataset",
            "qid": qid,
            "canonical_title": canonical,
            "redirect_count": len(redirects),
        }

    series = con.execute(
        f"""
        SELECT dt, SUM(views) AS views_topic
        FROM read_parquet('{DAILY_GLOB}')
        WHERE project IN (SELECT * FROM UNNEST(?))
          AND title IN (SELECT * FROM UNNEST(?))
        GROUP BY dt
        ORDER BY dt
        """,
        [list(projects), matched_titles],
    ).df()

    meta = {
        "qid": qid,
        "canonical_title": canonical,
        "redirect_count": len(redirects),
        "matched_titles_count": len(matched_titles),
        "matched_titles_sample": matched_titles[:50],
    }
    return series, meta
