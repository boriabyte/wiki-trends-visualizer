import requests
from functools import lru_cache

WIKIDATA_API = "https://www.wikidata.org/w/api.php"
ENWIKI_API = "https://en.wikipedia.org/w/api.php"

# IMPORTANT: Wikimedia APIs expect a descriptive User-Agent.
USER_AGENT = "UPB-IABD-WikiTrendsDashboard/1.0 (contact: student-project)"

_session = requests.Session()
_session.headers.update({
    "User-Agent": USER_AGENT,
    "Accept": "application/json",
})

def _get(url: str, params: dict):
    r = _session.get(url, params=params, timeout=30)
    # If you still get blocked, this will surface useful details
    r.raise_for_status()
    return r

@lru_cache(maxsize=2048)
def wikidata_search_qid(query: str, limit: int = 5) -> list[dict]:
    params = {
        "action": "wbsearchentities",
        "search": query.replace("_", " "),  # accept underscores from your UI
        "language": "en",
        "format": "json",
        "limit": limit,
    }
    r = _get(WIKIDATA_API, params=params)
    data = r.json()
    return [
        {"qid": item.get("id"), "label": item.get("label"), "description": item.get("description")}
        for item in data.get("search", [])
    ]

@lru_cache(maxsize=2048)
def wikidata_get_enwiki_title(qid: str) -> str | None:
    # Use wbgetentities (API) to avoid hitting Special:EntityData which sometimes triggers stricter rules
    params = {
        "action": "wbgetentities",
        "ids": qid,
        "props": "sitelinks",
        "sitefilter": "enwiki",
        "format": "json",
    }
    r = _get(WIKIDATA_API, params=params)
    data = r.json()
    ent = data.get("entities", {}).get(qid, {})
    sitelinks = ent.get("sitelinks", {})
    enwiki = sitelinks.get("enwiki")
    return enwiki.get("title") if enwiki else None

@lru_cache(maxsize=2048)
def enwiki_get_redirect_titles(canonical_title: str, hard_cap: int = 5000) -> list[str]:
    redirects = []
    cont = None

    while True:
        params = {
            "action": "query",
            "format": "json",
            "titles": canonical_title,
            "prop": "redirects",
            "rdlimit": "max",
        }
        if cont:
            params["rdcontinue"] = cont

        r = _get(ENWIKI_API, params=params)
        data = r.json()

        pages = data.get("query", {}).get("pages", {})
        for _, page in pages.items():
            for rd in page.get("redirects", []):
                t = rd.get("title")
                if t:
                    redirects.append(t)

        cont = data.get("continue", {}).get("rdcontinue")
        if not cont or len(redirects) >= hard_cap:
            break

    # Deduplicate preserve order
    seen = set()
    out = []
    for t in redirects:
        if t not in seen:
            seen.add(t)
            out.append(t)
    return out

def normalize_to_dump_title(title: str) -> str:
    return title.replace(" ", "_")
