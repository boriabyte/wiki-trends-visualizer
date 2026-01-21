import time
import random
from pathlib import Path
from urllib.parse import urljoin
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
from bs4 import BeautifulSoup


BASE_URL = "https://dumps.wikimedia.org/other/pageviews/2026/2026-01/"
OUT_DIR = Path("data/raw/gz files/january")

MAX_WORKERS = 6
CHUNK_SIZE = 1024 * 1024  # 1 MB
MAX_RETRIES = 8


def list_gz_urls(base_url: str) -> list[str]:
    """List all .gz URLs from the Wikimedia pageviews directory."""
    resp = requests.get(base_url, timeout=60)
    resp.raise_for_status()

    soup = BeautifulSoup(resp.text, "html.parser")

    urls = [
        urljoin(base_url, a["href"])
        for a in soup.find_all("a", href=True)
        if a["href"].endswith(".gz")
    ]
    return sorted(urls)


def download_one(url: str) -> str:
    """Download one .gz file with retries and atomic write."""
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    filename = url.split("/")[-1]
    out_path = OUT_DIR / filename
    tmp_path = out_path.with_suffix(out_path.suffix + ".part")

    # Idempotent: skip already-downloaded files
    if out_path.exists() and out_path.stat().st_size > 0:
        return f"SKIP {filename}"

    session = requests.Session()

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            with session.get(url, stream=True, timeout=120) as r:
                if r.status_code in (429, 502, 503, 504):
                    raise requests.HTTPError(
                        f"{r.status_code} transient error", response=r
                    )

                r.raise_for_status()

                with open(tmp_path, "wb") as f:
                    for chunk in r.iter_content(chunk_size=CHUNK_SIZE):
                        if chunk:
                            f.write(chunk)

            tmp_path.replace(out_path)
            return f"DONE {filename}"

        except (requests.Timeout, requests.ConnectionError, requests.HTTPError) as e:
            code = getattr(getattr(e, "response", None), "status_code", None)
            if isinstance(e, requests.HTTPError) and code not in (429, 502, 503, 504):
                raise

            sleep_s = min(60, 2 ** (attempt - 1)) + random.random()
            time.sleep(sleep_s)

    raise RuntimeError(f"FAILED after {MAX_RETRIES} retries: {filename}")


def fetch_data():
    """Fetch all pageview .gz files, skipping those already present."""
    urls = list_gz_urls(BASE_URL)
    print(f"Found {len(urls)} files")

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
        futures = [pool.submit(download_one, url) for url in urls]
        for fut in as_completed(futures):
            print(fut.result())
