"""Download the Stanford Open Policing CSV archives, smallest first.

Resumes partial downloads with HTTP Range, validates every archive with
``zipfile.testzip`` and retries with backoff. Keeps concurrency low on
purpose: stacks.stanford.edu redirected every request to its status page
after a burst of parallel requests.

Usage:
    python models/us_stanford_open_policing/code/download.py [--workers 3]
"""

import argparse
import time
import urllib.request
import zipfile
from concurrent.futures import ThreadPoolExecutor

try:
    from models.us_stanford_open_policing.code import constants
except ModuleNotFoundError:  # run as a script
    import constants  # type: ignore[no-redef]

CHUNK = 1 << 20


def _valid(path) -> bool:
    try:
        with zipfile.ZipFile(path) as z:
            return z.testzip() is None
    except (zipfile.BadZipFile, OSError):
        return False


def _remote_size(url: str) -> int:
    req = urllib.request.Request(url, method="HEAD")
    with urllib.request.urlopen(req, timeout=60) as r:
        if r.url != url:
            raise ConnectionError(f"redirected to {r.url}")
        return int(r.headers["Content-Length"])


def fetch(url: str, attempts: int = 8) -> str:
    dest = constants.INPUT_DIR / url.rsplit("/", 1)[1]
    part = dest.with_name(dest.name + ".part")
    for attempt in range(attempts):
        if dest.exists() and _valid(dest):
            return f"ok {dest.name}"
        try:
            size = _remote_size(url)
            have = part.stat().st_size if part.exists() else 0
            if have > size:
                part.unlink()
                have = 0
            if have < size:
                req = urllib.request.Request(
                    url, headers={"Range": f"bytes={have}-"}
                )
                with urllib.request.urlopen(req, timeout=120) as r:
                    if r.url != url:
                        raise ConnectionError(f"redirected to {r.url}")
                    mode = "ab" if r.status == 206 else "wb"
                    with part.open(mode) as f:
                        while chunk := r.read(CHUNK):
                            f.write(chunk)
            part.replace(dest)
            if _valid(dest):
                return f"ok {dest.name}"
            dest.unlink()
        except Exception as exc:
            print(
                f"retry {dest.name}: {type(exc).__name__}: {exc}", flush=True
            )
        time.sleep(min(600, 30 * 2**attempt))
    return f"FAIL {dest.name}"


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--workers", type=int, default=3)
    args = p.parse_args()
    constants.INPUT_DIR.mkdir(parents=True, exist_ok=True)
    order = constants.DATA_DIR / "urls_by_size.txt"
    urls = (
        order.read_text().split() if order.exists() else constants.SOURCE_URLS
    )
    with ThreadPoolExecutor(args.workers) as pool:
        for msg in pool.map(fetch, urls):
            print(msg, flush=True)


if __name__ == "__main__":
    main()
