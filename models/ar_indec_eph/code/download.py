"""Download every published EPH wave archive from INDEC.

INDEC answers a missing file with HTTP 200 and a ~37 KB HTML page, so each
download is validated on size and magic bytes rather than on the status code.
"""

import time
from concurrent.futures import ThreadPoolExecutor

import requests

from models.ar_indec_eph.code.constants import (
    HTML_ERROR_SIZE,
    INPUT_DIR,
    waves,
)

HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; basedosdados/1.0)"}
MAGIC = {b"PK\x03\x04": "zip", b"Rar!": "rar"}


def looks_like_archive(path) -> bool:
    with open(path, "rb") as handle:
        head = handle.read(4)
    return any(head.startswith(m) for m in MAGIC)


def fetch(wave: dict) -> tuple[dict, str]:
    url = wave["url"]
    dest = INPUT_DIR / url.rsplit("/", 1)[-1]
    if (
        dest.exists()
        and dest.stat().st_size > HTML_ERROR_SIZE
        and looks_like_archive(dest)
    ):
        return wave, "cached"

    for attempt in range(4):
        try:
            with requests.get(
                url, headers=HEADERS, stream=True, timeout=180
            ) as response:
                response.raise_for_status()
                with open(dest, "wb") as handle:
                    for chunk in response.iter_content(1 << 16):
                        handle.write(chunk)
            if (
                dest.stat().st_size <= HTML_ERROR_SIZE
                or not looks_like_archive(dest)
            ):
                dest.unlink(missing_ok=True)
                raise ValueError("served the HTML error page, not an archive")
            return wave, "ok"
        except Exception as exc:
            if attempt == 3:
                return wave, f"FAIL {exc}"
            time.sleep(2 * (attempt + 1))
    return wave, "FAIL unreachable"


def main() -> int:
    INPUT_DIR.mkdir(parents=True, exist_ok=True)
    all_waves = waves()
    with ThreadPoolExecutor(6) as pool:
        results = list(pool.map(fetch, all_waves))

    failed = [(w, s) for w, s in results if s.startswith("FAIL")]
    ok = sum(1 for _, s in results if s == "ok")
    cached = sum(1 for _, s in results if s == "cached")
    total = sum(
        (INPUT_DIR / w["url"].rsplit("/", 1)[-1]).stat().st_size
        for w, s in results
        if not s.startswith("FAIL")
    )
    print(
        f"downloaded {ok}, cached {cached}, failed {len(failed)} of {len(all_waves)}"
    )
    print(f"total on disk: {total / 1e6:.0f} MB in {INPUT_DIR}")
    for wave, status in failed:
        print(
            f"  FAIL {wave['year']}Q{wave['quarter']} {wave['url']} -> {status}"
        )
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
