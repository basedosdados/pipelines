"""Download the Justiça em Números consolidated database from the CNJ portal.

The zip filename embeds its release date (``2026/06/23-jun-2026.zip``) and
changes with every annual release, so the URL is discovered by scraping the
"Base de Dados" page rather than hardcoded. A hardcoded name goes stale at the
next release and the pipeline would silently keep re-downloading last year's
file.
"""

from __future__ import annotations

import os
import re
import zipfile
from pathlib import Path

import requests

BASE_PAGE = "https://www.cnj.jus.br/pesquisas-judiciarias/justica-em-numeros/base-de-dados/"

# The portal rejects requests without a browser user agent.
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/140.0 Safari/537.36"
    )
}

DATA_ROOT = Path(
    os.environ.get(
        "BR_CNJ_DATAJUD_DATA",
        Path.home() / "Downloads" / "br_cnj_datajud_data",
    )
)
INPUT_DIR = DATA_ROOT / "input"

# The page links several zips; the database release is the one whose name is a
# date, e.g. "23-jun-2026.zip". The other link is an unrelated legacy upload.
RELEASE_RE = re.compile(
    r"https://www\.cnj\.jus\.br/wp-content/uploads/\d{4}/\d{2}/"
    r"\d{1,2}-[a-z]{3}-\d{4}\.zip",
    re.IGNORECASE,
)


def find_release_url() -> str:
    """Return the URL of the most recent database zip advertised on the page."""
    response = requests.get(BASE_PAGE, headers=HEADERS, timeout=120)
    response.raise_for_status()
    urls = sorted(set(RELEASE_RE.findall(response.text)))
    if not urls:
        raise RuntimeError(
            f"No Justiça em Números release zip found on {BASE_PAGE}. "
            "The portal layout or the filename convention has changed."
        )

    # Sort by the upload path's year/month, which tracks the release order.
    def upload_period(url: str) -> tuple[int, int]:
        # The pattern already matched in RELEASE_RE, so a miss here is
        # impossible; sort such a URL last rather than raising.
        match = re.search(r"/uploads/(\d{4})/(\d{2})/", url)
        if match is None:
            return (0, 0)
        return int(match.group(1)), int(match.group(2))

    return max(urls, key=upload_period)


def download(url: str | None = None) -> Path:
    """Download and extract the release zip. Returns the extraction directory."""
    url = url or find_release_url()
    INPUT_DIR.mkdir(parents=True, exist_ok=True)
    archive = INPUT_DIR / url.rsplit("/", 1)[-1]

    if not archive.exists():
        print(f"downloading {url}")
        with requests.get(
            url, headers=HEADERS, timeout=600, stream=True
        ) as response:
            response.raise_for_status()
            declared = int(response.headers.get("Content-Length", 0))
            written = 0
            tmp = archive.with_suffix(archive.suffix + ".part")
            with open(tmp, "wb") as handle:
                for chunk in response.iter_content(chunk_size=1 << 20):
                    handle.write(chunk)
                    written += len(chunk)
        # A dropped connection mid-stream looks like a successful download;
        # compare against Content-Length before accepting the file.
        if declared and written != declared:
            tmp.unlink(missing_ok=True)
            raise RuntimeError(
                f"truncated download: got {written} bytes, expected {declared}"
            )
        tmp.rename(archive)
    else:
        print(f"reusing {archive}")

    with zipfile.ZipFile(archive) as zf:
        zf.extractall(INPUT_DIR)
        names = zf.namelist()
    print(f"extracted {len(names)} files: {names}")
    return INPUT_DIR


if __name__ == "__main__":
    download()
