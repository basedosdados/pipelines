"""Download every source document for au_treasury_budget.

Everything lands under ``$AU_TREASURY_BUDGET_DATA`` (default
``~/Downloads/au_treasury_budget_data``), never in the repository and never under
Dropbox.

    input/historical/<release_id>.docx    BP1 historical statement / FBO Appendix B
    input/chart_data/<release_id>.zip     the release's chart-data archive
    input/prose/<release_id>.docx         BP1 Statement 3, for the chart-3.8 note
    input/igr2023w/                       2023 IGR Word bundle, unpacked
    input/igr2023cd/                      2023 IGR chart data, unpacked

**budget.gov.au answers an unknown path with HTTP 200.** A missing document comes
back as a 7,349-byte "page not found" HTML body, not a 404, so a status check
alone would happily save a web page as ``bp1_bs-11.docx`` and the parser would
then report an empty statement rather than a bad URL. Every download here is
checked for the ZIP magic bytes that a DOCX or XLSX must begin with.
"""

from __future__ import annotations

import argparse
import io
import json
import os
import pathlib
import sys
import urllib.error
import urllib.request
import zipfile

import releases

DATA_ROOT = pathlib.Path(
    os.environ.get(
        "AU_TREASURY_BUDGET_DATA",
        pathlib.Path.home() / "Downloads" / "au_treasury_budget_data",
    )
)
INPUT = DATA_ROOT / "input"

#: Budget Paper No. 1, Statement 3 -- the statement carrying the payment-growth
#: chart and the prose that states its projection windows and its nominal basis.
#: Only the releases whose per-statement DOCX exists are listed; the note was read
#: from these four and is recorded in ``clean_payment_growth.PERIOD_OVERRIDES``.
STATEMENT_3_DOCX: dict[str, str] = {
    "budget_2026_27": "https://budget.gov.au/content/bp1/download/bp1_bs-3.docx",
    "budget_2025_26": "https://archive.budget.gov.au/2025-26/bp1/download/bp1_bs-3.docx",
    "budget_2024_25": "https://archive.budget.gov.au/2024-25/bp1/download/bp1_bs-3.docx",
    "budget_2022_23_october": (
        "https://archive.budget.gov.au/2022-23-october/bp1/download/bp1_bs-3.docx"
    ),
}

ZIP_MAGIC = (b"PK\x03\x04", b"PK\x05\x06", b"PK\x07\x08")


class DownloadError(RuntimeError):
    pass


def fetch(url: str) -> bytes:
    request = urllib.request.Request(
        url, headers={"User-Agent": releases.USER_AGENT}
    )
    try:
        with urllib.request.urlopen(request, timeout=120) as response:
            return response.read()
    except urllib.error.HTTPError as error:
        raise DownloadError(f"{url}: HTTP {error.code}") from error
    except Exception as error:
        raise DownloadError(f"{url}: {error}") from error


def fetch_zip_container(url: str) -> bytes:
    """Fetch a DOCX, XLSX or ZIP, rejecting a soft 404 disguised as HTML.

    budget.gov.au returns 200 with a short HTML body for any path it does not
    recognise. Checking the magic bytes -- every Office file and every archive is
    a ZIP -- is what distinguishes a real document from that page.
    """
    payload = fetch(url)
    if not payload.startswith(ZIP_MAGIC):
        head = payload[:120].decode("utf-8", "replace").replace("\n", " ")
        raise DownloadError(
            f"{url}: served {len(payload)} bytes that are not a ZIP container. "
            f"budget.gov.au returns {releases.SOFT_404_BYTES} bytes of HTML with "
            f"status 200 for an unknown path, so this is almost certainly a dead "
            f"URL rather than a server error. Body starts: {head!r}"
        )
    return payload


def save(path: pathlib.Path, payload: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(payload)


def unpack(payload: bytes, destination: pathlib.Path) -> list[str]:
    destination.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(io.BytesIO(payload)) as archive:
        names = [n for n in archive.namelist() if not n.endswith("/")]
        archive.extractall(destination)
    return names


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--skip-existing",
        action="store_true",
        help="leave files already on disk alone",
    )
    args = parser.parse_args()

    downloaded: list[dict[str, object]] = []
    failed: list[dict[str, object]] = []

    def get(url: str, path: pathlib.Path, label: str) -> bytes | None:
        if args.skip_existing and path.exists() and path.stat().st_size > 0:
            print(f"  skip   {label}")
            return None
        try:
            payload = fetch_zip_container(url)
        except DownloadError as error:
            print(f"  FAIL   {label}: {error}")
            failed.append({"label": label, "url": url, "error": str(error)})
            return None
        save(path, payload)
        print(f"  ok     {label}  {len(payload):,} bytes")
        downloaded.append({"label": label, "url": url, "bytes": len(payload)})
        return payload

    print("Historical-data statements (BP1 Statement 10/11, FBO Appendix B)")
    for release in releases.releases_with_historical():
        assert release.historical_url
        get(
            release.historical_url,
            INPUT / "historical" / f"{release.release_id}.docx",
            release.release_id,
        )

    print("\nChart-data archives")
    for release in releases.releases_with_chart_data():
        assert release.chart_data_url
        get(
            release.chart_data_url,
            INPUT / "chart_data" / f"{release.release_id}.zip",
            release.release_id,
        )

    print("\nBudget Paper No. 1, Statement 3 (chart-3.8 prose)")
    for release_id, url in STATEMENT_3_DOCX.items():
        get(url, INPUT / "prose" / f"{release_id}.docx", release_id)

    print("\n2023 Intergenerational Report")
    word = get(
        releases.IGR_2023_WORD_ZIP,
        INPUT / "igr2023-word.zip",
        "igr2023 word bundle",
    )
    if word:
        names = unpack(word, INPUT / "igr2023w")
        print(f"         unpacked {len(names)} files to input/igr2023w/")
    charts = get(
        releases.IGR_2023_CHART_ZIP,
        INPUT / "igr2023-chartdata.zip",
        "igr2023 chart data",
    )
    if charts:
        names = unpack(charts, INPUT / "igr2023cd")
        print(f"         unpacked {len(names)} files to input/igr2023cd/")

    (DATA_ROOT / "download_report.json").write_text(
        json.dumps({"downloaded": downloaded, "failed": failed}, indent=2)
    )
    print(f"\ndownloaded={len(downloaded)} failed={len(failed)}")
    if failed:
        print(
            "Downloads failed; the tables below would be built from partial data."
        )
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
