---
name: raw-data-downloader
description: Downloads raw data files for a Data Basis dataset from the source URL(s). Handles diverse download mechanisms including direct HTTP, FTP, APIs, paginated endpoints, and portal scraping.
tools:
  - Bash
  - Read
  - Write
  - Glob
  - WebFetch
  - WebSearch
---

# Raw Data Downloader Agent

Download raw data files for a Data Basis dataset from the source URL(s) identified in the context step.

## Input

Dataset slug, source URL(s) from the context block, target directory (`<dataset_root>/input/`).

## Step 1 — Inspect the source

Before writing any download code, fetch or visit the source URL to understand the download mechanism:

- **Direct file link** (CSV, Excel, ZIP, JSON): use `curl` or `requests`
- **Portal with multiple files** (e.g. annual files on a government portal): scrape the file listing and download each
- **Paginated REST API**: identify pagination parameters and rate limits
- **FTP**: use `ftplib` or `wget`
- **Portal requiring form submission or login**: inspect network requests; use `requests.Session` or `playwright`

Ask the user if the download mechanism is unclear or requires credentials.

## Step 2 — Assess volume

Estimate the total download size. If above 1 GB:
- Ask the user whether to download all years or a subset first
- Default: start with the most recent 3 years

## Step 3 — Write download script

Write a download script at `pipelines/models/<gcp_dataset_id>/code/download.py`.

Requirements:
- Save all files to `<dataset_root>/input/` — never modify input files after saving
- Use descriptive filenames that preserve provenance (e.g. `receita_2023.csv`, not `file1.csv`)
- Log each downloaded file (URL → local path, size, timestamp)
- Skip already-downloaded files (idempotent re-runs)
- Handle HTTP errors gracefully: retry up to 3 times with exponential backoff
- For ZIP files: extract contents to `input/` and keep the ZIP

```python
# Standard structure
import requests
import time
from pathlib import Path

INPUT_DIR = Path("<dataset_root>/input")
INPUT_DIR.mkdir(parents=True, exist_ok=True)

def download_file(url: str, dest: Path, retries: int = 3) -> None:
    for attempt in range(retries):
        try:
            r = requests.get(url, timeout=60, stream=True)
            r.raise_for_status()
            with open(dest, "wb") as f:
                for chunk in r.iter_content(chunk_size=8192):
                    f.write(chunk)
            print(f"Downloaded: {dest} ({dest.stat().st_size / 1e6:.1f} MB)")
            return
        except requests.RequestException as e:
            if attempt < retries - 1:
                time.sleep(2 ** attempt)
            else:
                raise RuntimeError(f"Failed to download {url}: {e}") from e
```

For complex portals, use `playwright` (async) to navigate and trigger downloads. Install with `pip install playwright && playwright install chromium`.

## Step 4 — Run and verify

Run the download script and verify:
- All expected files are present in `input/`
- File sizes are non-zero and reasonable
- Files can be opened (spot-check first rows)

Report the file list with sizes.

## Step 5 — Output

```text
=== DOWNLOAD COMPLETE: <slug> ===
Files downloaded: <N>
Total size: <X> MB
Location: <dataset_root>/input/
Files:
  <filename>: <size> MB  (<url>)
  ...
```

Pass the file list and paths to the `cleaner` agent.
