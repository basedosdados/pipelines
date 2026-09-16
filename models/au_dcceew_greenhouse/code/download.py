"""Download Australia's National Greenhouse Gas Inventory from the ANGA OData API.

Source: https://greenhouseaccounts.climatechange.gov.au/OData/ (DCCEEW, CC BY 4.0).

Three classification families, ten jurisdictions each (30 entity sets):
  - AR5_ParisInventory_<JUR>  -> UNFCCC/Paris category inventory (year x category x gas)
  - AR5_ANZSIC_<JUR>          -> economic-sector (ANZSIC) reclassification (year x sector x gas)
  - AR5_ScopeTwo_<JUR>        -> indirect (Scope 2, electricity) emissions (year x sector)

Notes that shaped this code:
  - dcceew.gov.au blocks scripted GETs (HTTP 403); the ANGA OData host does NOT,
    but it wants a browser User-Agent, so we send one.
  - The server IGNORES $top and rejects $count, so there is no paging: each call
    streams the full entity set. We download each set once, in full, to input/.

Pure functions here (no Prefect) so the recurring pipeline can import them.

Usage:
    uv run python models/au_dcceew_greenhouse/code/download.py
"""

import os
import time
from pathlib import Path

import requests

BASE = "https://greenhouseaccounts.climatechange.gov.au/OData"
UA = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36"
)

# family API prefix -> our table slug
FAMILIES = {
    "AR5_ParisInventory": "inventory_unfccc",
    "AR5_ANZSIC": "inventory_anzsic",
    "AR5_ScopeTwo": "inventory_scope2",
}

# OData entity-set jurisdiction token -> our lowercase jurisdiction code
JURISDICTIONS = {
    "AUSTRALIA": "australia",
    "ACT": "act",
    "ET": "et",
    "NSW": "nsw",
    "NT": "nt",
    "QLD": "qld",
    "SA": "sa",
    "TAS": "tas",
    "VIC": "vic",
    "WA": "wa",
}

OUTPUT_ROOT = Path(
    os.environ.get(
        "AU_DCCEEW_GREENHOUSE_DATA",
        Path.home() / "Downloads" / "au_dcceew_greenhouse_data",
    )
)


def fetch_entity_set(
    family_prefix: str, jur_token: str, retries: int = 4
) -> bytes:
    """Return the raw JSON bytes of one OData entity set (the whole set)."""
    url = f"{BASE}/{family_prefix}_{jur_token}?$format=json"
    last = None
    for attempt in range(retries):
        try:
            r = requests.get(url, headers={"User-Agent": UA}, timeout=300)
            r.raise_for_status()
            return r.content
        except Exception as e:
            last = e
            time.sleep(3 * (attempt + 1))
    raise RuntimeError(f"failed to fetch {url}: {last}")


def download_all(input_dir: Path | None = None) -> Path:
    """Download all 30 entity sets to <input>/<table_slug>/<jur_code>.json."""
    input_dir = input_dir or (OUTPUT_ROOT / "input")
    for family_prefix, table_slug in FAMILIES.items():
        out = input_dir / table_slug
        out.mkdir(parents=True, exist_ok=True)
        for jur_token, jur_code in JURISDICTIONS.items():
            dest = out / f"{jur_code}.json"
            content = fetch_entity_set(family_prefix, jur_token)
            dest.write_bytes(content)
            print(
                f"  {table_slug}/{jur_code}: {len(content) / 1e6:.2f} MB",
                flush=True,
            )
    return input_dir


if __name__ == "__main__":
    print(f"Downloading to {OUTPUT_ROOT / 'input'}", flush=True)
    download_all()
    print("done", flush=True)
