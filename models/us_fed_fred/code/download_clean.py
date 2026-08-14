"""
us_fed_fred — download + clean (one-shot onboarding).

Pulls the curated public-domain seed series (see ../SEED_SERIES.md) from the FRED
REST API and writes two partitioned/typed parquet outputs conforming to the
architecture CSVs:

  output/observation/year=YYYY/data.parquet   long: year, date, series_id, value
  output/series/data.parquet                  catalog: one row per series

License gate (both applied):
  1. Source allowlist  — keep only U.S.-federal-agency sources (public domain).
  2. "Copyright" in /series notes — FRED's own marker for restricted series.
Every dropped series is logged to code/excluded_series.csv.

Credential: FRED_API_KEY from the environment ONLY (or a gitignored .env at the
scratch dir). Never hard-coded, never committed.

Pure functions here are relocated to pipelines/datasets/us_fed_fred/utils.py at
the recurring-pipeline step and imported back — do not duplicate the transform.
"""

from __future__ import annotations

import argparse
import csv
import os
import time
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import requests

BASE = "https://api.stlouisfed.org/fred"
DATA_ROOT = Path(
    os.environ.get(
        "US_FED_FRED_DATA", Path.home() / "Downloads" / "us_fed_fred_data"
    )
)
CODE_DIR = Path(__file__).resolve().parent

# U.S. federal-agency sources whose works are public domain (17 U.S.C. §105).
SOURCE_ALLOWLIST = {
    "Board of Governors of the Federal Reserve System (US)",
    "U.S. Bureau of Labor Statistics",
    "U.S. Bureau of Economic Analysis",
    "U.S. Census Bureau",
    "U.S. Department of the Treasury. Fiscal Service",
    "U.S. Office of Management and Budget",
    "U.S. Employment and Training Administration",
    "Federal Reserve Bank of St. Louis",
}

# The seed set. Kept inline so the script is self-contained; mirrors SEED_SERIES.md.
SEED_SERIES = [
    # Board of Governors of the Federal Reserve System
    "FEDFUNDS",
    "DFF",
    "DGS10",
    "DGS2",
    "DGS3MO",
    "DTB3",
    "T10Y2Y",
    "T10Y3M",
    "WALCL",
    "M2SL",
    "M1SL",
    "BOGMBASE",
    "INDPRO",
    "TCU",
    "TOTALSL",
    "DEXUSEU",
    "DEXJPUS",
    "DEXCHUS",
    # U.S. Bureau of Labor Statistics
    "CPIAUCSL",
    "CPILFESL",
    "UNRATE",
    "U6RATE",
    "CIVPART",
    "EMRATIO",
    "PAYEMS",
    "MANEMP",
    "CES0500000003",
    "JTSJOL",
    "PPIACO",
    # U.S. Bureau of Economic Analysis
    "GDP",
    "GDPC1",
    "A191RL1Q225SBEA",
    "PCE",
    "PCEPI",
    "PCEPILFE",
    "DSPIC96",
    "PSAVERT",
    "CP",
    # U.S. Census Bureau
    "HOUST",
    "PERMIT",
    "RSAFS",
    "DGORDER",
    "TTLCONS",
    "BUSINV",
    # U.S. Treasury / Fiscal Service
    "GFDEBTN",
    "GFDEGDQ188S",
    "MTSDS133FMS",
    "FYFSD",
    # U.S. Employment and Training Administration (DOL)
    "ICSA",
    # Federal Reserve Bank of St. Louis (derived, public)
    "USREC",
]


# --------------------------------------------------------------------------- #
# API client
# --------------------------------------------------------------------------- #
def get_api_key() -> str:
    key = os.environ.get("FRED_API_KEY", "").strip()
    if not key:
        env_file = DATA_ROOT / ".env"
        if env_file.exists():
            for line in env_file.read_text().splitlines():
                if line.strip().startswith("FRED_API_KEY"):
                    key = line.split("=", 1)[1].strip().strip("'\"")
                    break
    if not key:
        raise SystemExit(
            "FRED_API_KEY not set. Export it in the environment "
            "(export FRED_API_KEY=...) or put it in "
            f"{DATA_ROOT / '.env'} (gitignored). Free key: "
            "https://fredaccount.stlouisfed.org/apikeys"
        )
    return key


_SESSION = requests.Session()
_LAST_CALL = [0.0]
_MIN_INTERVAL = 0.6  # ~100 req/min, safely under the 120/min FRED limit


def fred_get(endpoint: str, api_key: str, **params) -> dict:
    params.update({"api_key": api_key, "file_type": "json"})
    for attempt in range(5):
        wait = _MIN_INTERVAL - (time.time() - _LAST_CALL[0])
        if wait > 0:
            time.sleep(wait)
        _LAST_CALL[0] = time.time()
        r = _SESSION.get(f"{BASE}/{endpoint}", params=params, timeout=60)
        if r.status_code == 429:
            time.sleep(5 * (attempt + 1))
            continue
        r.raise_for_status()
        return r.json()
    raise RuntimeError(f"FRED API repeatedly rate-limited on {endpoint}")


# --------------------------------------------------------------------------- #
# Metadata + license resolution
# --------------------------------------------------------------------------- #
_RELEASE_SOURCE_CACHE: dict[str, str] = {}


def resolve_source_name(series_id: str, api_key: str) -> tuple[str, str]:
    """Return (release_name, source_name) via series -> release -> sources."""
    rel = fred_get("series/release", api_key, series_id=series_id)
    releases = rel.get("releases", [])
    if not releases:
        return "", ""
    release = releases[0]
    release_id = str(release["id"])
    release_name = release.get("name", "")
    if release_id not in _RELEASE_SOURCE_CACHE:
        src = fred_get("release/sources", api_key, release_id=release_id)
        names = [s.get("name", "") for s in src.get("sources", [])]
        _RELEASE_SOURCE_CACHE[release_id] = names[0] if names else ""
    return release_name, _RELEASE_SOURCE_CACHE[release_id]


def fetch_series_meta(series_id: str, api_key: str) -> dict | None:
    resp = fred_get("series", api_key, series_id=series_id)
    seriess = resp.get("seriess", [])
    return seriess[0] if seriess else None


def is_copyrighted(notes: str) -> bool:
    return "copyright" in (notes or "").lower()


# --------------------------------------------------------------------------- #
# Observations
# --------------------------------------------------------------------------- #
def fetch_observations(
    series_id: str, api_key: str
) -> list[tuple[int, str, str, float]]:
    """Return list of (year, date, series_id, value) for the latest revision."""
    resp = fred_get(
        "series/observations", api_key, series_id=series_id, sort_order="asc"
    )
    rows = []
    for o in resp.get("observations", []):
        raw = o.get("value", ".")
        if raw in (".", "", None):
            continue  # FRED missing marker -> NULL, dropped from the long table
        try:
            val = float(raw)
        except ValueError:
            continue
        date = o["date"]
        rows.append((int(date[:4]), date, series_id, val))
    return rows


# --------------------------------------------------------------------------- #
# Parquet writers — all-STRING staging (us_bls_cpi convention).
# Values pass through the architecture's REAL types first (so year serializes as
# "1959" not "1959.0" and dates as "1959-01-01"), then cast to string via arrow
# — never astype(str), which would render NULL as the literal "nan" and defeat
# the dbt safe_cast. The dbt model safe_casts every column back to its real type.
# --------------------------------------------------------------------------- #
OBS_TYPED = pa.schema(
    [
        ("year", pa.int64()),
        ("date", pa.date32()),
        ("series_id", pa.string()),
        ("value", pa.float64()),
    ]
)
OBS_STRING = pa.schema([(f.name, pa.string()) for f in OBS_TYPED])

SERIES_COLS = [
    "series_id",
    "title",
    "units",
    "units_short",
    "frequency",
    "frequency_short",
    "seasonal_adjustment",
    "seasonal_adjustment_short",
    "observation_start",
    "observation_end",
    "last_updated",
    "source_name",
    "release_name",
    "notes",
]
SERIES_SCHEMA = pa.schema([(c, pa.string()) for c in SERIES_COLS])


def write_observations(rows: list[tuple], out_dir: Path) -> int:
    import datetime as dt

    by_year: dict[int, list[tuple]] = {}
    for year, date, sid, val in rows:
        by_year.setdefault(year, []).append((date, sid, val))
    n = 0
    for year, recs in sorted(by_year.items()):
        dates = [dt.date.fromisoformat(d) for d, _, _ in recs]
        sids = [s for _, s, _ in recs]
        vals = [v for _, _, v in recs]
        typed = pa.table(
            {
                "year": pa.array([year] * len(recs), pa.int64()),
                "date": pa.array(dates, pa.date32()),
                "series_id": pa.array(sids, pa.string()),
                "value": pa.array(vals, pa.float64()),
            },
            schema=OBS_TYPED,
        )
        tbl = typed.cast(OBS_STRING)  # all-STRING staging, NULLs preserved
        pdir = out_dir / "observation" / f"year={year}"
        pdir.mkdir(parents=True, exist_ok=True)
        pq.write_table(tbl, pdir / "data.parquet", compression="snappy")
        n += len(recs)
    return n


def write_series(catalog: list[dict], out_dir: Path) -> int:
    cols = SERIES_SCHEMA.names
    arrays = {
        c: pa.array([r.get(c, "") or "" for r in catalog], pa.string())
        for c in cols
    }
    tbl = pa.table(arrays, schema=SERIES_SCHEMA)
    sdir = out_dir / "series"
    sdir.mkdir(parents=True, exist_ok=True)
    pq.write_table(tbl, sdir / "data.parquet", compression="snappy")
    return len(catalog)


# --------------------------------------------------------------------------- #
# Orchestration
# --------------------------------------------------------------------------- #
def run(limit: int | None = None) -> None:
    api_key = get_api_key()
    out_dir = DATA_ROOT / "output"
    out_dir.mkdir(parents=True, exist_ok=True)

    kept_catalog: list[dict] = []
    all_obs: list[tuple] = []
    excluded: list[dict] = []
    series_ids = SEED_SERIES[:limit] if limit else SEED_SERIES

    for i, sid in enumerate(series_ids, 1):
        meta = fetch_series_meta(sid, api_key)
        if meta is None:
            excluded.append(
                {"series_id": sid, "source_name": "", "reason": "not found"}
            )
            print(f"[{i}/{len(series_ids)}] {sid}: NOT FOUND — skipped")
            continue
        notes = meta.get("notes", "") or ""
        release_name, source_name = resolve_source_name(sid, api_key)

        if is_copyrighted(notes):
            excluded.append(
                {
                    "series_id": sid,
                    "source_name": source_name,
                    "reason": "copyright in notes",
                }
            )
            print(
                f"[{i}/{len(series_ids)}] {sid}: EXCLUDED (copyright in notes)"
            )
            continue
        if source_name and source_name not in SOURCE_ALLOWLIST:
            excluded.append(
                {
                    "series_id": sid,
                    "source_name": source_name,
                    "reason": "source not in allowlist",
                }
            )
            print(
                f"[{i}/{len(series_ids)}] {sid}: EXCLUDED (source: {source_name})"
            )
            continue

        obs = fetch_observations(sid, api_key)
        all_obs.extend(obs)
        kept_catalog.append(
            {
                "series_id": sid,
                "title": meta.get("title", ""),
                "units": meta.get("units", ""),
                "units_short": meta.get("units_short", ""),
                "frequency": meta.get("frequency", ""),
                "frequency_short": meta.get("frequency_short", ""),
                "seasonal_adjustment": meta.get("seasonal_adjustment", ""),
                "seasonal_adjustment_short": meta.get(
                    "seasonal_adjustment_short", ""
                ),
                "observation_start": meta.get("observation_start", ""),
                "observation_end": meta.get("observation_end", ""),
                "last_updated": meta.get("last_updated", ""),
                "source_name": source_name,
                "release_name": release_name,
                "notes": notes,
            }
        )
        print(
            f"[{i}/{len(series_ids)}] {sid}: {len(obs):>7} obs  ({source_name})"
        )

    n_obs = write_observations(all_obs, out_dir)
    n_ser = write_series(kept_catalog, out_dir)

    excl_path = CODE_DIR / "excluded_series.csv"
    with excl_path.open("w", newline="") as f:
        w = csv.DictWriter(
            f, fieldnames=["series_id", "source_name", "reason"]
        )
        w.writeheader()
        w.writerows(excluded)

    print("\n=== SUMMARY ===")
    print(f"series kept   : {n_ser}")
    print(f"observations  : {n_obs:,}")
    print(f"series excluded: {len(excluded)}  (see {excl_path})")
    print(f"output        : {out_dir}")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument(
        "--limit",
        type=int,
        default=None,
        help="only process the first N seed series (smoke test)",
    )
    args = ap.parse_args()
    run(limit=args.limit)
