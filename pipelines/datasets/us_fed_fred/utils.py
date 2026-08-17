"""Download + cleaning transform for us_fed_fred (shared by the pipeline and the
one-shot bootstrap in ``models/us_fed_fred/code/``).

Pure functions (no Prefect) so they are importable and unit-testable. The
recurring pipeline wraps them in ``@task`` (see ``tasks.py``); the bootstrap CLI
imports ``download_all``/``clean_all`` directly. Schema/column order come from the
architecture CSVs (the single source of truth).

The work splits in two:

  ``download_all(input_dir)``  fetch each seed series' metadata + observations,
                               apply the public-domain license gate, and persist
                               the kept series as raw JSON under ``input_dir``.
  ``clean_all(input_dir, out)``  read that raw JSON and write the two tables as
                               all-STRING partitioned parquet under ``out``.

Splitting them lets ``clean_all`` be re-run (and transform-parity-tested) against
a cached ``input/`` without re-hitting the FRED API.

License gate (both applied at download):
  1. Source allowlist — keep only U.S.-federal-agency sources (public domain).
  2. "Copyright" in ``/series`` notes — FRED's own marker for restricted series.
Every dropped series is logged to ``input_dir/_excluded.csv``.

Credential: the FRED API key comes from ``FRED_API_KEY`` in the environment, a
gitignored ``.env`` at the scratch dir, or — on the deployed worker — the Vault
secret named by ``constants.VAULT_SECRET_PATH``. Never hard-coded, never committed.
"""

from __future__ import annotations

import csv
import datetime as dt
import json
import logging
import os
import time
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq
import requests

from pipelines.datasets.us_fed_fred.constants import constants

log = logging.getLogger("us_fed_fred")

BASE = constants.BASE_URL.value
MIN_INTERVAL = constants.MIN_INTERVAL.value
SEED_SERIES = constants.SEED_SERIES.value
SOURCE_ALLOWLIST = set(constants.SOURCE_ALLOWLIST.value)
SERIES_COLS = constants.SERIES_COLS.value
_ARCH = constants.ARCHITECTURE_DIR.value

# BigQuery type -> pyarrow type, for building the typed staging schema.
PA = {
    "STRING": pa.string(),
    "INT64": pa.int64(),
    "FLOAT64": pa.float64(),
    "DATE": pa.date32(),
}


# ── schema (architecture CSVs are the source of truth) ───────────────────────
def read_arch(table: str) -> list[dict]:
    """Read a table's architecture CSV — the schema source of truth.

    Column order and BigQuery types come from here, never from the API response,
    so the pipeline and the one-shot bootstrap cannot drift apart.

    Args:
        table: Table slug (``"observation"`` or ``"series"``), matching the CSV
            filename.

    Returns:
        One dict per column, in architecture order.
    """
    with open(_ARCH / f"{table}.csv", newline="") as fh:
        return list(csv.DictReader(fh))


def _typed_schema(table: str) -> pa.Schema:
    """Architecture-typed pyarrow schema for a table."""
    return pa.schema(
        [pa.field(a["name"], PA[a["bigquery_type"]]) for a in read_arch(table)]
    )


def _string_schema(table: str) -> pa.Schema:
    """All-STRING pyarrow schema for a table (staging convention)."""
    return pa.schema(
        [pa.field(a["name"], pa.string()) for a in read_arch(table)]
    )


# ── credential ───────────────────────────────────────────────────────────────
def get_api_key() -> str:
    """Return the FRED API key from the environment, scratch ``.env``, or Vault.

    Resolution order: ``FRED_API_KEY`` in the environment (local/bootstrap runs),
    then a gitignored ``.env`` at the scratch dir, then — on the deployed worker —
    the Vault secret named by ``constants.VAULT_SECRET_PATH``. The Vault import is
    lazy so local runs need no ``VAULT_ADDRESS``/``VAULT_TOKEN``.

    Returns:
        The FRED API key.

    Raises:
        RuntimeError: If no key is found in any source.
    """
    key = os.environ.get("FRED_API_KEY", "").strip()
    if key:
        return key

    scratch = Path(
        os.environ.get(
            "US_FED_FRED_DATA", Path.home() / "Downloads" / "us_fed_fred_data"
        )
    )
    env_file = scratch / ".env"
    if env_file.exists():
        for line in env_file.read_text().splitlines():
            if line.strip().startswith("FRED_API_KEY"):
                return line.split("=", 1)[1].strip().strip("'\"")

    try:  # deployed worker: read from Vault
        from pipelines.utils.vault import get_credentials_from_secret

        secret = get_credentials_from_secret(constants.VAULT_SECRET_PATH.value)
        key = str(secret.get(constants.VAULT_SECRET_KEY.value, "")).strip()
    except Exception as e:
        log.warning(f"Vault lookup for the FRED key failed: {e}")
        key = ""

    if not key:
        raise RuntimeError(
            "FRED_API_KEY not found. Set it in the environment, in the scratch "
            f"{env_file}, or in the Vault secret "
            f"'{constants.VAULT_SECRET_PATH.value}'. Free key: "
            "https://fredaccount.stlouisfed.org/apikeys"
        )
    return key


# ── API client ───────────────────────────────────────────────────────────────
_SESSION = requests.Session()
_LAST_CALL = [0.0]


def fred_get(
    endpoint: str, api_key: str, **params: str | int
) -> dict[str, Any]:
    """Call a FRED REST endpoint, rate-limited, and return parsed JSON.

    Sleeps to stay under the 120 requests/minute limit and retries on HTTP 429
    with linear backoff.

    Args:
        endpoint: Path under the FRED base URL (e.g. ``"series"``).
        api_key: The FRED API key.
        **params: Extra query parameters (e.g. ``series_id``, ``release_id``).

    Returns:
        The decoded JSON response body.

    Raises:
        RuntimeError: If the endpoint stays rate-limited after retries.
    """
    params.update({"api_key": api_key, "file_type": "json"})
    for attempt in range(5):
        wait = MIN_INTERVAL - (time.time() - _LAST_CALL[0])
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


# ── metadata + license resolution ────────────────────────────────────────────
_RELEASE_SOURCE_CACHE: dict[str, str] = {}


def resolve_source_name(series_id: str, api_key: str) -> tuple[str, str]:
    """Return ``(release_name, source_name)`` via series -> release -> sources.

    Args:
        series_id: The FRED series identifier.
        api_key: The FRED API key.

    Returns:
        The release name and the originating source name (both ``""`` if
        unresolved).
    """
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


def fetch_series_meta(series_id: str, api_key: str) -> dict[str, Any] | None:
    """Return the ``/series`` metadata object for a series, or ``None``.

    Args:
        series_id: The FRED series identifier.
        api_key: The FRED API key.

    Returns:
        The series metadata dict, or ``None`` if the series does not exist.
    """
    resp = fred_get("series", api_key, series_id=series_id)
    seriess = resp.get("seriess", [])
    return seriess[0] if seriess else None


def is_copyrighted(notes: str) -> bool:
    """Return whether the series notes flag it as copyrighted.

    FRED marks restricted third-party series with the word "Copyright" in their
    notes; such series are excluded from this public-domain dataset.

    Args:
        notes: The series ``notes`` text.

    Returns:
        True if the notes contain "copyright" (case-insensitive).
    """
    return "copyright" in (notes or "").lower()


def fetch_observations(
    series_id: str, api_key: str
) -> list[tuple[str, float]]:
    """Return ``(date, value)`` pairs for a series' latest revision.

    FRED's missing marker ``.`` (and blank/unparseable values) are dropped so the
    long table carries only real observations.

    Args:
        series_id: The FRED series identifier.
        api_key: The FRED API key.

    Returns:
        Observations as ``(date, value)`` pairs, ascending by date.
    """
    resp = fred_get(
        "series/observations", api_key, series_id=series_id, sort_order="asc"
    )
    rows: list[tuple[str, float]] = []
    for o in resp.get("observations", []):
        raw = o.get("value", ".")
        if raw in (".", "", None):
            continue
        try:
            val = float(raw)
        except ValueError:
            continue
        rows.append((o["date"], val))
    return rows


# ── download (fetch + license gate -> raw JSON) ──────────────────────────────
def download_all(
    input_dir: Path, limit: int | None = None, api_key: str | None = None
) -> Path:
    """Fetch the seed series, apply the license gate, and persist raw JSON.

    For each seed series, resolves its source, drops it if copyrighted or outside
    the public-domain source allowlist (logging the reason), and writes each kept
    series to ``input_dir/<series_id>.json`` as ``{"meta": {...}, "observations":
    [[date, value], ...]}``. The kept order and the exclusion log are written to
    ``_kept.json`` and ``_excluded.csv``.

    Args:
        input_dir: Directory to write raw JSON into; created if absent.
        limit: If given, only process the first ``limit`` seed series (smoke test).
        api_key: FRED API key; resolved via :func:`get_api_key` when ``None``.

    Returns:
        The same ``input_dir``, for chaining.
    """
    api_key = api_key or get_api_key()
    input_dir.mkdir(parents=True, exist_ok=True)
    series_ids = SEED_SERIES[:limit] if limit else SEED_SERIES

    kept: list[str] = []
    excluded: list[dict[str, str]] = []
    for i, sid in enumerate(series_ids, 1):
        meta = fetch_series_meta(sid, api_key)
        if meta is None:
            excluded.append(
                {"series_id": sid, "source_name": "", "reason": "not found"}
            )
            log.info(f"[{i}/{len(series_ids)}] {sid}: NOT FOUND — skipped")
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
            log.info(f"[{i}/{len(series_ids)}] {sid}: EXCLUDED (copyright)")
            continue
        # Reject an unresolved (empty) source too: it was never verified against
        # the public-domain allowlist, so it must not be published.
        if source_name not in SOURCE_ALLOWLIST:
            excluded.append(
                {
                    "series_id": sid,
                    "source_name": source_name,
                    "reason": "source missing or not in allowlist",
                }
            )
            shown = source_name or "(unresolved)"
            log.info(
                f"[{i}/{len(series_ids)}] {sid}: EXCLUDED (source: {shown})"
            )
            continue

        obs = fetch_observations(sid, api_key)
        record = {
            "meta": {
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
            },
            "observations": obs,
        }
        (input_dir / f"{sid}.json").write_text(json.dumps(record))
        kept.append(sid)
        log.info(
            f"[{i}/{len(series_ids)}] {sid}: {len(obs):>7} obs ({source_name})"
        )

    (input_dir / "_kept.json").write_text(json.dumps(kept))
    with (input_dir / "_excluded.csv").open("w", newline="") as f:
        w = csv.DictWriter(
            f, fieldnames=["series_id", "source_name", "reason"]
        )
        w.writeheader()
        w.writerows(excluded)
    log.info(
        f"download: {len(kept)} kept, {len(excluded)} excluded -> {input_dir}"
    )
    return input_dir


# ── clean (raw JSON -> all-STRING partitioned parquet) ───────────────────────
def _write_observation(
    kept: list[str], input_dir: Path, out_dir: Path
) -> tuple[int, str | None]:
    """Write the long observations as year-partitioned all-STRING parquet.

    Values pass through the architecture's real types first (so ``year``
    serializes as ``"1959"`` not ``"1959.0"`` and ``date`` as ``"1959-01-01"``),
    then cast to string via arrow — never ``astype(str)``, which would render a
    NULL as the literal ``"nan"`` and defeat the dbt ``safe_cast``.

    Args:
        kept: Ordered kept series ids.
        input_dir: Directory of raw per-series JSON.
        out_dir: Output root; files land at
            ``out_dir/observation/year=<YYYY>/data.parquet``.

    Returns:
        ``(row_count, max_date)`` where ``max_date`` is the latest ``YYYY-MM-DD``
        observation date across all series (``None`` if there are no rows).
    """
    typed, string = _typed_schema("observation"), _string_schema("observation")
    by_year: dict[int, list[tuple[str, str, float]]] = {}
    max_date: str | None = None
    for sid in kept:
        record = json.loads((input_dir / f"{sid}.json").read_text())
        for date, val in record["observations"]:
            by_year.setdefault(int(date[:4]), []).append((date, sid, val))
            if max_date is None or date > max_date:
                max_date = date

    n = 0
    tdir = out_dir / "observation"
    for year, recs in sorted(by_year.items()):
        dates = [dt.date.fromisoformat(d) for d, _, _ in recs]
        table = pa.table(
            {
                "year": pa.array([year] * len(recs), pa.int64()),
                "date": pa.array(dates, pa.date32()),
                "series_id": pa.array([s for _, s, _ in recs], pa.string()),
                "value": pa.array([v for _, _, v in recs], pa.float64()),
            },
            schema=typed,
        ).cast(string)
        pdir = tdir / f"year={year}"
        pdir.mkdir(parents=True, exist_ok=True)
        pq.write_table(table, pdir / "data.parquet", compression="snappy")
        n += len(recs)
    log.info(f"observation: {n:,} rows -> {tdir}")
    return n, max_date


def _write_series(kept: list[str], input_dir: Path, out_dir: Path) -> int:
    """Write the series metadata catalog as a single all-STRING parquet.

    Args:
        kept: Ordered kept series ids.
        input_dir: Directory of raw per-series JSON.
        out_dir: Output root; the file lands at ``out_dir/series/data.parquet``.

    Returns:
        The number of series written.
    """
    metas = [
        json.loads((input_dir / f"{sid}.json").read_text())["meta"]
        for sid in kept
    ]
    arrays = {
        c: pa.array([str(m.get(c, "") or "") for m in metas], pa.string())
        for c in SERIES_COLS
    }
    table = pa.table(arrays, schema=_string_schema("series"))
    sdir = out_dir / "series"
    sdir.mkdir(parents=True, exist_ok=True)
    pq.write_table(table, sdir / "data.parquet", compression="snappy")
    log.info(f"series: {len(metas):,} rows -> {sdir}")
    return len(metas)


def clean_all(input_dir: Path, output_dir: Path) -> dict:
    """Build both tables from the raw JSON downloaded by :func:`download_all`.

    The single entry point shared by the recurring pipeline (via
    :func:`pipelines.datasets.us_fed_fred.tasks.clean_fred`) and the one-shot
    bootstrap in ``models/us_fed_fred/code/``.

    Args:
        input_dir: Directory of raw per-series JSON (from :func:`download_all`).
        output_dir: Root output directory.

    Returns:
        Mapping of table slug to output directory, plus ``"max_date"`` — the
        latest ``YYYY-MM-DD`` observation date, used to poll whether the source
        has newer data — and the row counts ``"n_observation"``/``"n_series"``.
    """
    output_dir.mkdir(parents=True, exist_ok=True)
    kept = json.loads((input_dir / "_kept.json").read_text())
    n_obs, max_date = _write_observation(kept, input_dir, output_dir)
    n_ser = _write_series(kept, input_dir, output_dir)
    return {
        "observation": output_dir / "observation",
        "series": output_dir / "series",
        "max_date": max_date,
        "n_observation": n_obs,
        "n_series": n_ser,
    }
