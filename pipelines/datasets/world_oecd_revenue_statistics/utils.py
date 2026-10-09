"""Download + cleaning transform for world_oecd_revenue_statistics.

Pure functions (no Prefect) so the one-shot onboarding bootstrap
(``models/world_oecd_revenue_statistics/code/``) and the recurring Prefect
pipeline share one implementation. Schema/column order come from the architecture
CSVs (the single source of truth).

The design is WIDE: one row per country x year x government level x tax category,
with the OECD unit dimension pivoted into five value columns (``pct_gdp``,
``pct_institutional_sector``, ``pct_revenue_category``, ``value_national_currency``,
``value_usd``). ``MEASURE`` (always ``TAX_REV``) and ``CTRY_SPECIFIC_REVENUE``
(always ``_T``) are constant in the comparative cube and drop out; a runtime
assertion fails loudly if that ever stops holding.
"""

import csv
import logging
import re
import time
import urllib.error
import urllib.request
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from pipelines.datasets.world_oecd_revenue_statistics.constants import (
    constants,
)

log = logging.getLogger("world_oecd_revenue_statistics")

FLOW_REF = constants.FLOW_REF.value
SDMX = constants.SDMX_BASE.value
UA = constants.USER_AGENT.value
AGENCY = constants.AGENCY.value
DSD = constants.DSD.value
UNIT_TO_COLUMN = constants.UNIT_TO_COLUMN.value
ABSOLUTE_UNITS = constants.ABSOLUTE_UNITS.value
ARCH_DIR = constants.ARCHITECTURE_DIR.value

# key columns of the wide grain (raw SDMX names)
_GRAIN = ["REF_AREA", "TIME_PERIOD", "SECTOR", "STANDARD_REVENUE"]


# ── HTTP (adaptive throttle: the OECD limiter is volume+frequency based) ──────
_state = {"gap": 20.0, "last": 0.0}
_GAP_STEP, _GAP_MAX, _BACKOFF_BASE, _MAX_RETRIES = 8.0, 90.0, 60, 12


def get(url, timeout=600):
    """GET the OECD API, throttled; never returns a non-200 body as data.

    Every 429 permanently widens the inter-request gap for the rest of the run so
    it settles near what the server will serve. Returns the text body on 200,
    ``None`` on a genuine empty result, raises after ``_MAX_RETRIES``.
    """
    for attempt in range(_MAX_RETRIES):
        wait = _state["gap"] - (time.monotonic() - _state["last"])
        if wait > 0:
            time.sleep(wait)
        req = urllib.request.Request(url, headers={"User-Agent": UA})
        try:
            with urllib.request.urlopen(req, timeout=timeout) as r:
                _state["last"] = time.monotonic()
                return r.read().decode("utf-8", "replace")
        except urllib.error.HTTPError as e:
            _state["last"] = time.monotonic()
            body = e.read().decode("utf-8", "replace")
            if e.code == 404 and body.strip().startswith(
                ("No Results", "Could not")
            ):
                return None
            # A 403 whose body is a Cloudflare "Just a moment" interstitial means the
            # IP is being challenged for heavy/bot-like use (curl/urllib cannot pass a
            # JS challenge). Back off long and gently so a transient challenge clears on
            # its own; do not hammer it (that extends the block).
            if e.code == 403 and "Just a moment" in body:
                _state["gap"] = min(_state["gap"] + 2 * _GAP_STEP, _GAP_MAX)
                back = 300
                log.warning(
                    "403 Cloudflare challenge; long backoff %ss; gap now %.0fs (try %d)",
                    back,
                    _state["gap"],
                    attempt + 1,
                )
                time.sleep(back)
                continue
            if e.code == 429 or e.code >= 500:
                if e.code == 429:
                    _state["gap"] = min(_state["gap"] + _GAP_STEP, _GAP_MAX)
                back = _BACKOFF_BASE * (2 ** min(attempt, 3))
                log.warning(
                    "%s from OECD; backoff %ss; gap now %.0fs (try %d)",
                    e.code,
                    back,
                    _state["gap"],
                    attempt + 1,
                )
                time.sleep(back)
                continue
            raise
        except Exception as e:
            _state["last"] = time.monotonic()
            log.warning("error %s; backoff 120s (try %d)", e, attempt + 1)
            time.sleep(120)
    raise RuntimeError(f"still failing after {_MAX_RETRIES} attempts: {url}")


# ── structure (codelists: hierarchy + labels) ────────────────────────────────
def fetch_structure(cache_path: Path) -> str:
    """Download the DSD with its codelists (cached)."""
    cache_path = Path(cache_path)
    if cache_path.exists() and cache_path.stat().st_size > 1000:
        return cache_path.read_text(encoding="utf-8")
    url = f"{SDMX}/datastructure/{AGENCY}/{DSD}/latest?references=children"
    xml = get(url)
    if xml is None:
        raise RuntimeError(f"empty DSD response from {url}")
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    cache_path.write_text(xml, encoding="utf-8")
    return xml


def _parse_codelist(xml: str, clid: str):
    """{code: (parent, english_label)} for one codelist."""
    m = re.search(
        rf'<structure:Codelist [^>]*id="{re.escape(clid)}".*?</structure:Codelist>',
        xml,
        re.S,
    )
    if not m:
        return {}
    block = m.group(0)
    out = {}
    for cm in re.finditer(
        r'<structure:Code id="([^"]*)">(.*?)</structure:Code>', block, re.S
    ):
        cid, body = cm.group(1), cm.group(2)
        nm = re.search(
            r'<common:Name xml:lang="en">([^<]*)</common:Name>', body
        )
        par = re.search(r"<structure:Parent>\s*<Ref id=\"([^\"]*)\"", body)
        out[cid] = (par.group(1) if par else "", nm.group(1) if nm else "")
    return out


def parse_codelists(xml: str) -> dict:
    """All codelists needed downstream, as {name: {code: (parent, label)}}."""
    return {
        "standard_revenue": _parse_codelist(
            xml, constants.CL_STANDARD_REVENUE.value
        ),
        "sector": _parse_codelist(xml, constants.CL_SECTOR.value),
        "obs_status": _parse_codelist(xml, constants.CL_OBS_STATUS.value),
    }


# ── architecture ─────────────────────────────────────────────────────────────
def read_arch(slug: str):
    """Architecture rows (list of dicts) in column order."""
    with (ARCH_DIR / f"{slug}.csv").open(encoding="utf-8") as f:
        return list(csv.DictReader(f))


def _iso3_set() -> set:
    p = (
        Path(__file__).resolve().parents[3]
        / "models"
        / "world_oecd_revenue_statistics"
        / "code"
        / "country_iso3.csv"
    )
    with p.open(encoding="utf-8") as f:
        return {r["sigla_iso3"] for r in csv.DictReader(f)}


# ── download (chunked by area; resume-safe) ──────────────────────────────────
def download_all(input_dir: Path, version: str | None = None) -> Path:
    """Download the whole comparative cube, one codes-only CSV per REF_AREA.

    Resume-safe. NOTE: the OECD host Cloudflare-challenges heavy automated pulls
    (see the module docstring / project memory), so this can be blocked on a
    Prefect worker; the on-demand flow accepts a pre-staged ``input_dir`` for that
    case. Returns ``input_dir``.
    """
    import io

    version = version or constants.DEFAULT_VERSION.value
    base = f"{SDMX}/data/{FLOW_REF},{version}"
    input_dir = Path(input_dir)
    input_dir.mkdir(parents=True, exist_ok=True)

    cache = input_dir / "_areas.txt"
    if cache.exists():
        areas = [a for a in cache.read_text().split() if a]
    else:
        body = get(
            f"{base}/......?format=csvfile&startPeriod=2021&endPeriod=2021"
        )
        if body is None:
            raise RuntimeError("could not fetch area universe")
        areas = sorted(
            {r["REF_AREA"] for r in csv.DictReader(io.StringIO(body))}
        )
        cache.write_text("\n".join(areas))

    for a in areas:
        dst = input_dir / f"{a}.csv"
        if dst.exists() and dst.stat().st_size > 0:
            continue
        body = get(f"{base}/{a}......?format=csvfile")
        if body and not body.startswith(("Could not", "No Results")):
            dst.write_text(body)
    return input_dir


# ── transform (wide pivot) ───────────────────────────────────────────────────
def build_revenue(
    input_dir: Path, codelists: dict, version: str
) -> pd.DataFrame:
    """Read every downloaded per-area CSV and pivot units into value columns.

    Returns the full wide table (all countries). Rows with no ``OBS_VALUE`` are
    dropped (the API returns the full cross-product, mostly empty).
    """
    input_dir = Path(input_dir)
    files = sorted(
        f for f in input_dir.glob("*.csv") if not f.name.startswith("_")
    )
    if not files:
        raise FileNotFoundError(f"no downloaded area CSVs in {input_dir}")
    iso3 = _iso3_set()
    std = codelists["standard_revenue"]
    frames = []
    for path in files:
        df = pd.read_csv(path, dtype=str, low_memory=False)
        if df.empty:
            continue
        df = df[df["OBS_VALUE"].notna() & (df["OBS_VALUE"].str.strip() != "")]
        if df.empty:
            continue
        frames.append(_pivot_area(df))
    wide = pd.concat(frames, ignore_index=True)

    # hierarchy + derived columns. Top-level categories (e.g. _T) have no parent in
    # the codelist; map the empty parent to NULL, not "" (an empty string is a value
    # that would fail dictionary coverage and misrepresent "no parent").
    wide["tax_category_parent_id"] = (
        wide["tax_category_id"]
        .map(lambda c: std.get(c, ("", ""))[0])
        .replace("", pd.NA)
    )
    wide["country_iso3_code"] = wide["reference_area"].where(
        wide["reference_area"].isin(iso3)
    )
    wide["source_flow_version"] = version

    order = [a["name"] for a in read_arch("revenue")]
    for c in order:
        if c not in wide.columns:
            wide[c] = pd.NA
    wide = wide[order]
    log.info("revenue: %d wide rows from %d areas", len(wide), len(files))
    return wide


def _pivot_area(df: pd.DataFrame) -> pd.DataFrame:
    """Pivot one area's long rows into the wide grain.

    Asserts the grain is unique per unit — i.e. that MEASURE and
    CTRY_SPECIFIC_REVENUE really are constant here. If OECD ever varies them, this
    raises rather than silently collapsing two cells.
    """
    d = df.copy()
    d["year"] = pd.to_numeric(d["TIME_PERIOD"], errors="coerce").astype(
        "Int64"
    )
    val = pd.to_numeric(d["OBS_VALUE"], errors="coerce")
    mult = pd.to_numeric(d["UNIT_MULT"], errors="coerce").fillna(0)

    dup = d.duplicated(subset=[*_GRAIN, "UNIT_MEASURE"], keep=False)
    if dup.any():
        ex = d[dup][
            [*_GRAIN, "UNIT_MEASURE", "MEASURE", "CTRY_SPECIFIC_REVENUE"]
        ].head(4)
        raise AssertionError(
            "grain collision (MEASURE/CTRY_SPECIFIC_REVENUE not constant):\n"
            f"{ex.to_string(index=False)}"
        )

    d["_absval"] = val * (10.0**mult)
    rows = {}
    for unit, col in UNIT_TO_COLUMN.items():
        sel = d["UNIT_MEASURE"] == unit
        src = d["_absval"] if unit in ABSOLUTE_UNITS else val
        rows[col] = (
            d.loc[sel, _GRAIN].assign(**{col: src[sel]}).set_index(_GRAIN)[col]
        )
    wide = pd.DataFrame(rows).reset_index()

    # per-cell attributes: currency from the XDC row; status/code/year from any
    attrs = (
        d.assign(
            _cur=d["CURRENCY"].where(d["UNIT_MEASURE"] == "XDC"),
        )
        .groupby(_GRAIN, dropna=False)
        .agg(
            currency=(
                "_cur",
                lambda s: s.dropna().iloc[0] if s.notna().any() else pd.NA,
            ),
            observation_status=(
                "OBS_STATUS",
                lambda s: s.dropna().iloc[0] if s.notna().any() else pd.NA,
            ),
            tax_category_code=(
                "REVENUE_CODE",
                lambda s: s.dropna().iloc[0] if s.notna().any() else pd.NA,
            ),
            year=("year", "first"),
        )
        .reset_index()
    )
    out = wide.merge(attrs, on=_GRAIN, how="left")
    out = out.rename(
        columns={
            "REF_AREA": "reference_area",
            "SECTOR": "government_level_id",
            "STANDARD_REVENUE": "tax_category_id",
        }
    ).drop(columns=["TIME_PERIOD"])
    # normalize sentinel currency "_Z" (means not-applicable) to null
    out["currency"] = out["currency"].where(out["currency"].ne("_Z"))
    return out


# ── parquet (all-STRING, hive-partitioned by year) ───────────────────────────
def _to_string_table(df: pd.DataFrame, order) -> pa.Table:
    """All-STRING arrow table; cast through arrow so NULL stays NULL and INT64
    serializes as "2013" not "2013.0"."""
    arrays = []
    for name in order:
        col = df[name]
        if str(col.dtype).startswith(("Int", "int", "float", "Float")):
            arrays.append(pa.array(col, from_pandas=True).cast(pa.string()))
        else:
            arrays.append(
                pa.array(
                    col.astype(object), type=pa.string(), from_pandas=True
                )
            )
    return pa.Table.from_arrays(arrays, names=list(order))


def write_partitioned(df: pd.DataFrame, output_dir: Path) -> Path:
    """Write revenue as all-STRING Snappy parquet, one file per year."""
    order = [a["name"] for a in read_arch("revenue")]
    tdir = Path(output_dir) / "revenue"
    total = 0
    for year, g in df.dropna(subset=["year"]).groupby("year", sort=True):
        # pyrefly: ignore [bad-argument-type]
        pdir = tdir / f"year={int(year)}"
        pdir.mkdir(parents=True, exist_ok=True)
        pq.write_table(
            _to_string_table(g, order),
            pdir / "data.parquet",
            compression="snappy",
        )
        total += len(g)
    log.info("revenue: %d rows -> %s", total, tdir)
    return tdir


# ── dictionary (derived from observed codes) ─────────────────────────────────
def build_dicionario(
    df: pd.DataFrame, codelists: dict, output_dir: Path
) -> Path:
    """One dictionary row per observed coded value, labelled from the codelists."""
    std = codelists["standard_revenue"]
    sector = codelists["sector"]
    obs = codelists["obs_status"]
    label = {
        "government_level_id": {k: v[1] for k, v in sector.items()},
        "tax_category_id": {k: v[1] for k, v in std.items()},
        "tax_category_parent_id": {k: v[1] for k, v in std.items()},
        "observation_status": {k: v[1] for k, v in obs.items()},
    }
    rows = []
    for col, mapping in label.items():
        seen = df[col].dropna().unique()
        for key in sorted(seen):
            if key == "" or key is pd.NA:
                continue
            rows.append(("revenue", col, key, None, mapping.get(key, "")))
    d = pd.DataFrame(
        rows,
        columns=[
            "id_tabela",
            "nome_coluna",
            "chave",
            "cobertura_temporal",
            "valor",
        ],
    )
    order = [a["name"] for a in read_arch("dicionario")]
    tdir = Path(output_dir) / "dicionario"
    tdir.mkdir(parents=True, exist_ok=True)
    pq.write_table(
        _to_string_table(d, order), tdir / "data.parquet", compression="snappy"
    )
    log.info("dicionario: %d rows -> %s", len(d), tdir)
    return tdir


# ── orchestration ────────────────────────────────────────────────────────────
def clean_all(
    input_dir: Path,
    output_dir: Path,
    structure_cache: Path,
    version: str | None = None,
) -> dict:
    """Build the revenue table and its dictionary from downloaded CSVs.

    Returns a dict with output dirs, the max year present, and the codelists.
    """
    version = version or constants.DEFAULT_VERSION.value
    xml = fetch_structure(structure_cache)
    codelists = parse_codelists(xml)
    wide = build_revenue(input_dir, codelists, version)
    rev_dir = write_partitioned(wide, output_dir)
    dic_dir = build_dicionario(wide, codelists, output_dir)
    max_year = int(wide["year"].dropna().max()) if len(wide) else None
    return {
        "revenue": rev_dir,
        "dicionario": dic_dir,
        "max_year": max_year,
        "rows": len(wide),
    }
