"""Pure download and cleaning functions for us_dol_oflc.

No Prefect imports: these functions are shared by the recurring pipeline
(``tasks.py``) and the one-shot onboarding bootstrap
(``models/us_dol_oflc/code/clean_data.py``), so the cleaning transform exists in
exactly one place.

The Department of Labor renames, splits and merges columns almost every fiscal
year. The committed crosswalk under ``models/us_dol_oflc/code/crosswalk/``
resolves that churn; ``read_file`` refuses to read a workbook the crosswalk does
not describe rather than guessing.
"""

from __future__ import annotations

import csv
import datetime as dt
import re
from collections import defaultdict
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import python_calamine as pc

from pipelines.datasets.us_dol_oflc import canonical_map as cm
from pipelines.datasets.us_dol_oflc import wage_units as wu
from pipelines.datasets.us_dol_oflc.constants import constants

CROSSWALK_DIR = constants.CROSSWALK_DIR.value

# Columns derived here rather than read from the source.
DERIVED = {
    "year", "source_file",
    "wage_offered_from_annual", "wage_offered_to_annual", "prevailing_wage_annual",
}

# Values the source uses for "blank".
NULLISH = {"", "NA", "N/A", "NULL", "NONE", "UNKNOWN", "-", "--", "."}


# --------------------------------------------------------------------------
# Value coercion
# --------------------------------------------------------------------------

def _clean_str(v: object) -> str | None:
    if v is None:
        return None
    if isinstance(v, float) and v != v:  # NaN
        return None
    if isinstance(v, (dt.date, dt.datetime)):
        return v.isoformat()
    if isinstance(v, float) and v.is_integer():
        s = str(int(v))
    else:
        s = str(v)
    s = " ".join(s.split())
    return None if s.upper() in NULLISH else s


_MONEY = re.compile(r"[^0-9.\-]")


def _to_float(v: object) -> float | None:
    s = _clean_str(v)
    if s is None:
        return None
    s = _MONEY.sub("", s)
    if s in ("", "-", "."):
        return None
    try:
        f = float(s)
    except ValueError:
        return None
    return None if f != f else f


def _to_int(v: object) -> int | None:
    f = _to_float(v)
    return None if f is None else int(round(f))


_DATE_FORMATS = ("%Y-%m-%d", "%m/%d/%Y", "%m/%d/%y", "%d-%b-%y", "%Y%m%d",
                 "%m-%d-%Y", "%b %d, %Y")


def _to_date(v: object) -> str | None:
    if isinstance(v, dt.datetime):
        return v.date().isoformat()
    if isinstance(v, dt.date):
        return v.isoformat()
    s = _clean_str(v)
    if s is None:
        return None
    s = s.split("T")[0].split(" ")[0]
    for fmt in _DATE_FORMATS:
        try:
            return dt.datetime.strptime(s, fmt).date().isoformat()
        except ValueError:
            continue
    return None


COERCE = {"STRING": _clean_str, "INT64": _to_int, "FLOAT64": _to_float,
          "DATE": _to_date}


# --------------------------------------------------------------------------
# Crosswalk
# --------------------------------------------------------------------------

def load_crosswalk(program: str) -> dict[tuple[int, str], dict[str, str]]:
    """(fiscal_year, source_file) -> {source_column: canonical_column}."""
    out: dict[tuple[int, str], dict[str, str]] = defaultdict(dict)
    with open(CROSSWALK_DIR / f"{program}.csv") as fh:
        for row in csv.DictReader(fh):
            if row["disposition"] != "mapped":
                continue
            out[(int(row["fiscal_year"]), row["source_file"])][
                row["source_column"]] = row["canonical_column"]
    return out


def read_sheet(path: Path) -> tuple[list[str], list[list]]:
    ws = pc.CalamineWorkbook.from_path(str(path)).get_sheet_by_index(0)
    rows = ws.to_python()
    if not rows:
        return [], []
    return [str(c).strip() for c in rows[0]], rows[1:]


# --------------------------------------------------------------------------
# Build one program
# --------------------------------------------------------------------------

def source_files(program: str, input_dir: Path) -> dict[int, list[Path]]:
    """Source workbooks for one program, grouped by fiscal year.

    A fiscal year can have several files: the two FY2009 LCA systems, the four
    quarterly LCA files from FY2020, the two FY2024 PERM form versions, the two
    FY2025 H-2A form versions.
    """
    by_year: dict[int, list[Path]] = defaultdict(list)
    for path in sorted(input_dir.iterdir()):
        if path.suffix not in (".xls", ".xlsx"):
            continue
        m = re.match(rf"{program}_(\d{{4}})", path.stem)
        if m:
            by_year[int(m.group(1))].append(path)
    return dict(sorted(by_year.items()))


def read_file(path: Path, fy: int, program: str, order: list[str],
              types: dict[str, str], xw, unknown_units) -> pd.DataFrame:
    """One source workbook as a canonical-schema DataFrame."""
    mapping = xw.get((fy, path.name))
    if not mapping:
        raise SystemExit(f"No crosswalk entry for {path.name} (FY{fy})")
    header, rows = read_sheet(path)
    idx = {col: i for i, col in enumerate(header)}
    data: dict[str, list] = {}
    for src, canon in mapping.items():
        i = idx[src]
        fn = COERCE[types[canon]]
        data[canon] = [fn(r[i]) if i < len(r) else None for r in rows]
    n = len(rows)
    for canon in order:
        if canon in DERIVED or canon in data:
            continue
        data[canon] = [None] * n
    data["year"] = [fy] * n
    data["source_file"] = [path.name] * n

    df = pd.DataFrame(data)
    for amount, unit, target in (
        ("wage_offered_from", "wage_unit_of_pay", "wage_offered_from_annual"),
        ("wage_offered_to", "wage_unit_of_pay", "wage_offered_to_annual"),
        ("prevailing_wage", "prevailing_wage_unit_of_pay", "prevailing_wage_annual"),
    ):
        if target not in order:
            continue
        if unit in df.columns:
            canon_unit = df[unit].map(wu.normalise)
            for raw, norm in zip(df[unit], canon_unit):
                if raw is not None and norm is None and _clean_str(raw):
                    unknown_units[str(raw)] += 1
            df[unit] = canon_unit
            df[target] = [wu.annualise(a, u) for a, u in zip(df[amount], canon_unit)]
        else:
            df[target] = None
    print(f"  {path.name}: {n:,} rows -> FY{fy}", flush=True)
    return df[order]


def build(program: str, report: dict, input_dir: Path, output_dir: Path,
          years: set[int] | None = None, resume: bool = False) -> None:
    """Clean one program, writing and freeing one fiscal year at a time.

    Holding every year in memory would need tens of gigabytes for the LCA
    table, so each fiscal year is written to its own partition and released
    before the next one is read.
    """
    spec = cm.columns(program)
    order = [c for c, _ in spec]
    types = dict(spec)
    xw = load_crosswalk(program)
    unknown_units: dict[str, int] = defaultdict(int)

    typed = pa.schema([pa.field(c, {"STRING": pa.string(), "INT64": pa.int64(),
                                    "FLOAT64": pa.float64(), "DATE": pa.string()}[t])
                       for c, t in spec])
    strings = pa.schema([pa.field(c, pa.string()) for c, _ in spec])

    tdir = output_dir / program
    total = 0
    per_year = {}
    for fy, paths in source_files(program, input_dir).items():
        if years and fy not in years:
            continue
        pdir = tdir / f"year={fy}"
        target = pdir / "data.parquet"
        if resume and target.exists():
            print(f"  FY{fy}: already written, skipping", flush=True)
            continue
        frames = [read_file(p, fy, program, order, types, xw, unknown_units)
                  for p in paths]
        df = pd.concat(frames, ignore_index=True) if len(frames) > 1 else frames[0]
        del frames
        before = len(df)
        df = df.drop_duplicates(subset=["case_number"], keep="last")
        dropped = before - len(df)
        pdir.mkdir(parents=True, exist_ok=True)
        at = pa.Table.from_pandas(df, schema=typed, preserve_index=False)
        pq.write_table(at.cast(strings), target, compression="snappy")
        total += len(df)
        per_year[fy] = {"rows": len(df), "files": [p.name for p in paths],
                        "duplicate_case_numbers_dropped": dropped}
        print(f"  FY{fy}: {len(df):,} rows from {len(paths)} file(s) "
              f"({dropped:,} duplicate case numbers)", flush=True)
        del df, at

    key = program if not years else f"{program}:{min(years)}-{max(years)}"
    report[key] = {"rows": total, "columns": len(order), "by_year": per_year,
                   "unrecognised_wage_units": dict(unknown_units)}
    print(f"{program}: {total:,} rows, {len(order)} columns -> {tdir}\n", flush=True)



# --------------------------------------------------------------------------
# Download
# --------------------------------------------------------------------------

def _session():
    """A curl_cffi session impersonating Chrome.

    www.dol.gov is behind Akamai, which returns HTTP 403 to ``requests``,
    ``curl`` and ``wget`` regardless of headers — the block keys on the TLS
    fingerprint. curl_cffi reproduces Chrome's fingerprint and is admitted.
    """
    from curl_cffi import requests as cffi_requests

    return cffi_requests.Session(impersonate=constants.IMPERSONATE.value,
                                 timeout=1800)


def list_source_files(program: str) -> dict[str, str]:
    """Every disclosure workbook for one program: ``{href: absolute url}``.

    Scrapes the performance page rather than guessing URLs, because the file
    naming is inconsistent from year to year.
    """
    session = _session()
    html = session.get(constants.PERFORMANCE_PAGE.value).text
    pattern = re.compile(constants.FILE_PATTERNS.value[program], re.I)
    found: dict[str, str] = {}
    for href in re.findall(r'href="([^"]+\.xlsx?)"', html):
        name = href.rsplit("/", 1)[-1]
        if not pattern.search(name):
            continue
        url = href if href.startswith("http") else constants.BASE_URL.value + href
        found[name] = url.replace("//media/", "/media/")
    return found


def download_file(url: str, dest: Path, session=None) -> Path:
    """Download one workbook, writing atomically."""
    session = session or _session()
    dest.parent.mkdir(parents=True, exist_ok=True)
    response = session.get(
        url, headers={"Referer": constants.PERFORMANCE_PAGE.value}, stream=True)
    if response.status_code != 200:
        raise RuntimeError(f"HTTP {response.status_code} for {url}")
    tmp = dest.with_suffix(dest.suffix + ".part")
    size = 0
    with open(tmp, "wb") as fh:
        for chunk in response.iter_content(1 << 20):
            fh.write(chunk)
            size += len(chunk)
    if size < 10_000:
        tmp.unlink(missing_ok=True)
        raise RuntimeError(f"Suspiciously small download ({size} bytes) for {url}")
    tmp.rename(dest)
    return dest


def fiscal_year_of(name: str) -> int | None:
    """Fiscal year a source file name refers to, or None if it has none."""
    m = re.search(r"FY_?(\d{4})", name, re.I)
    if m:
        return int(m.group(1))
    m = re.search(r"FY_?(\d{2})\b", name, re.I)
    if m:
        return 2000 + int(m.group(1))
    return None


def local_name(program: str, name: str) -> str:
    """Local file name for a source workbook, matching the crosswalk key."""
    fy = fiscal_year_of(name)
    quarter = re.search(r"_Q([1-4])", name, re.I)
    suffix = f"q{quarter.group(1)}" if quarter and fy and fy >= 2020 else ""
    return f"{program}_{fy}{suffix}{Path(name).suffix}"


# --------------------------------------------------------------------------
# Fiscal-year orchestration for the recurring pipeline
# --------------------------------------------------------------------------

def current_fiscal_year(today: dt.date | None = None) -> int:
    """Federal fiscal year containing ``today``.

    The US federal fiscal year runs 1 October to 30 September and is named for
    the calendar year it ends in, so October 2025 is already FY2026.
    """
    today = today or dt.date.today()
    start = constants.FISCAL_YEAR_START_MONTH.value
    return today.year + 1 if today.month >= start else today.year


def refresh_fiscal_years(open_fy: int | None = None) -> list[int]:
    """Fiscal years the recurring pipeline re-materialises on each run.

    Only the open fiscal year and the one before it. A closed year changes only
    when the Department of Labor reissues it, and the previous year is kept in
    scope because its final annual file lands after the year ends.
    """
    open_fy = open_fy or current_fiscal_year()
    return [open_fy - 1, open_fy]


def download_fiscal_years(program: str, years: list[int], input_dir: Path) -> list[Path]:
    """Download every published workbook for ``program`` in ``years``.

    The quarterly LCA files for a fiscal year are disjoint for FY2020-FY2025 and
    cumulative for FY2026, so every file the source lists for the year is taken
    and :func:`build` de-duplicates on case number. That is correct under both
    regimes.
    """
    session = _session()
    input_dir.mkdir(parents=True, exist_ok=True)
    got: list[Path] = []
    for name, url in sorted(list_source_files(program).items()):
        fy = fiscal_year_of(name)
        if fy not in years:
            continue
        dest = input_dir / local_name(program, name)
        if dest.exists() and dest.stat().st_size > 10_000:
            got.append(dest)
            continue
        got.append(download_file(url, dest, session))
    return got


def clean_fiscal_years(program: str, years: list[int], input_dir: Path,
                       output_dir: Path) -> dict:
    """Re-materialise ``years`` of ``program`` from the downloaded workbooks.

    The whole fiscal year is rebuilt rather than appended to: a new quarterly
    file restates part of the year, so appending would duplicate cases.
    """
    report: dict = {}
    build(program, report, input_dir, output_dir, set(years), resume=False)
    return report


def max_decision_date(output_dir: Path, program: str) -> str | None:
    """Latest ``decision_date`` present in a cleaned program table.

    This is the source's own coverage high-water mark and is what the
    source-update poll compares against.
    """
    import pyarrow.dataset as pads

    tdir = output_dir / program
    if not tdir.exists():
        return None
    ds = pads.dataset(tdir, format="parquet", partitioning="hive")
    best: str | None = None
    for batch in ds.to_batches(columns=["decision_date"]):
        values = [v for v in batch.column(0).to_pylist() if v]
        if values:
            top = max(values)
            best = top if best is None or top > best else best
    return best
