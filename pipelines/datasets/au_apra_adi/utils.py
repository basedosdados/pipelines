"""Download + cleaning transform for au_apra_adi (shared by the recurring pipeline
and the one-shot bootstrap in models/au_apra_adi/code/).

The source is APRA's "Quarterly ADI performance" workbook. Each data tab is one
(institution type x sub-topic). Institution type comes from the tab's table
number (titles overlap); sub-topic from the title keywords. Fund/institution type
is folded into a row column. Flat statements (financial performance, financial
position, performance ratios) become wide tables (one column per measure);
nested statements (capital adequacy, asset quality, liquidity LCR/MLH) are melted
to long — the same reasoning that melts the two-Basel-era capital adequacy block.

Measure codes are generated from the source labels with a header-path scheme:
a row with no data in any quarter is a section header; a duplicated label is
disambiguated by its section, then by an occurrence counter. Validated to yield
unique codes across every tab.
"""

import csv
import datetime
import logging
import re
from collections import Counter
from pathlib import Path

import openpyxl
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import requests

from pipelines.datasets.au_apra_adi.constants import constants

log = logging.getLogger("au_apra_adi")

PA = {"STRING": pa.string(), "INT64": pa.int64(), "FLOAT64": pa.float64()}
_ARCH = constants.ARCHITECTURE_DIR.value
TAB_TYPE = constants.TAB_TYPE.value
INSTITUTION_LABELS = constants.INSTITUTION_LABELS.value
WIDE_TABLES = constants.WIDE_TABLES.value
LONG_TABLES = constants.LONG_TABLES.value
LABEL_ALIASES = constants.LABEL_ALIASES.value

_MONTHS = {
    "jan": 1,
    "feb": 2,
    "mar": 3,
    "apr": 4,
    "may": 5,
    "jun": 6,
    "jul": 7,
    "aug": 8,
    "sep": 9,
    "oct": 10,
    "nov": 11,
    "dec": 12,
}
_RATIO_PAT = re.compile(
    r"ratio|to loans|to assets|to deposits|to income|to non-performing|"
    r"to impaired|to shareholders|margin|\bshare\b|coverage|proportion|"
    r"requirement|growth in|net loans to|deposits to|equity to",
    re.I,
)


# ── helpers ──────────────────────────────────────────────────────────────────
def _norm(s) -> str:
    return re.sub(r"\s+", " ", str(s)).strip()


def _as_quarter(v):
    if isinstance(v, (datetime.datetime, datetime.date)):
        return (v.year, (v.month - 1) // 3 + 1)
    if isinstance(v, str):
        m = re.match(r"^([A-Za-z]{3})[a-z]*\s+(\d{4})$", v.strip())
        if m and m.group(1).lower() in _MONTHS:
            return (
                int(m.group(2)),
                (_MONTHS[m.group(1).lower()] - 1) // 3 + 1,
            )
    return None


def _num(v):
    if v is None:
        return None
    if isinstance(v, (int, float)):
        return float(v)
    s = str(v).strip().replace(",", "")
    if s in ("", "*", "-", "n/a", "N/A", "..", "np", "NP"):
        return None
    try:
        return float(s)
    except ValueError:
        return None


def _slug(s: str) -> str:
    s = s.lower().strip()
    s = re.sub(r"\s+[a-z]$", "", s)  # drop trailing footnote letter
    s = re.sub(r"\(.*?\)", "", s)  # drop parentheticals like ($m)
    s = re.sub(r"[^a-z0-9]+", "_", s).strip("_")
    return s


def _unit_for(label: str, subtopic: str) -> str:
    low = label.lower()
    if "number of entities" in low or low == "number of entities":
        return "unit"
    if "($m)" in low or "$m" in low:
        return "aud_million"
    if subtopic in ("financial_performance", "financial_position"):
        return "aud_million"
    # capital / asset_quality / liquidity / ratios: ratio-like -> proportion
    if _RATIO_PAT.search(label):
        return "proportion"
    return "aud_million"


def tab_meta(tab: str, title: str):
    """Return (institution_type, subtopic) for a data tab from its id and title."""
    num = (
        tab.replace("Tab ", "").rstrip("abcdefg")
        if tab.startswith("Tab ")
        else tab.rstrip("abcdef")
    )
    itype = TAB_TYPE.get(num)
    t = title.lower()
    if "financial performance" in t:
        sub = "financial_performance"
    elif "financial position" in t:
        sub = "financial_position"
    elif "capital adequacy" in t:
        sub = "capital_adequacy"
    elif "asset quality" in t:
        sub = "asset_quality"
    elif "liquidity" in t and "lcr" in t:
        sub = "liquidity_lcr"
    elif "liquidity" in t and "mlh" in t:
        sub = "liquidity_mlh"
    elif "performance ratios" in t:
        sub = "performance_ratios"
    else:
        sub = None
    return itype, sub


# ── parse ────────────────────────────────────────────────────────────────────
def _parse_tab(ws):
    """Return (quarters, rows) where rows is [(measure_code, label, unit, values)]."""
    grid = list(ws.iter_rows(values_only=True))
    hi, best = 0, -1
    for i, r in enumerate(grid):
        c = sum(1 for v in r if _as_quarter(v))
        if c > best:
            hi, best = i, c
    title = ""
    for r in grid[: hi + 1]:
        for v in r:
            if v and str(v).strip():
                title = _norm(v)
                break
        if title:
            break
    quarters = [
        (j, _as_quarter(grid[hi][j]))
        for j in range(len(grid[hi]))
        if _as_quarter(grid[hi][j])
    ]
    raw = []  # (prefix, label, values)
    # `prefix` disambiguates duplicated labels: a real section header (e.g.
    # "Credit exposures") is used as-is; a generic "of which:" header is replaced
    # by the line item it hangs off (the last data row), giving legible names
    # like interest_income__other rather than of_which__other.
    prefix = None
    last_data_label = None
    for r in grid[hi + 1 :]:
        lab = r[0]
        if lab is None:
            continue
        s = _norm(lab)
        if not s or s.lower().startswith(("*", "($", "quarter", "note")):
            continue
        vals = [_num(r[j]) for j, _ in quarters]
        if not any(v is not None for v in vals):
            prefix = last_data_label if re.match(r"of which", s, re.I) else s
            continue
        raw.append((prefix, s, vals))
        last_data_label = s
    # generate unique codes: base slug; if the base is duplicated in this tab,
    # prefix with its section/owner; then de-dup with a counter. Apply aliases.
    base = [_slug(lbl) for _, lbl, _v in raw]
    cnt = Counter(base)
    seen = Counter()
    rows = []
    for (pre, lab, vals), b in zip(raw, base, strict=True):
        code = b if cnt[b] == 1 else (f"{_slug(pre)}__{b}" if pre else b)
        seen[code] += 1
        if seen[code] > 1:
            code = f"{code}__{seen[code]}"
        code = LABEL_ALIASES.get(code, code)
        label = lab if (cnt[b] == 1 or not pre) else f"{pre}: {lab}"
        rows.append((code, label, _unit_for(lab, ""), vals))
    return title, quarters, rows


def _iter_tabs(wb):
    """Yield (tab, institution_type, subtopic, quarters, rows) for every data tab."""
    for tab in wb.sheetnames:
        if not re.match(r"^(Tab |A\.)", tab):
            continue
        title, quarters, rows = _parse_tab(wb[tab])
        itype, sub = tab_meta(tab, title)
        if itype is None or sub is None:
            log.warning(f"skipping unmapped tab {tab!r} ({title!r})")
            continue
        yield tab, itype, sub, quarters, rows


# ── schema ───────────────────────────────────────────────────────────────────
def read_arch(table: str) -> list[dict]:
    with open(_ARCH / f"{table}.csv", newline="") as fh:
        return list(csv.DictReader(fh))


def _long_records(wb, subtopic):
    recs = []
    for _tab, itype, sub, quarters, rows in _iter_tabs(wb):
        if sub != subtopic:
            continue
        for code, _label, _unit, vals in rows:
            unit = _unit_for(_label.split(": ")[-1], subtopic)
            # pyrefly: ignore [not-iterable]
            for (yr, qn), v in zip(
                (q for _, q in quarters), vals, strict=True
            ):
                if v is not None:
                    recs.append((yr, qn, itype, code, unit, v))
    return pd.DataFrame(
        recs,
        columns=[
            "year",
            "quarter",
            "institution_type",
            "measure",
            "unit",
            "value",
        ],
    )


def _wide_columns(wb, subtopic):
    """Canonical measure-column order for a wide subtopic: first-seen order across
    tabs (All ADIs first), aliases merged."""
    order = []
    for _tab, _it, sub, _q, rows in _iter_tabs(wb):
        if sub != subtopic:
            continue
        for code, _l, _u, _v in rows:
            if code not in order:
                order.append(code)
    return order


def build_table(wb, table: str) -> pd.DataFrame:
    if table in LONG_TABLES:
        df = _long_records(wb, table)
        df = df.drop_duplicates(
            ["year", "quarter", "institution_type", "measure"]
        )
        df["year"] = pd.to_numeric(df["year"]).astype("int64")
        df["quarter"] = pd.to_numeric(df["quarter"]).astype("int64")
        return df.sort_values(
            ["institution_type", "year", "quarter", "measure"]
        ).reset_index(drop=True)
    # wide
    cols = _wide_columns(wb, table)
    recs = []
    for _tab, itype, sub, quarters, rows in _iter_tabs(wb):
        if sub != table:
            continue
        rowmap = {code: vals for code, _l, _u, vals in rows}
        # pyrefly: ignore [not-iterable]
        for i, (_, (yr, qn)) in enumerate(quarters):
            rec = {"year": yr, "quarter": qn, "institution_type": itype}
            for code in cols:
                v = rowmap.get(code)
                rec[code] = v[i] if v is not None else None
            recs.append(rec)
    df = pd.DataFrame(
        recs, columns=["year", "quarter", "institution_type", *cols]
    )
    df["year"] = pd.to_numeric(df["year"]).astype("int64")
    df["quarter"] = pd.to_numeric(df["quarter"]).astype("int64")
    for c in cols:
        if c == "number_of_entities":
            df[c] = pd.to_numeric(df[c], errors="coerce").astype("Int64")
    return df.sort_values(["institution_type", "year", "quarter"]).reset_index(
        drop=True
    )


# ── write ────────────────────────────────────────────────────────────────────
def write_partitioned(df: pd.DataFrame, table: str, output_dir: Path) -> Path:
    arch = read_arch(table)
    order = [a["name"] for a in arch]
    typed = pa.schema(
        [pa.field(a["name"], PA[a["bigquery_type"]]) for a in arch]
    )
    string_schema = pa.schema([pa.field(a["name"], pa.string()) for a in arch])
    out = df[order]
    tdir = output_dir / table
    for year, g in out.groupby("year", sort=True):
        # pyrefly: ignore [bad-argument-type]
        pdir = tdir / f"year={int(year)}"
        pdir.mkdir(parents=True, exist_ok=True)
        at = pa.Table.from_pandas(g, schema=typed, preserve_index=False)
        at = at.cast(string_schema)
        pq.write_table(at, pdir / "data.parquet", compression="snappy")
    log.info(f"{table}: {len(out):,} rows -> {tdir}")
    return tdir


def build_dicionario(wb, output_dir: Path) -> Path:
    """institution_type labels (all tables) + measure code->label (long tables)."""
    rows: list = []
    for t in constants.DATA_TABLES.value:
        for code, label in INSTITUTION_LABELS.items():
            rows.append((t, "institution_type", code, None, label))
    for sub in LONG_TABLES:
        seen = {}
        for _tab, _it, s, _q, rrows in _iter_tabs(wb):
            if s != sub:
                continue
            for code, label, _u, _v in rrows:
                seen.setdefault(
                    code, label.split(": ")[-1] if ": " in label else label
                )
        for code, label in seen.items():
            rows.append((sub, "measure", code, None, label))
    df = pd.DataFrame(
        rows,
        columns=[
            "id_tabela",
            "nome_coluna",
            "chave",
            "cobertura_temporal",
            "valor",
        ],
    )
    schema = pa.schema(
        [
            pa.field(a["name"], PA[a["bigquery_type"]])
            for a in read_arch("dicionario")
        ]
    )
    tdir = output_dir / "dicionario"
    tdir.mkdir(parents=True, exist_ok=True)
    pq.write_table(
        pa.Table.from_pandas(df, schema=schema, preserve_index=False),
        tdir / "data.parquet",
        compression="snappy",
    )
    log.info(f"dicionario: {len(df):,} rows -> {tdir}")
    return tdir


# ── download ─────────────────────────────────────────────────────────────────
def discover_source_url(session: requests.Session | None = None) -> str:
    sess = session or requests.Session()
    r = sess.get(
        constants.LANDING_URL.value,
        headers={"User-Agent": constants.USER_AGENT.value},
        timeout=120,
    )
    r.raise_for_status()
    pat = re.compile(
        r'href="([^"]*?' + constants.FILE_PATTERN.value + r'[^"]*?\.xlsx)"',
        re.IGNORECASE,
    )
    hrefs = pat.findall(r.text)
    if not hrefs:
        raise RuntimeError(
            "No ADI performance .xlsx link found on landing page"
        )
    href = hrefs[0]
    return href if href.startswith("http") else constants.BASE_URL.value + href


def download_workbook(
    input_dir: Path, session: requests.Session | None = None
) -> Path:
    sess = session or requests.Session()
    url = discover_source_url(sess)
    input_dir.mkdir(parents=True, exist_ok=True)
    dest = input_dir / "quarterly_adi_performance.xlsx"
    r = sess.get(
        url, headers={"User-Agent": constants.USER_AGENT.value}, timeout=300
    )
    r.raise_for_status()
    dest.write_bytes(r.content)
    log.info(f"downloaded {url} -> {dest}")
    return dest


def clean_all(input_dir: Path, output_dir: Path) -> dict:
    xlsx = next(input_dir.glob("*.xlsx"))
    wb = openpyxl.load_workbook(xlsx, read_only=True, data_only=True)
    result: dict = {}
    max_yq = None
    for table in constants.DATA_TABLES.value:
        df = build_table(wb, table)
        result[table] = write_partitioned(df, table, output_dir)
        if len(df):
            last = df.sort_values(["year", "quarter"]).iloc[-1]
            yq = f"{int(last['year'])}-{int(last['quarter'])}"
            if max_yq is None or yq > max_yq:
                max_yq = yq
    result["dicionario"] = build_dicionario(wb, output_dir)
    result["max_year_quarter"] = max_yq
    return result
