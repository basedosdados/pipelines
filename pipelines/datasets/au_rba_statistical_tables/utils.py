"""Pure download + cleaning functions for au_rba_statistical_tables.

No Prefect imports here — this module is the single source of truth for the
transform, shared by the recurring pipeline (``tasks.py``) and the one-shot
onboarding bootstrap under ``models/au_rba_statistical_tables/code/``.

The RBA publishes each statistical table as a CSV with a metadata block keyed by
row label (``Title``, ``Description``, ``Frequency``, ``Type``, ``Units``,
``Source``, ``Publication date``, ``Series ID``) above a dated data block. Each
data column is one series; the transform pivots that wide block into long
``(table_code, series_id, date, value)`` rows and lifts the metadata block into
a series catalogue.

Two source properties drive the design:

1. ``series_id`` is NOT globally unique. 233 mnemonics appear in both the
   ``b13.1.2-*`` and ``b13.2.1-*`` tables carrying different values (827 of 834
   overlapping cells differ). The key is therefore ``(table_code, series_id)``.

2. Four file families are not ``(series_id, date)`` time series at all and are
   excluded — see ``NON_TIMESERIES_PREFIXES``.

Licence gate: only series every one of whose named sources publishes under
CC BY 4.0 (RBA, APRA, ABS) are kept. See ``models/au_rba_statistical_tables/LICENCE.md``.
"""

from __future__ import annotations

import csv
import io
import re
import time
from collections import defaultdict
from dataclasses import dataclass, field
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import requests

USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/120 Safari/537.36"
)
BASE = "https://www.rba.gov.au"
TABLES_INDEX = f"{BASE}/statistics/tables/"

# Metadata row labels -> canonical field name. The second group is the legacy
# layout used by a5-data.csv.
META_LABELS = {
    "title": "title",
    "description": "description",
    "frequency": "frequency",
    "type": "series_type",
    "units": "units",
    "source": "source",
    "publication date": "publication_date",
    "series id": "series_id",
    "mnemonic": "series_id",
    "last updated": "publication_date",
}

# Families whose rows are transaction-, instrument-, or forecast-vintage-level
# detail rather than a (series_id, date) time series. Including them would
# produce ~20,700 duplicate keys and misstate the table's grain.
#   a3-, a3.2-  open market operations / securities lending / switches
#   f16.1-      per-bond semi-government detail
#   j1-         market economists' forecasts, keyed by survey date AND target quarter
NON_TIMESERIES_PREFIXES = ("a3-", "a3.2-", "f16.1-", "j1-")

# Publishers that independently release under CC BY 4.0, so their material may be
# redistributed under the RBA notice's "published licence terms" fallback.
CCBY_PUBLISHERS = {"rba", "reserve bank of australia", "apra", "abs"}

_MONTHS = [
    "jan",
    "feb",
    "mar",
    "apr",
    "may",
    "jun",
    "jul",
    "aug",
    "sep",
    "oct",
    "nov",
    "dec",
]
_NUM_RE = re.compile(r"^-?\d+(\.\d+)?([eE][-+]?\d+)?$")

# series_break.break_type is a coded value. The RBA repeats the same three
# definitions verbatim in the preamble of all 32 series-breaks files, so they are
# stable enough to hard-code as the dictionary.
BREAK_TYPE_LABELS = {
    "A": (
        "Breaks in series due to the establishment of new banks. All new banks shown. "
        "Date refers to the first month in which the institution reported as a bank."
    ),
    "B": (
        "Breaks in series due to other changes in bank reporting. "
        "Some smaller breaks are not listed."
    ),
    "C": (
        "Breaks in series due to changes in the coverage and reporting of non-banks. "
        "Other than the counterpart breaks associated with the conversion of NBFIs to banks "
        "and transfer of assets and liabilities from NBFIs to banks. "
        "Some smaller breaks are not listed."
    ),
    "B,C": "Break attributable to both a change in bank reporting (B) and in non-bank coverage (C).",
    "Other": "Break not classified into types A, B or C by the RBA.",
}

# The RBA's Frequency field carries case drift and one persistent typo
# ("Semiannnual"). Normalised to a small controlled vocabulary.
_FREQ_CANON = {
    "daily": "Daily",
    "weekly": "Weekly",
    "fortnightly": "Fortnightly",
    "monthly": "Monthly",
    "quarterly": "Quarterly",
    "semiannual": "Semiannual",
    "semiannnual": "Semiannual",
    "semi-annual": "Semiannual",
    "yearly": "Yearly",
    "annual": "Yearly",
    "as announced": "As announced",
    "per operation": "Per operation",
    "see notes": "See notes",
}


# --------------------------------------------------------------------------
# download
# --------------------------------------------------------------------------
def list_csv_urls(session: requests.Session | None = None) -> list[str]:
    """Scrape the statistical-tables index for every CSV path."""
    session = session or requests.Session()
    resp = session.get(
        TABLES_INDEX, headers={"User-Agent": USER_AGENT}, timeout=120
    )
    resp.raise_for_status()
    paths = sorted(
        set(
            re.findall(
                r'href="(/statistics/tables/csv/[^"]+\.csv)"', resp.text
            )
        )
    )
    return [BASE + p for p in paths]


def download_all(dest: Path, throttle: float = 0.3) -> list[Path]:
    """Download every statistical-table CSV into ``dest``. Returns saved paths."""
    dest = Path(dest)
    dest.mkdir(parents=True, exist_ok=True)
    session = requests.Session()
    out = []
    for url in list_csv_urls(session):
        target = dest / url.rsplit("/", 1)[-1]
        resp = session.get(
            url, headers={"User-Agent": USER_AGENT}, timeout=180
        )
        if resp.status_code != 200:
            continue
        target.write_bytes(resp.content)
        out.append(target)
        time.sleep(throttle)
    return out


# --------------------------------------------------------------------------
# parsing primitives
# --------------------------------------------------------------------------
def parse_date(raw: str) -> str | None:
    """RBA mixes ``dd-Mon-yyyy`` and ``dd/mm/yyyy``. Return ISO date or None."""
    s = (raw or "").strip()
    if not s:
        return None
    m = re.fullmatch(r"(\d{1,2})-([A-Za-z]{3})-(\d{2,4})", s)
    if m:
        try:
            mo = _MONTHS.index(m.group(2).lower()) + 1
        except ValueError:
            return None
        year = int(m.group(3))
        if year < 100:  # legacy two-digit year, e.g. "08-Oct-25"
            year += 2000 if year < 70 else 1900
        return f"{year:04d}-{mo:02d}-{int(m.group(1)):02d}"
    m = re.fullmatch(r"(\d{1,2})/(\d{1,2})/(\d{4})", s)
    if m:
        return f"{int(m.group(3)):04d}-{int(m.group(2)):02d}-{int(m.group(1)):02d}"
    return None


def parse_value(raw: str) -> float | None:
    """Numeric value, or None when the cell carries no number.

    Handles RBA formatting drift: a ``$`` prefix appears in f9 from 2020,
    thousands separators appear sporadically, and ``N/A`` marks an explicit gap.
    Cells that are still non-numeric — such as the ``"17.00 to 17.50"`` target
    ranges in a2 — become None; the source carries those as prose, not data.
    """
    s = (raw or "").strip()
    # The en dash is a real missing-value marker in the RBA files, not a typo.
    if not s or s.upper() in {"N/A", "NA", "-", "–", "..", "."}:  # noqa: RUF001
        return None
    s = s.replace("$", "").replace(",", "").replace(" ", "")
    if s.startswith("(") and s.endswith(")"):  # accounting negative
        s = "-" + s[1:-1]
    if not _NUM_RE.match(s):
        return None
    return float(s)


def normalise_frequency(raw: str) -> str:
    """Fold the Frequency field onto a controlled vocabulary."""
    s = (raw or "").strip()
    return _FREQ_CANON.get(s.lower(), s)


def is_redistributable(source: str) -> bool:
    """True when every publisher named in ``Source`` releases under CC BY 4.0.

    Splitting on ``; / ,`` and newlines, every token must be in
    ``CCBY_PUBLISHERS``. This drops the commercial-vendor series (Bloomberg,
    Refinitiv, ASX, FENICS, Austraclear, ...) including mixed attributions such
    as ``"Bloomberg; RBA"``, where the vendor's content is inseparable.
    """
    if not source or not source.strip():
        return False
    tokens = [
        t.strip().lower() for t in re.split(r"[;/,\n]+", source) if t.strip()
    ]
    return bool(tokens) and all(t in CCBY_PUBLISHERS for t in tokens)


def table_code_and_name(header: str) -> tuple[str, str]:
    """Split ``"F1 INTEREST RATES AND YIELDS – MONEY MARKET"`` into code + name."""  # noqa: RUF002
    h = (header or "").strip()
    m = re.match(r"^([A-Z]\d+(?:\.\d+)*)\s+(.*)$", h)
    if m:
        return m.group(1), m.group(2).strip()
    return "", h


def is_timeseries_file(name: str) -> bool:
    """False for the transaction/instrument/forecast detail families."""
    return not name.startswith(NON_TIMESERIES_PREFIXES)


# --------------------------------------------------------------------------
# table parsing
# --------------------------------------------------------------------------
@dataclass
class ParsedTable:
    file: str
    table_code: str
    table_name: str
    series: list[dict] = field(default_factory=list)
    # (table_code, series_id, iso_date, value)
    observations: list[tuple] = field(default_factory=list)
    skipped_reason: str = ""


def parse_table(path: Path) -> ParsedTable:
    """Parse one RBA statistical-table CSV into series metadata + long observations."""
    path = Path(path)
    rows = list(
        csv.reader(
            io.StringIO(path.read_text(encoding="utf-8-sig", errors="replace"))
        )
    )
    if not rows:
        return ParsedTable(path.name, "", "", skipped_reason="empty file")

    code, name = table_code_and_name(rows[0][0] if rows[0] else "")
    out = ParsedTable(path.name, code, name)

    meta: dict[str, list[str]] = {}
    data_start = None
    for i, row in enumerate(rows):
        if not row or not row[0].strip():
            continue
        label = row[0].strip().lower()
        if label in META_LABELS:
            # First occurrence wins; trailing note blocks can repeat a label.
            meta.setdefault(META_LABELS[label], row[1:])
        elif parse_date(row[0]):
            data_start = i
            break

    if not meta.get("series_id") or data_start is None:
        out.skipped_reason = "no Series ID / Mnemonic block, or no dated rows"
        return out

    sids = [s.strip() for s in meta["series_id"]]

    def col(fieldname, j):
        vals = meta.get(fieldname) or []
        return vals[j].strip() if j < len(vals) else ""

    keep_idx: dict[int, str] = {}
    for j, sid in enumerate(sids):
        if not sid or sid in keep_idx.values():
            continue  # blank, or a mnemonic repeated within one file (f16.1)
        source = col("source", j)
        out.series.append(
            {
                "table_code": code,
                "series_id": sid,
                "table_name": name,
                "title": col("title", j),
                "description": col("description", j),
                "frequency": normalise_frequency(col("frequency", j)),
                "series_type": col("series_type", j),
                "units": col("units", j),
                "source": source.replace("\n", " ").strip(),
                "publication_date": parse_date(col("publication_date", j)),
                "source_file": path.name,
                "redistributable": is_redistributable(source),
            }
        )
        keep_idx[j] = sid

    for row in rows[data_start:]:
        if not row:
            continue
        iso = parse_date(row[0])
        if not iso:
            continue
        for j, sid in keep_idx.items():
            val = parse_value(row[j + 1] if j + 1 < len(row) else "")
            if val is None:
                continue  # drops empty future padding rows and N/A gaps
            out.observations.append((code, sid, iso, val))

    return out


def parse_series_breaks(path: Path) -> list[dict]:
    """Parse a ``*-series-breaks.csv`` into rows.

    Layout: prose preamble, a ``Date,Break type,Series title,Details`` header,
    then the break rows.
    """
    path = Path(path)
    rows = list(
        csv.reader(
            io.StringIO(path.read_text(encoding="utf-8-sig", errors="replace"))
        )
    )
    if not rows:
        return []
    code, name = table_code_and_name(rows[0][0] if rows[0] else "")
    header_i = next(
        (
            i
            for i, r in enumerate(rows)
            if r and r[0].strip().lower() == "date" and len(r) >= 3
        ),
        None,
    )
    if header_i is None:
        return []

    out = []
    for row in rows[header_i + 1 :]:
        if not row or not row[0].strip():
            continue
        iso = parse_date(row[0])
        if not iso:
            continue

        def cell(k, _row=row):
            return _row[k].strip() if k < len(_row) else ""

        out.append(
            {
                "table_code": code,
                "table_name": name,
                "date": iso,
                "break_type": cell(1),
                "series_title": cell(2),
                "details": cell(3),
            }
        )
    return out


# --------------------------------------------------------------------------
# orchestration
# --------------------------------------------------------------------------
def clean_all(input_dir: Path) -> dict:
    """Parse every CSV in ``input_dir`` into the three output tables.

    Returns ``{"observation", "series", "series_break", "excluded", "skipped"}``.
    Licence gate and the non-time-series family filter are both applied here.
    """
    files = sorted(Path(input_dir).glob("*.csv"))
    series_all, obs_all, breaks, skipped = [], [], [], []

    for path in files:
        if path.name.endswith("-series-breaks.csv"):
            breaks.extend(parse_series_breaks(path))
            continue
        if not is_timeseries_file(path.name):
            skipped.append(
                {"file": path.name, "reason": "non-time-series family"}
            )
            continue
        parsed = parse_table(path)
        if parsed.skipped_reason:
            skipped.append(
                {"file": path.name, "reason": parsed.skipped_reason}
            )
            continue
        series_all.extend(parsed.series)
        obs_all.extend(parsed.observations)

    keep = {
        (s["table_code"], s["series_id"])
        for s in series_all
        if s["redistributable"]
    }
    excluded = [s for s in series_all if not s["redistributable"]]
    observations = [o for o in obs_all if (o[0], o[1]) in keep]

    # Derive per-series coverage from the observations actually kept.
    span: dict[tuple, list] = defaultdict(lambda: [None, None])
    for tc, sid, iso, _ in observations:
        cur = span[(tc, sid)]
        if cur[0] is None or iso < cur[0]:
            cur[0] = iso
        if cur[1] is None or iso > cur[1]:
            cur[1] = iso

    series = []
    for s in series_all:
        if not s["redistributable"]:
            continue
        start, end = span.get((s["table_code"], s["series_id"]), (None, None))
        rec = {
            k: v
            for k, v in s.items()
            if k not in {"redistributable", "source_file"}
        }
        rec["observation_start"] = start
        rec["observation_end"] = end
        series.append(rec)

    # Dictionary for the one coded column in the dataset.
    used_types = sorted({b["break_type"] for b in breaks if b["break_type"]})
    dicionario = [
        {
            "id_tabela": "series_break",
            "nome_coluna": "break_type",
            "chave": code,
            "cobertura_temporal": "",
            "valor": BREAK_TYPE_LABELS.get(code, ""),
        }
        for code in used_types
    ]

    return {
        "observation": observations,
        "series": series,
        "series_break": breaks,
        "dicionario": dicionario,
        "excluded": excluded,
        "skipped": skipped,
        "n_files": len(files),
    }


# --------------------------------------------------------------------------
# parquet output — all-STRING staging (see .claude/rules/bigquery-conventions.md)
# --------------------------------------------------------------------------
def _string_table(rows: list[dict], columns: list[str]) -> pa.Table:
    """Build an all-STRING arrow table with a stable column order.

    Cast through arrow rather than ``astype(str)``: the latter renders NULL as
    the literal ``"nan"``, which ``safe_cast`` will not turn back into NULL.
    """
    arrays = []
    for c in columns:
        vals = [r.get(c) for r in rows]
        if vals and any(
            isinstance(v, (int, float)) and not isinstance(v, bool)
            for v in vals
        ):
            arr = pa.array(vals)  # let arrow infer the real type first
            arr = arr.cast(pa.string())
        else:
            arr = pa.array(
                [None if v is None else str(v) for v in vals], type=pa.string()
            )
        arrays.append(arr)
    return pa.Table.from_arrays(arrays, names=columns)


OBSERVATION_COLUMNS = ["year", "date", "table_code", "series_id", "value"]
SERIES_COLUMNS = [
    "table_code",
    "series_id",
    "table_name",
    "title",
    "description",
    "frequency",
    "series_type",
    "units",
    "source",
    "publication_date",
    "observation_start",
    "observation_end",
]
SERIES_BREAK_COLUMNS = [
    "table_code",
    "table_name",
    "date",
    "break_type",
    "series_title",
    "details",
]
DICIONARIO_COLUMNS = [
    "id_tabela",
    "nome_coluna",
    "chave",
    "cobertura_temporal",
    "valor",
]


def write_partitioned(cleaned: dict, output_dir: Path) -> dict:
    """Write the three tables as all-STRING parquet. Returns row counts.

    ``observation`` is hive-partitioned by ``year``; the two small catalogue
    tables are single files.
    """
    output_dir = Path(output_dir)
    counts = {}

    by_year: dict[str, list[dict]] = defaultdict(list)
    for tc, sid, iso, value in cleaned["observation"]:
        by_year[iso[:4]].append(
            {
                "year": iso[:4],
                "date": iso,
                "table_code": tc,
                "series_id": sid,
                "value": value,
            }
        )
    total = 0
    for year, rows in sorted(by_year.items()):
        d = output_dir / "observation" / f"year={year}"
        d.mkdir(parents=True, exist_ok=True)
        # `year` is the partition key: hive-encoded in the path, dropped from the file.
        cols = [c for c in OBSERVATION_COLUMNS if c != "year"]
        pq.write_table(
            _string_table(rows, cols), d / "data.parquet", compression="snappy"
        )
        total += len(rows)
    counts["observation"] = total

    for name, cols in (
        ("series", SERIES_COLUMNS),
        ("series_break", SERIES_BREAK_COLUMNS),
        ("dicionario", DICIONARIO_COLUMNS),
    ):
        rows = cleaned[name]
        d = output_dir / name
        d.mkdir(parents=True, exist_ok=True)
        pq.write_table(
            _string_table(rows, cols), d / "data.parquet", compression="snappy"
        )
        counts[name] = len(rows)

    return counts
