"""Clean the OFLC disclosure files into one partitioned table per program.

Reads every source workbook in ``<OFLC_DATA_DIR>/input``, resolves its columns
through the committed crosswalk (``crosswalk/<program>.csv``), coerces types,
derives annualised wages, and writes hive-partitioned all-STRING Snappy Parquet
to ``<OFLC_DATA_DIR>/output/<program>/year=<FY>/data.parquet``.

Staging is all-STRING by Data Basis convention: the dbt model ``safe_cast``s
every column, and ``pipelines.utils.gcs.dump_header`` stringifies the header
BigQuery infers the staging schema from, so typed parquet is rejected. Values
still pass through their real types first — so ``year`` serialises as "2024",
not "2024.0" — and are cast to string via arrow, never ``astype(str)``, which
would render NULL as the literal "nan".

Usage:
    uv run python models/us_dol_oflc/code/clean_data.py [program ...]
"""

from __future__ import annotations

import csv
import datetime as dt
import json
import os
import re
import sys
from collections import defaultdict
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import python_calamine as pc

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
import canonical_map as cm  # noqa: E402
import wage_units as wu  # noqa: E402

DATA = Path(os.environ.get("OFLC_DATA_DIR", Path.home() / "Downloads/us_dol_oflc_data"))
INPUT = DATA / "input"
OUTPUT = DATA / "output"
REPORT = DATA / "clean_report.json"

PROGRAMS = ["lca", "perm", "h2a", "h2b"]

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
    with open(HERE / "crosswalk" / f"{program}.csv") as fh:
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

def build(program: str, report: dict) -> None:
    spec = cm.columns(program)
    order = [c for c, _ in spec]
    types = dict(spec)
    xw = load_crosswalk(program)
    by_year: dict[int, list[pd.DataFrame]] = defaultdict(list)
    unknown_units: dict[str, int] = defaultdict(int)

    files = sorted(
        p for p in INPUT.iterdir()
        if p.suffix in (".xls", ".xlsx") and p.stem.split("_")[0] == program
    )
    for path in files:
        fy = int(re.match(r"[a-z0-9]+_(\d{4})", path.stem).group(1))
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
            ("prevailing_wage", "prevailing_wage_unit_of_pay",
             "prevailing_wage_annual"),
        ):
            if target not in order:
                continue
            if unit in df.columns:
                canon_unit = df[unit].map(wu.normalise)
                for raw, norm in zip(df[unit], canon_unit):
                    if raw is not None and norm is None and _clean_str(raw):
                        unknown_units[str(raw)] += 1
                df[unit] = canon_unit
                df[target] = [
                    wu.annualise(a, u) for a, u in zip(df[amount], canon_unit)
                ]
            else:
                df[target] = None
        by_year[fy].append(df[order])
        print(f"  {path.name}: {n:,} rows -> FY{fy}", flush=True)

    typed = pa.schema([pa.field(c, {"STRING": pa.string(), "INT64": pa.int64(),
                                    "FLOAT64": pa.float64(), "DATE": pa.string()}[t])
                       for c, t in spec])
    strings = pa.schema([pa.field(c, pa.string()) for c, _ in spec])

    tdir = OUTPUT / program
    total = 0
    per_year = {}
    for fy in sorted(by_year):
        df = pd.concat(by_year[fy], ignore_index=True)
        before = len(df)
        df = df.drop_duplicates(subset=["case_number"], keep="last")
        dropped = before - len(df)
        pdir = tdir / f"year={fy}"
        pdir.mkdir(parents=True, exist_ok=True)
        at = pa.Table.from_pandas(df, schema=typed, preserve_index=False)
        pq.write_table(at.cast(strings), pdir / "data.parquet", compression="snappy")
        total += len(df)
        per_year[fy] = {"rows": len(df), "duplicate_case_numbers_dropped": dropped}
        print(f"  FY{fy}: {len(df):,} rows ({dropped:,} duplicate case numbers)",
              flush=True)

    report[program] = {"rows": total, "columns": len(order),
                       "by_year": per_year,
                       "unrecognised_wage_units": dict(unknown_units)}
    print(f"{program}: {total:,} rows, {len(order)} columns -> {tdir}\n", flush=True)


def main() -> int:
    wanted = [a for a in sys.argv[1:] if a in PROGRAMS] or PROGRAMS
    report = json.loads(REPORT.read_text()) if REPORT.exists() else {}
    for program in wanted:
        print(f"=== {program} ===", flush=True)
        build(program, report)
        REPORT.write_text(json.dumps(report, indent=1))
    for program, info in report.items():
        if info.get("unrecognised_wage_units"):
            print(f"WARNING {program}: unrecognised wage units "
                  f"{info['unrecognised_wage_units']}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
