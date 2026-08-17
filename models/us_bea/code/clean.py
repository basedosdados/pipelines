"""Download + clean all six us_bea tables into partitioned Parquet.

Pull strategy (verified against the live API):
  - NIPA: one GetData per TableName, Frequency='A,Q,M', Year='ALL'
          (rows carry no Frequency field; derive from TimePeriod).
  - GDPbyIndustry: per TableID, Frequency A then Q, Industry='ALL', Year='ALL'.
  - Regional: LineCode='ALL' is UNSUPPORTED -> loop specific line codes,
              wildcard the geography (GeoFips = STATE / COUNTY / MSA).
      regional_state  = SA*/SQ*/PR*/TA* tables, GeoFips=STATE
      regional_county = CA* tables,           GeoFips=COUNTY
      regional_metro  = MA* tables,           GeoFips=MSA

Output: <outdir>/<db_table>/year=<YYYY>/<part>.parquet  (typed pa.Schema).
Resume: a table is skipped if <outdir>/.done/<db_table> exists.

Run:  python -m models.us_bea.code.clean [table ...]   (default: all)
"""

from __future__ import annotations

import json
import os
import re
import shutil
import sys
import time

import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.parquet as pq

from . import bea

OUTDIR = os.environ.get(
    "US_BEA_OUT", os.path.expanduser("~/Downloads/us_bea_data/output")
)


# ------------------------------------------------------------------ schemas --
def _schema(cols: list[tuple[str, pa.DataType]]) -> pa.Schema:
    return pa.schema([pa.field(n, t) for n, t in cols])


STR = pa.string()
INT = pa.int64()
FLT = pa.float64()
SCHEMAS = {
    "nipa": _schema(
        [
            ("year", INT),
            ("quarter", STR),
            ("month", STR),
            ("frequency", STR),
            ("table_name", STR),
            ("line_number", STR),
            ("series_code", STR),
            ("line_description", STR),
            ("metric_name", STR),
            ("unit", STR),
            ("unit_mult", STR),
            ("value", FLT),
            ("note_ref", STR),
        ]
    ),
    "gdp_by_industry": _schema(
        [
            ("year", INT),
            ("quarter", STR),
            ("frequency", STR),
            ("table_id", STR),
            ("table_description", STR),
            ("industry", STR),
            ("industry_description", STR),
            ("value", FLT),
            ("note_ref", STR),
        ]
    ),
    "regional_state": _schema(
        [
            ("year", INT),
            ("quarter", STR),
            ("frequency", STR),
            ("geo_fips", STR),
            ("id_state", STR),
            ("geo_name", STR),
            ("table_name", STR),
            ("line_code", STR),
            ("series_code", STR),
            ("line_description", STR),
            ("unit", STR),
            ("unit_mult", STR),
            ("value", FLT),
            ("note_ref", STR),
        ]
    ),
    "regional_county": _schema(
        [
            ("year", INT),
            ("geo_fips", STR),
            ("id_county", STR),
            ("id_state", STR),
            ("geo_name", STR),
            ("table_name", STR),
            ("line_code", STR),
            ("series_code", STR),
            ("line_description", STR),
            ("unit", STR),
            ("unit_mult", STR),
            ("value", FLT),
            ("note_ref", STR),
        ]
    ),
    "regional_metro": _schema(
        [
            ("year", INT),
            ("geo_fips", STR),
            ("id_cbsa", STR),
            ("geo_name", STR),
            ("table_name", STR),
            ("line_code", STR),
            ("series_code", STR),
            ("line_description", STR),
            ("unit", STR),
            ("unit_mult", STR),
            ("value", FLT),
            ("note_ref", STR),
        ]
    ),
}


# ------------------------------------------------------------------ helpers --
def _freq(year, quarter, month):
    return "M" if month else ("Q" if quarter else "A")


_REGION_PREFIX = {"91", "92", "93", "94", "95", "96", "97", "98"}


def _id_state(geo_fips: str):
    """NN000 -> NN for real states/territories; None for US(00000) and BEA regions."""
    if not geo_fips or len(geo_fips) != 5 or not geo_fips.endswith("000"):
        return None
    st = geo_fips[:2]
    if st == "00" or st in _REGION_PREFIX:
        return None
    return st


def _strip_tag(desc: str) -> str:
    # "[SAGDP2] Gross domestic product" -> "Gross domestic product"
    return re.sub(r"^\[[^\]]*\]\s*", "", desc or "").strip()


def _line_desc_map(dataset: str, table: str) -> dict:
    out = {}
    for x in bea.param_values_filtered(dataset, "LineCode", TableName=table):
        if x["key"] is not None:
            out[x["key"]] = _strip_tag(x["desc"])
    return out


# --------------------------------------------------------------- row builders --
def _rows_nipa(api_rows):
    for r in api_rows:
        y, q, m = bea.split_time_period(r.get("TimePeriod"))
        if y is None:
            continue
        yield {
            "year": y,
            "quarter": q,
            "month": m,
            "frequency": _freq(y, q, m),
            "table_name": r.get("TableName"),
            "line_number": r.get("LineNumber"),
            "series_code": r.get("SeriesCode"),
            "line_description": r.get("LineDescription"),
            "metric_name": r.get("METRIC_NAME"),
            "unit": r.get("CL_UNIT"),
            "unit_mult": r.get("UNIT_MULT"),
            "value": bea.clean_value(r.get("DataValue")),
            "note_ref": r.get("NoteRef"),
        }


def _rows_gi(api_rows, table_desc):
    for r in api_rows:
        try:
            y = int(r.get("Year"))
        except (TypeError, ValueError):
            continue
        freq = r.get("Frequency")
        yield {
            "year": y,
            "quarter": bea.norm_gi_quarter(freq, r.get("Quarter")),
            "frequency": freq,
            "table_id": str(r.get("TableID")),
            "table_description": table_desc,
            "industry": r.get("Industry"),
            "industry_description": r.get("IndustrYDescription"),
            "value": bea.clean_value(r.get("DataValue")),
            "note_ref": r.get("NoteRef"),
        }


def _rows_regional(api_rows, table, line_code, line_desc, level):
    for r in api_rows:
        y, q, _ = bea.split_time_period(r.get("TimePeriod"))
        if y is None:
            continue
        gf = r.get("GeoFips")
        base = {
            "year": y,
            "geo_fips": gf,
            "geo_name": r.get("GeoName"),
            "table_name": table,
            "line_code": line_code,
            "series_code": r.get("Code"),
            "line_description": line_desc,
            "unit": r.get("CL_UNIT"),
            "unit_mult": r.get("UNIT_MULT"),
            "value": bea.clean_value(r.get("DataValue")),
            "note_ref": r.get("NoteRef"),
        }
        if level == "state":
            base.update(
                quarter=q, frequency=_freq(y, q, None), id_state=_id_state(gf)
            )
        elif level == "county":
            base.update(
                id_county=gf,
                id_state=(gf[:2] if gf and len(gf) == 5 else None),
            )
        elif level == "metro":
            base.update(id_cbsa=gf)
        yield base


# ----------------------------------------------------------------- writers ---
FLUSH_ROWS = 500_000


class Writer:
    """Buffers rows for one db-table, flushing in chunks to bound memory and
    keep the part-file count reasonable (county would otherwise emit ~20k files).
    `tag` (the source BEA table) is embedded in each part-file name so a partial
    BEA table can be purged and re-run on resume."""

    def __init__(self, table: str):
        self.table = table
        self.buf: list[dict] = []
        self.total = 0
        self.tag = "x"

    def add(self, rows):
        self.buf.extend(rows)
        if len(self.buf) >= FLUSH_ROWS:
            self.flush()

    def flush(self):
        if not self.buf:
            return
        tbl = pa.Table.from_pylist(self.buf, schema=SCHEMAS[self.table])
        ds.write_dataset(
            tbl,
            os.path.join(OUTDIR, self.table),
            format="parquet",
            partitioning=["year"],
            partitioning_flavor="hive",
            existing_data_behavior="overwrite_or_ignore",
            basename_template=f"part-{self.tag}-{int(time.time() * 1000) % 10**9}-{{i}}.parquet",
        )
        self.total += len(self.buf)
        self.buf = []


def _purge_tag(table: str, tag: str):
    import glob

    for f in glob.glob(
        os.path.join(OUTDIR, table, "**", f"part-{tag}-*.parquet"),
        recursive=True,
    ):
        os.remove(f)


def _progress_path(table):
    return os.path.join(OUTDIR, ".progress", f"{table}.json")


def _progress_load(table):
    p = _progress_path(table)
    return set(json.load(open(p))) if os.path.exists(p) else set()


def _progress_add(table, tag):
    os.makedirs(os.path.dirname(_progress_path(table)), exist_ok=True)
    done = _progress_load(table)
    done.add(tag)
    with open(_progress_path(table), "w") as fh:
        json.dump(sorted(done), fh)


def _reset(table: str):
    shutil.rmtree(os.path.join(OUTDIR, table), ignore_errors=True)
    for p in (os.path.join(OUTDIR, ".done", table), _progress_path(table)):
        if os.path.exists(p):
            os.remove(p)


def _mark_done(table: str, n: int):
    d = os.path.join(OUTDIR, ".done")
    os.makedirs(d, exist_ok=True)
    with open(os.path.join(d, table), "w") as f:
        f.write(str(n))


def _is_done(table: str) -> bool:
    return os.path.exists(os.path.join(OUTDIR, ".done", table))


# ------------------------------------------------------------- table runners --
def _regional_tables(prefixes) -> list[str]:
    tabs = [x["key"] for x in bea.param_values("Regional", "TableName")]
    return [t for t in tabs if any(t.startswith(p) for p in prefixes)]


def run_nipa():
    tabs = [x["key"] for x in bea.param_values("NIPA", "TableName")]
    done = _progress_load("nipa")
    print(f"[nipa] {len(tabs)} tables ({len(done)} already done)")
    w = Writer("nipa")
    for i, t in enumerate(tabs, 1):
        if t in done:
            continue
        _purge_tag("nipa", t)
        w.tag = t
        try:
            w.add(
                _rows_nipa(
                    bea.get_data(
                        "NIPA", TableName=t, Frequency="A,Q,M", Year="ALL"
                    )
                )
            )
        except bea.BEAError as e:
            print(f"  [nipa] {t} skipped: {e.desc}")
            continue
        w.flush()
        _progress_add("nipa", t)
        if i % 25 == 0:
            print(f"  [nipa] {i}/{len(tabs)} tables, {w.total:,} rows")
    w.flush()
    _mark_done("nipa", w.total)
    print(f"[nipa] done: {w.total:,} rows")


def run_gdp_by_industry():
    tids = bea.param_values("GDPbyIndustry", "TableID")
    desc = {x["key"]: (x["desc"] or "").strip() for x in tids}
    done = _progress_load("gdp_by_industry")
    print(f"[gdp_by_industry] {len(tids)} tables")
    w = Writer("gdp_by_industry")
    for x in tids:
        tid = x["key"]
        if tid in done:
            continue
        _purge_tag("gdp_by_industry", tid)
        w.tag = tid
        for freq in ("A", "Q"):
            try:
                w.add(
                    _rows_gi(
                        bea.get_data(
                            "GDPbyIndustry",
                            TableID=tid,
                            Industry="ALL",
                            Frequency=freq,
                            Year="ALL",
                        ),
                        desc.get(tid, ""),
                    )
                )
            except bea.BEAError as e:
                print(f"  [gi] T{tid} {freq} skipped: {e.desc}")
                continue
        w.flush()
        _progress_add("gdp_by_industry", tid)
    w.flush()
    _mark_done("gdp_by_industry", w.total)
    print(f"[gdp_by_industry] done: {w.total:,} rows")


def _run_regional(table_key, prefixes, geofips, level):
    tabs = _regional_tables(prefixes)
    done = _progress_load(table_key)
    print(
        f"[{table_key}] {len(tabs)} BEA tables ({','.join(prefixes)}) @ GeoFips={geofips} "
        f"({len(done)} already done)"
    )
    w = Writer(table_key)
    for ti, t in enumerate(tabs, 1):
        if t in done:
            print(f"  [{table_key}] {ti}/{len(tabs)} {t}: skip (done)")
            continue
        _purge_tag(table_key, t)
        w.tag = t
        ldesc = _line_desc_map("Regional", t)
        codes = list(ldesc.keys())
        before = w.total + len(w.buf)
        for lc in codes:
            try:
                api = bea.get_data(
                    "Regional",
                    TableName=t,
                    LineCode=lc,
                    GeoFips=geofips,
                    Year="ALL",
                )
            except bea.BEAError as e:
                if e.code in ("204", "100"):  # no data / invalid line
                    continue
                print(f"  [{table_key}] {t} line {lc}: {e.desc}")
                continue
            w.add(_rows_regional(api, t, lc, ldesc.get(lc, ""), level))
        got = w.total + len(w.buf) - before
        w.flush()
        _progress_add(table_key, t)
        print(
            f"  [{table_key}] {ti}/{len(tabs)} {t}: {got:,} rows ({len(codes)} lines)"
        )
    w.flush()
    _mark_done(table_key, w.total)
    print(f"[{table_key}] done: {w.total:,} rows")


def run_regional_state():
    _run_regional("regional_state", ("SA", "SQ", "PR", "TA"), "STATE", "state")


def run_regional_county():
    _run_regional("regional_county", ("CA",), "COUNTY", "county")


def run_regional_metro():
    _run_regional("regional_metro", ("MA",), "MSA", "metro")


_FREQ_LABELS = {"A": "Annual", "Q": "Quarterly", "M": "Monthly"}


def run_dicionario():
    """Code->label maps for the dictionary-covered columns across all tables."""
    rows: list[dict] = []

    def add(id_tabela, nome_coluna, chave, valor):
        rows.append(
            {
                "id_tabela": id_tabela,
                "nome_coluna": nome_coluna,
                "chave": str(chave),
                "cobertura_temporal": "",
                "valor": valor,
            }
        )

    # table_name -> description, per db table
    nipa_tabs = bea.param_values("NIPA", "TableName")
    for x in nipa_tabs:
        add("nipa", "table_id", x["key"], x["desc"])
    reg_tabs = {
        x["key"]: x["desc"] for x in bea.param_values("Regional", "TableName")
    }
    fam = {
        "regional_state": ("SA", "SQ", "PR", "TA"),
        "regional_county": ("CA",),
        "regional_metro": ("MA",),
    }
    for dbt_tbl, prefs in fam.items():
        for code, desc in reg_tabs.items():
            if any(code.startswith(p) for p in prefs):
                add(dbt_tbl, "table_id", code, desc)
    # gdp_by_industry: table_id + industry
    for x in bea.param_values("GDPbyIndustry", "TableID"):
        add("gdp_by_industry", "table_id", x["key"], x["desc"])
    for x in bea.param_values("GDPbyIndustry", "Industry"):
        add("gdp_by_industry", "industry", x["key"], x["desc"])
    # frequency labels, per table that has the column
    for dbt_tbl, freqs in {
        "nipa": "AQM",
        "gdp_by_industry": "AQ",
        "regional_state": "AQ",
    }.items():
        for f in freqs:
            add(dbt_tbl, "frequency", f, _FREQ_LABELS[f])

    outdir = os.path.join(OUTDIR, "dicionario")
    os.makedirs(outdir, exist_ok=True)
    tbl = pa.Table.from_pylist(rows, schema=SCHEMAS["dicionario"])
    pq.write_table(tbl, os.path.join(outdir, "dicionario.parquet"))
    _mark_done("dicionario", len(rows))
    print(f"[dicionario] done: {len(rows):,} rows")


SCHEMAS["dicionario"] = _schema(
    [
        ("id_tabela", STR),
        ("nome_coluna", STR),
        ("chave", STR),
        ("cobertura_temporal", STR),
        ("valor", STR),
    ]
)

RUNNERS = {
    "nipa": run_nipa,
    "gdp_by_industry": run_gdp_by_industry,
    "regional_state": run_regional_state,
    "regional_county": run_regional_county,
    "regional_metro": run_regional_metro,
    "dicionario": run_dicionario,
}


def main(argv):
    tables = argv or list(RUNNERS.keys())
    for t in tables:
        if t not in RUNNERS:
            print(f"unknown table {t}; choices: {list(RUNNERS)}")
            continue
        if _is_done(t):
            print(f"[{t}] already done (skip); use RESET=1 to redo")
            continue
        # no reset here: the runner resumes from .progress (per-BEA-table checkpoints).
        RUNNERS[t]()


if __name__ == "__main__":
    if os.environ.get("RESET"):
        for t in sys.argv[1:] or list(RUNNERS):
            _reset(t)
    main(sys.argv[1:])
