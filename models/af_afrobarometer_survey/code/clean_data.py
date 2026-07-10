#!/usr/bin/env python3
"""Clean the nine Afrobarometer merged-round SPSS files into partitioned-free
Parquet, and build the cross-round `dicionario` table.

For each round it:
  1. downloads the merged .sav into input/ (cached),
  2. reads it with pyreadstat (user_missing=True, so coded values such as
     998 'Refused' / 999 "Don't know" are preserved, not blanked),
  3. keeps every original variable (lowercased, BQ-safe name) with its raw
     coded value, casting to INT64 / FLOAT64 / STRING / DATE from the SPSS
     print format,
  4. prepends a derived `country_iso3_code` (from the COUNTRY value label),
  5. writes output/<slug>/data.parquet,
  6. records column metadata to code/meta_cache.json.

The `dicionario` table is assembled from every variable's value labels across
all rounds: (id_tabela, nome_coluna, chave, cobertura_temporal, valor).

Usage:
    .venv/bin/python models/af_afrobarometer_survey/code/clean_data.py [round1 ...]
"""

from __future__ import annotations

import datetime as dt
import json
import logging
import math
import sys
import urllib.request
from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

sys.path.insert(0, str(Path(__file__).resolve().parent))
import pyreadstat
from common import (
    ARCH_DIR,
    INPUT_DIR,
    META_CACHE,
    OUTPUT_DIR,
    ROUNDS,
    clean_name,
    country_to_iso3,
    infer_bq_type,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("clean")

# Nominal fielding years, used only if a round lacks a parseable interview date.
FALLBACK_YEARS = {
    1: (1999, 2001),
    2: (2002, 2004),
    3: (2005, 2006),
    4: (2008, 2009),
    5: (2011, 2013),
    6: (2014, 2015),
    7: (2016, 2018),
    8: (2019, 2021),
    9: (2021, 2023),
}


def _demojibake(s):
    """Repair UTF-8 bytes that were decoded as latin1 ('SÃ£o' -> 'São').
    Non-strings and genuinely-latin1 strings pass through unchanged."""
    if not isinstance(s, str):
        return s
    try:
        return s.encode("latin-1").decode("utf-8")
    except (UnicodeEncodeError, UnicodeDecodeError):
        return s


def read_sav_robust(path: str):
    """Read a .sav, falling back through encodings when the file's declared
    encoding mis-decodes a string cell (Round 6 stores UTF-8 bytes under a
    declared encoding readstat cannot iconv). Only latin1 reads every byte;
    the resulting mojibake is then repaired back to UTF-8."""
    last = None
    for enc in (None, "latin1"):
        try:
            kw = {"user_missing": True}
            if enc:
                kw["encoding"] = enc
            df, meta = pyreadstat.read_sav(path, **kw)
            if enc == "latin1":
                log.info("  (read with encoding=latin1; repairing UTF-8)")
                for c in df.columns:
                    if df[c].dtype == object:
                        df[c] = df[c].map(_demojibake)
                meta.column_names_to_labels = {
                    k: _demojibake(v)
                    for k, v in meta.column_names_to_labels.items()
                }
                meta.variable_value_labels = {
                    var: {code: _demojibake(lab) for code, lab in m.items()}
                    for var, m in meta.variable_value_labels.items()
                }
            return df, meta
        except pyreadstat.ReadstatError as e:
            last = e
    raise last


def download(url: str, dest: Path) -> None:
    if dest.exists() and dest.stat().st_size > 0:
        return
    INPUT_DIR.mkdir(parents=True, exist_ok=True)
    log.info(f"  downloading {url}")
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
    with urllib.request.urlopen(req, timeout=300) as r, open(dest, "wb") as f:
        f.write(r.read())


def _is_nan(v) -> bool:
    return v is None or (isinstance(v, float) and math.isnan(v))


# SPSS encodes some missing interview dates as the calendar epoch
# (1582-10-14) or as zeros; treat anything outside a plausible window as null.
_DATE_MIN = dt.date(1995, 1, 1)
_DATE_MAX = dt.date(2035, 1, 1)


def _sane_date(d: dt.date):
    return d if _DATE_MIN <= d <= _DATE_MAX else None


def to_arrow(series: pd.Series, bqtype: str, is_time: bool) -> pa.Array:
    """Build a typed Arrow array with NaN/None -> null."""
    if bqtype == "STRING" and is_time:
        vals = [
            None
            if _is_nan(v)
            else v.strftime("%H:%M:%S")
            if isinstance(v, dt.time)
            else str(v)
            for v in series
        ]
        return pa.array(vals, type=pa.string())
    if bqtype == "STRING":
        vals = [None if _is_nan(v) else str(v) for v in series]
        return pa.array(vals, type=pa.string())
    if bqtype == "DATE":
        vals = []
        for v in series:
            if _is_nan(v):
                vals.append(None)
            elif isinstance(v, dt.datetime):
                vals.append(_sane_date(v.date()))
            elif isinstance(v, dt.date):
                vals.append(_sane_date(v))
            else:
                vals.append(None)
        return pa.array(vals, type=pa.date32())
    if bqtype == "INT64":
        num = pd.to_numeric(series, errors="coerce").round().astype("Int64")
        return pa.array(num, type=pa.int64(), from_pandas=True)
    # FLOAT64
    num = pd.to_numeric(series, errors="coerce").astype("float64")
    return pa.array(num, type=pa.float64(), from_pandas=True)


def find_var(meta, target: str) -> str | None:
    """Original variable name whose lowercased name equals `target`."""
    for name in meta.column_names:
        if name.lower() == target:
            return name
    return None


def coverage_from_dates(df: pd.Series | None, num: int) -> tuple[int, int]:
    if df is not None:
        years = [
            v.year
            for v in df
            if isinstance(v, dt.date) and 1995 <= v.year <= 2035
        ]
        if years:
            return min(years), max(years)
    return FALLBACK_YEARS[num]


def process_round(rnd: dict) -> dict:
    num, slug = rnd["num"], rnd["slug"]
    log.info(f"=== {slug} (round {num}) ===")
    path = INPUT_DIR / rnd["filename"]
    download(rnd["url"], path)

    df, meta = read_sav_robust(str(path))
    fmts = meta.original_variable_types
    labels = meta.column_names_to_labels
    vlabels = meta.variable_value_labels
    n_rows = len(df)
    log.info(f"  read {n_rows} rows x {len(df.columns)} cols")

    # --- derive country_iso3_code -------------------------------------------
    country_var = find_var(meta, "country")
    if country_var is None:
        raise RuntimeError(f"{slug}: no COUNTRY variable found")
    cmap = vlabels.get(country_var, {})
    unmapped = sorted(
        {lab for code, lab in cmap.items() if country_to_iso3(lab) is None}
    )
    if unmapped:
        raise RuntimeError(f"{slug}: unmapped country labels: {unmapped}")
    code_to_iso = {code: country_to_iso3(lab) for code, lab in cmap.items()}
    iso3 = [
        code_to_iso.get(v) if not _is_nan(v) else None for v in df[country_var]
    ]

    # --- coverage from interview date ---------------------------------------
    date_var = find_var(meta, "dateintr")
    date_series = df[date_var] if date_var else None
    cov_start, cov_end = coverage_from_dates(date_series, num)
    log.info(f"  coverage {cov_start}-{cov_end}; country ISO3 mapped OK")

    # --- build columns ------------------------------------------------------
    arrays = {"country_iso3_code": pa.array(iso3, type=pa.string())}
    col_meta = [
        {
            "name": "country_iso3_code",
            "original_name": "",
            "bigquery_type": "STRING",
            "description": "Country ISO3 code (derived from COUNTRY).",
            "covered_by_dictionary": "no",
            "directory_column": "br_bd_diretorios_mundo.pais:sigla_iso3",
        }
    ]
    seen = {"country_iso3_code"}
    round_vlabels: dict[str, dict] = {}

    for orig in meta.column_names:
        name = clean_name(orig)
        if name in seen:  # collision after sanitisation
            k = 2
            while f"{name}_{k}" in seen:
                k += 1
            name = f"{name}_{k}"
        seen.add(name)
        fmt = fmts.get(orig, "")
        s = df[orig]
        is_str = fmt.upper().startswith("A")
        bqtype = infer_bq_type(fmt, _integral(s) if not is_str else False)
        is_time = bqtype == "STRING" and fmt.upper().startswith(
            ("TIME", "DTIME")
        )
        # safety: a date-format column that pyreadstat did not convert to date
        if bqtype == "DATE" and not _has_dates(s):
            bqtype = infer_bq_type("F8.2", _integral(s))
        arrays[name] = to_arrow(s, bqtype, is_time)
        has_vl = orig in vlabels
        col_meta.append(
            {
                "name": name,
                "original_name": orig,
                "bigquery_type": bqtype,
                "description": (labels.get(orig) or "").strip(),
                "covered_by_dictionary": "yes" if has_vl else "no",
                "directory_column": "",
            }
        )
        if has_vl:
            round_vlabels[name] = vlabels[orig]

    table = pa.table(arrays)
    out_dir = OUTPUT_DIR / slug
    out_dir.mkdir(parents=True, exist_ok=True)
    pq.write_table(table, out_dir / "data.parquet", compression="snappy")
    log.info(
        f"  wrote {out_dir / 'data.parquet'} ({table.num_rows} rows, "
        f"{table.num_columns} cols)"
    )

    return {
        "slug": slug,
        "num": num,
        "rows": n_rows,
        "coverage": [cov_start, cov_end],
        "columns": col_meta,
        "value_labels": round_vlabels,
    }


def _integral(s: pd.Series) -> bool:
    num = pd.to_numeric(s, errors="coerce").to_numpy(dtype="float64")
    nn = num[~np.isnan(num)]
    if nn.size == 0:
        return True
    return bool(np.all(np.mod(nn, 1) == 0))


def _has_dates(s: pd.Series) -> bool:
    return any(isinstance(v, dt.date) for v in s.head(200))


def cov_str(start: int, end: int) -> str:
    return str(start) if start == end else f"{start}(1){end}"


def build_dicionario(rounds_meta: list[dict]) -> None:
    """Assemble the cross-round dicionario from all value labels."""
    rows = []
    for rm in rounds_meta:
        cov = cov_str(*rm["coverage"])
        for col, mapping in rm["value_labels"].items():
            for code, label in mapping.items():
                key = (
                    str(int(code))
                    if isinstance(code, float) and float(code).is_integer()
                    else str(code)
                )
                rows.append(
                    {
                        "id_tabela": rm["slug"],
                        "nome_coluna": col,
                        "chave": key,
                        "cobertura_temporal": cov,
                        "valor": "" if label is None else str(label),
                    }
                )
    tbl = pa.table(
        {
            "id_tabela": pa.array([r["id_tabela"] for r in rows], pa.string()),
            "nome_coluna": pa.array(
                [r["nome_coluna"] for r in rows], pa.string()
            ),
            "chave": pa.array([r["chave"] for r in rows], pa.string()),
            "cobertura_temporal": pa.array(
                [r["cobertura_temporal"] for r in rows], pa.string()
            ),
            "valor": pa.array([r["valor"] for r in rows], pa.string()),
        }
    )
    out_dir = OUTPUT_DIR / "dicionario"
    out_dir.mkdir(parents=True, exist_ok=True)
    pq.write_table(tbl, out_dir / "data.parquet", compression="snappy")
    log.info(
        f"  dicionario: {tbl.num_rows} rows across {len(rounds_meta)} rounds"
    )


def main():
    ARCH_DIR.mkdir(parents=True, exist_ok=True)
    wanted = set(sys.argv[1:])
    rounds = [r for r in ROUNDS if not wanted or r["slug"] in wanted]

    # merge into existing cache so partial runs accumulate
    cache = {}
    if META_CACHE.exists():
        cache = json.loads(META_CACHE.read_text())

    for rnd in rounds:
        rm = process_round(rnd)
        cache[rm["slug"]] = rm
        META_CACHE.write_text(json.dumps(cache, ensure_ascii=False, indent=1))

    # (re)build dicionario from all cached rounds, in round order
    ordered = [cache[r["slug"]] for r in ROUNDS if r["slug"] in cache]
    build_dicionario(ordered)

    print("\n| table | rows | cols | coverage |")
    print("|-------|------|------|----------|")
    for rm in ordered:
        print(
            f"| {rm['slug']} | {rm['rows']} | {len(rm['columns'])} | "
            f"{cov_str(*rm['coverage'])} |"
        )


if __name__ == "__main__":
    main()
