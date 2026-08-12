#!/usr/bin/env python3
"""Clean the SESNSP incidencia delictiva files into partitioned parquet.

Reads the wide latin-1 CSVs already extracted under
~/Downloads/mx_sesnsp_incidencia_delictiva_data/input/unz_<table>/*.csv and melts the
12 Spanish month columns (Enero..Diciembre) into long format:
    ano, mes, id_entidad[, id_municipio], bien_juridico_afectado, tipo_delito,
    subtipo_delito, modalidad[, sexo, rango_edad], cantidad

Rules:
  - id_entidad = Clave_Ent zero-padded to 2 digits.
  - id_municipio = "Cve. Municipio" zero-padded to 5 digits (INEGI clave).
  - Melt keeps explicit 0 counts; drops months that are blank (not yet published).
  - Geography *name* columns (Entidad, Municipio) are dropped — they live in
    br_bd_diretorios_mx.
  - Output all-STRING Snappy parquet, hive-partitioned by ano (house convention;
    the dbt model safe_casts every column). Big table melted per-year to cap memory.

Usage:
    uv run --with pandas --with pyarrow python \
        models/mx_sesnsp_incidencia_delictiva/code/clean_data.py [table ...] [--sample N]
"""

import csv
import logging
import os
import sys
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

DATA = Path(
    os.environ.get(
        "MX_SESNSP_DATA",
        Path.home() / "Downloads" / "mx_sesnsp_incidencia_delictiva_data",
    )
)
INPUT = DATA / "input"
OUTPUT = DATA / "output"
ARCH = Path(__file__).resolve().parent / "architecture"

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s"
)
log = logging.getLogger("sesnsp")

MONTHS = {
    "Enero": 1,
    "Febrero": 2,
    "Marzo": 3,
    "Abril": 4,
    "Mayo": 5,
    "Junio": 6,
    "Julio": 7,
    "Agosto": 8,
    "Septiembre": 9,
    "Octubre": 10,
    "Noviembre": 11,
    "Diciembre": 12,
}

# table_slug -> (municipal, victimas)
TABLES = {
    "estatal_delitos": (False, False),
    "estatal_delitos_2015_2025": (False, False),
    "estatal_victimas": (False, True),
    "estatal_victimas_2015_2025": (False, True),
    "municipio_delitos": (True, False),
    "municipio_victimas": (True, True),
    "municipio_delitos_2015_2025": (True, False),
}


def arch_order(table):
    with open(ARCH / f"{table}.csv", newline="") as fh:
        return [r["name"] for r in csv.DictReader(fh)]


def find_csv(table):
    hits = sorted((INPUT / f"unz_{table}").glob("*.csv"))
    if not hits:
        raise FileNotFoundError(f"no CSV under {INPUT}/unz_{table}")
    return hits[0]


def melt_wide(df, muni, victimas):
    """Melt one wide chunk to the long architecture columns."""
    id_map = {
        "Clave_Ent": "id_entidad",
        "Bien jurídico afectado": "bien_juridico_afectado",
        "Tipo de delito": "tipo_delito",
        "Subtipo de delito": "subtipo_delito",
        "Modalidad": "modalidad",
    }
    if muni:
        id_map["Cve. Municipio"] = "id_municipio"
    if victimas:
        id_map["Sexo"] = "sexo"
        id_map["Rango de edad"] = "rango_edad"
    id_cols = list(id_map)
    month_cols = [m for m in MONTHS if m in df.columns]
    long = df.melt(
        id_vars=[*id_cols, "Año"],
        value_vars=month_cols,
        var_name="_mes",
        value_name="cantidad",
    )
    # drop not-yet-published months (blank), keep explicit 0
    long["cantidad"] = long["cantidad"].astype(str).str.strip()
    long = long[long["cantidad"] != ""].copy()
    long = long[long["cantidad"].str.lower() != "nan"].copy()
    long = long.rename(columns=id_map)
    long["ano"] = pd.to_numeric(long["Año"], errors="coerce").astype("Int64")
    long["mes"] = long["_mes"].map(MONTHS).astype("Int64")
    long["id_entidad"] = (
        long["id_entidad"].astype(str).str.strip().str.zfill(2)
    )
    if muni:
        long["id_municipio"] = (
            long["id_municipio"].astype(str).str.strip().str.zfill(5)
        )
    long["cantidad"] = pd.to_numeric(long["cantidad"], errors="coerce").astype(
        "Int64"
    )
    return long


def write_partition(long, table, year, mode_first):
    order = arch_order(table)
    missing = [c for c in order if c not in long.columns]
    if missing:
        raise ValueError(f"{table}: missing {missing}")
    out = long[order].copy()
    schema = pa.schema([pa.field(c, pa.string()) for c in order])
    # to all-STRING, NULL-preserving
    for c in order:
        s = out[c]
        out[c] = s.astype("object").where(s.notna(), None)
        out[c] = out[c].map(lambda v: None if v is None else str(v))
    at = pa.Table.from_pandas(out, schema=schema, preserve_index=False)
    pdir = OUTPUT / table / f"ano={int(year)}"
    pdir.mkdir(parents=True, exist_ok=True)
    pq.write_table(at, pdir / "data.parquet", compression="snappy")
    return at.num_rows


def clean_table(table, sample=None):
    muni, victimas = TABLES[table]
    src = find_csv(table)
    log.info("%s <- %s", table, src.name)
    # read all as string (embedded commas handled by csv quoting); latin-1
    reader = pd.read_csv(
        src, encoding="latin-1", dtype=str, nrows=sample, chunksize=None
    )
    df = reader
    total = 0
    years = sorted(
        pd.to_numeric(df["Año"], errors="coerce").dropna().astype(int).unique()
    )
    for y in years:
        chunk = df[pd.to_numeric(df["Año"], errors="coerce") == y]
        long = melt_wide(chunk, muni, victimas)
        n = write_partition(long, table, y, y == years[0])
        total += n
        log.info("  ano=%s: %s rows", y, f"{n:,}")
    log.info("%s: %s rows across %s year(s)", table, f"{total:,}", len(years))
    return total


def main():
    argv = sys.argv[1:]
    sample = None
    if "--sample" in argv:
        i = argv.index("--sample")
        sample = int(argv[i + 1])
        argv = argv[:i] + argv[i + 2 :]
    unknown = [a for a in argv if a not in TABLES]
    if unknown:
        sys.exit(f"unknown table(s): {unknown}")
    tables = argv or list(TABLES)
    for t in tables:
        clean_table(t, sample=sample)
    print("done")


if __name__ == "__main__":
    main()
