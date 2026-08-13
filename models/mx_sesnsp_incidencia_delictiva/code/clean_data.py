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

import logging
import os
import sys
from pathlib import Path

import pandas as pd

# Shared wide→long melt lives in the pipeline utils (DRY): the recurring Prefect
# pipeline and this one-shot bootstrap must not drift apart.
from pipelines.datasets.mx_sesnsp_incidencia_delictiva.utils import (
    MONTHS,  # noqa: F401 (re-exported for callers/tests of this module)
    melt_wide,
    write_partition,
)

DATA = Path(
    os.environ.get(
        "MX_SESNSP_DATA",
        Path.home() / "Downloads" / "mx_sesnsp_incidencia_delictiva_data",
    )
)
INPUT = DATA / "input"
OUTPUT = DATA / "output"

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s"
)
log = logging.getLogger("sesnsp")

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


def find_csv(table):
    hits = sorted((INPUT / f"unz_{table}").glob("*.csv"))
    if not hits:
        raise FileNotFoundError(f"no CSV under {INPUT}/unz_{table}")
    return hits[0]


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
        n = write_partition(long, table, y, OUTPUT)
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
