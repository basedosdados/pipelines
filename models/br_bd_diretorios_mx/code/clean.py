#!/usr/bin/env python3
"""Build br_bd_diretorios_mx (estado, municipio) from the INEGI AGEEML web service.

Source: https://gaia.inegi.org.mx/wscatgeo/v2/  (Catálogo Único de Claves, no key)
  - entidades: /mgee/            -> cvegeo, cve_ent, nomgeo, nom_abrev, ...
  - municipios: /mgem/{cve_ent}  -> cvegeo (5-digit), cve_ent, cve_mun, nomgeo, nom_cab, ...

Adds an EE999 "No especificado" sentinel municipio per entidad so the SESNSP crime
tables' id_municipio FK resolves (SESNSP encodes unknown municipality as 999).

Output: all-STRING Snappy parquet (unpartitioned; small static catalogs) under
~/Downloads/br_bd_diretorios_mx_data/output/<table>/data.parquet. dbt safe_casts.

Usage:
    uv run --with pandas --with pyarrow --with requests python \
        models/br_bd_diretorios_mx/code/clean.py
"""

import json
import logging
import os
import time
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import requests

DATA = Path(
    os.environ.get(
        "MX_DIR_DATA", Path.home() / "Downloads" / "br_bd_diretorios_mx_data"
    )
)
RAW = DATA / "input"
OUTPUT = DATA / "output"
ARCH = Path(__file__).resolve().parent / "architecture"
BASE = "https://gaia.inegi.org.mx/wscatgeo/v2"
UA = {"User-Agent": "Mozilla/5.0 (DataBasis onboarding)"}

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s"
)
log = logging.getLogger("mx_dir")


def arch_order(table):
    import csv

    with open(ARCH / f"{table}.csv", newline="") as fh:
        return [r["name"] for r in csv.DictReader(fh)]


def fetch(path):
    """GET a wscatgeo endpoint, cache raw JSON to scratch, return list of dicts."""
    RAW.mkdir(parents=True, exist_ok=True)
    cache = RAW / (path.strip("/").replace("/", "_") + ".json")
    if cache.exists():
        return json.loads(cache.read_text())["datos"]
    for attempt in range(4):
        r = requests.get(f"{BASE}/{path.strip('/')}/", headers=UA, timeout=60)
        if r.status_code == 200:
            cache.write_text(r.text)
            return r.json()["datos"]
        time.sleep(2 * (attempt + 1))
    r.raise_for_status()


def write_parquet(df, table):
    order = arch_order(table)
    missing = [c for c in order if c not in df.columns]
    if missing:
        raise ValueError(f"{table}: missing {missing}")
    out = df[order].astype("object").where(pd.notna(df[order]), None)
    schema = pa.schema([pa.field(c, pa.string()) for c in order])
    at = pa.Table.from_pandas(
        out.astype(str).where(out.notna(), None),
        schema=schema,
        preserve_index=False,
    )
    tdir = OUTPUT / table
    tdir.mkdir(parents=True, exist_ok=True)
    pq.write_table(at, tdir / "data.parquet", compression="snappy")
    log.info("%s: wrote %s rows", table, f"{at.num_rows:,}")


def build_estado():
    rows = fetch("mgee")
    df = pd.DataFrame(rows)
    df = df.rename(
        columns={
            "cve_ent": "id_estado",
            "nomgeo": "nombre",
            "nom_abrev": "abreviatura",
        }
    )
    df["id_estado"] = df["id_estado"].str.zfill(2)
    df = df.sort_values("id_estado").drop_duplicates("id_estado")
    write_parquet(df, "estado")
    return sorted(df["id_estado"].tolist())


def build_municipio(entidades):
    frames = []
    for ent in entidades:
        rows = fetch(f"mgem/{ent}")
        frames.append(pd.DataFrame(rows))
    df = pd.concat(frames, ignore_index=True)
    df = df.rename(
        columns={
            "cvegeo": "id_municipio",
            "cve_ent": "id_estado",
            "nomgeo": "nombre",
        }
    )
    df["id_municipio"] = df["id_municipio"].str.zfill(5)
    df["id_estado"] = df["id_estado"].str.zfill(2)
    df = df.sort_values("id_municipio").drop_duplicates("id_municipio")
    # Pure INEGI catalog: no sentinel rows. SESNSP's non-georeferenced aggregate
    # codes (municipio suffix 998/999, "No especificado"/"Otros municipios") are
    # excluded from the crime tables' FK tests via an ignore-where, not injected here.
    write_parquet(df, "municipio")
    log.info(
        "municipio: %s real municipios (pure INEGI, no sentinels)", len(df)
    )


if __name__ == "__main__":
    ents = build_estado()
    build_municipio(ents)
    print("done")
