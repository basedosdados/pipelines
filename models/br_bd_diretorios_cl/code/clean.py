#!/usr/bin/env python3
"""Build br_bd_diretorios_cl (region, provincia, comuna) from the INE Censo 2024 DPA.

Source: Instituto Nacional de Estadísticas de Chile, Censo de Población y Vivienda
2024, sheet "DPA" of the variable dictionary published in the public download
bucket (CC BY-SA 4.0):

    https://storage.googleapis.com/bktdescargascenso2024/Datos_agregados/
        diccionario_variables_glosas_censo2024.xlsx

That sheet is the whole división político-administrativa: 16 regiones, 56
provincias and 346 comunas, each with its código único territorial (CUT) and its
name in upper case.

Two transformations are applied and both are recorded in the architecture
`observations`:

1.  Names are title-cased with Spanish orthography (particles "de", "del", "la",
    "las", "los", "y", "e", "el" stay lower case unless they open the name).
    `--validate` checks this against the official spellings in Decreto Supremo
    N° 1439 as consolidated by SUBDERE; the derivation reproduces all 346.
2.  The region table gains three name variants that the INE sheet does not carry:
    `nombre_completo` (official name with its article), `sigla` (ISO 3166-2:CL)
    and `numero_romano` (the ordinal abolished by Ley N° 21.074 of 2018, kept for
    joining older sources).

Output: all-STRING Snappy parquet, unpartitioned (three small static catalogs),
at ~/Downloads/br_bd_diretorios_cl_data/output/<table>/data.parquet. The dbt
models safe_cast each column to its architecture type.

Usage:
    uv run --with pandas --with pyarrow --with openpyxl --with requests python \
        models/br_bd_diretorios_cl/code/clean.py [--validate]
"""

import csv
import logging
import os
import re
import sys
import unicodedata
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import requests

DATA = Path(
    os.environ.get(
        "CL_DIR_DATA", Path.home() / "Downloads" / "br_bd_diretorios_cl_data"
    )
)
RAW = DATA / "input"
OUTPUT = DATA / "output"
ARCH = Path(__file__).resolve().parent / "architecture"

DPA_URL = (
    "https://storage.googleapis.com/bktdescargascenso2024/Datos_agregados/"
    "diccionario_variables_glosas_censo2024.xlsx"
)
DPA_FILE = "diccionario_variables_glosas_censo2024.xlsx"
DPA_SHEET = "DPA"

# Decreto Supremo N° 1439 (2000) as consolidated by SUBDERE, used by --validate
# only: it predates the Región de Ñuble, so it is a spelling reference and never
# a source of codes.
SUBDERE_PDF_URL = (
    "https://www.sinim.gov.cl/archivos/centro_descargas/"
    "modificacion_instructivo_pres_codigos.pdf"
)

UA = {"User-Agent": "Mozilla/5.0 (DataBasis onboarding)"}

# Particles that stay lower case unless they open a name.
LOWER_WORDS = {"de", "del", "la", "las", "los", "el", "y", "e"}

# Region name variants absent from the INE sheet. `articulo` builds
# `nombre_completo` as f"Región {articulo} {nombre}"; the Región Metropolitana de
# Santiago takes no article and no roman ordinal.
#   sigla         ISO 3166-2:CL, without the "CL-" prefix
#   numero_romano ordinal abolished by Ley N° 21.074 (2018)
REGION_VARIANTS = {
    "01": {"articulo": "de", "sigla": "TA", "numero_romano": "I"},
    "02": {"articulo": "de", "sigla": "AN", "numero_romano": "II"},
    "03": {"articulo": "de", "sigla": "AT", "numero_romano": "III"},
    "04": {"articulo": "de", "sigla": "CO", "numero_romano": "IV"},
    "05": {"articulo": "de", "sigla": "VS", "numero_romano": "V"},
    "06": {"articulo": "del", "sigla": "LI", "numero_romano": "VI"},
    "07": {"articulo": "del", "sigla": "ML", "numero_romano": "VII"},
    "08": {"articulo": "del", "sigla": "BI", "numero_romano": "VIII"},
    "09": {"articulo": "de", "sigla": "AR", "numero_romano": "IX"},
    "10": {"articulo": "de", "sigla": "LL", "numero_romano": "X"},
    "11": {"articulo": "de", "sigla": "AI", "numero_romano": "XI"},
    "12": {"articulo": "de", "sigla": "MA", "numero_romano": "XII"},
    "13": {"articulo": None, "sigla": "RM", "numero_romano": None},
    "14": {"articulo": "de", "sigla": "LR", "numero_romano": "XIV"},
    "15": {"articulo": "de", "sigla": "AP", "numero_romano": "XV"},
    "16": {"articulo": "de", "sigla": "NB", "numero_romano": "XVI"},
}

EXPECTED = {"region": 16, "provincia": 56, "comuna": 346}

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s"
)
log = logging.getLogger("cl_dir")


def download(url, filename):
    """Fetch `url` into the scratch input dir once, return the local path."""
    RAW.mkdir(parents=True, exist_ok=True)
    dest = RAW / filename
    if dest.exists() and dest.stat().st_size > 0:
        return dest
    log.info("downloading %s", url)
    r = requests.get(url, headers=UA, timeout=300)
    r.raise_for_status()
    dest.write_bytes(r.content)
    return dest


def titlecase(name):
    """Title-case a Spanish place name written in upper case."""
    words = name.split()
    out = []
    for i, word in enumerate(words):
        if i > 0 and word.lower() in LOWER_WORDS:
            out.append(word.lower())
        else:
            out.append(
                "'".join(
                    p[:1].upper() + p[1:].lower() if p else p
                    for p in word.split("'")
                )
            )
    return " ".join(out)


def fold(name):
    """Accent- and punctuation-insensitive key, for name matching only."""
    decomposed = unicodedata.normalize("NFD", name)
    stripped = "".join(
        c for c in decomposed if unicodedata.category(c) != "Mn"
    )
    return re.sub(r"[^A-Z0-9]", "", stripped.upper())


def arch_order(table):
    with open(ARCH / f"{table}.csv", newline="", encoding="utf-8") as fh:
        return [row["name"] for row in csv.DictReader(fh)]


def write_parquet(df, table):
    """Write one all-STRING parquet in architecture column order."""
    order = arch_order(table)
    missing = [c for c in order if c not in df.columns]
    if missing:
        raise ValueError(f"{table}: missing columns {missing}")
    if len(df) != EXPECTED[table]:
        raise ValueError(
            f"{table}: expected {EXPECTED[table]} rows, got {len(df)}"
        )
    out = df[order].astype("object").where(pd.notna(df[order]), None)
    schema = pa.schema([pa.field(c, pa.string()) for c in order])
    table_arrow = pa.Table.from_pandas(
        out, schema=schema, preserve_index=False
    )
    tdir = OUTPUT / table
    tdir.mkdir(parents=True, exist_ok=True)
    pq.write_table(table_arrow, tdir / "data.parquet", compression="snappy")
    log.info("%s: wrote %s rows", table, f"{table_arrow.num_rows:,}")


def read_dpa():
    """Read the INE DPA sheet into a tidy frame with zero-padded CUT codes."""
    path = download(DPA_URL, DPA_FILE)
    df = pd.read_excel(path, sheet_name=DPA_SHEET, dtype=str)
    df = df.dropna(subset=["COD_REGION"])
    for col, width in (
        ("COD_REGION", 2),
        ("COD_PROVINCIA", 3),
        ("CUT", 5),
    ):
        df[col] = df[col].str.strip().str.zfill(width)
    for col in ("REGION", "PROVINCIA", "COMUNA"):
        df[col] = df[col].str.strip()
    log.info("INE DPA: %s comuna rows", f"{len(df):,}")
    return df


def build_region(dpa):
    df = (
        dpa[["COD_REGION", "REGION"]]
        .drop_duplicates()
        .sort_values("COD_REGION")
        .rename(columns={"COD_REGION": "id_region"})
    )
    df["nombre"] = df["REGION"].map(titlecase)
    unknown = set(df["id_region"]) - set(REGION_VARIANTS)
    if unknown:
        raise ValueError(f"region: no name variants for {sorted(unknown)}")
    variants = df["id_region"].map(REGION_VARIANTS)
    df["nombre_completo"] = [
        f"Región {v['articulo']} {n}" if v["articulo"] else f"Región {n}"
        for v, n in zip(variants, df["nombre"], strict=True)
    ]
    df["sigla"] = [v["sigla"] for v in variants]
    df["numero_romano"] = [v["numero_romano"] for v in variants]
    write_parquet(df, "region")
    return df


def build_provincia(dpa):
    df = (
        dpa[["COD_PROVINCIA", "COD_REGION", "PROVINCIA"]]
        .drop_duplicates()
        .sort_values("COD_PROVINCIA")
        .rename(
            columns={
                "COD_PROVINCIA": "id_provincia",
                "COD_REGION": "id_region",
            }
        )
    )
    df["nombre"] = df["PROVINCIA"].map(titlecase)
    write_parquet(df, "provincia")
    return df


def build_comuna(dpa):
    df = (
        dpa[["CUT", "COD_PROVINCIA", "COD_REGION", "COMUNA"]]
        .drop_duplicates()
        .sort_values("CUT")
        .rename(
            columns={
                "CUT": "id_comuna",
                "COD_PROVINCIA": "id_provincia",
                "COD_REGION": "id_region",
            }
        )
    )
    df["nombre"] = df["COMUNA"].map(titlecase)
    write_parquet(df, "comuna")
    return df


def check_hierarchy(region, provincia, comuna):
    """The CUT is hierarchical: assert the codes and the FKs agree."""
    bad = provincia[
        provincia["id_provincia"].str[:2] != provincia["id_region"]
    ]
    if len(bad):
        raise ValueError(f"provincia: CUT prefix mismatch\n{bad}")
    bad = comuna[comuna["id_comuna"].str[:3] != comuna["id_provincia"]]
    if len(bad):
        raise ValueError(f"comuna: CUT prefix mismatch\n{bad}")
    bad = comuna[comuna["id_comuna"].str[:2] != comuna["id_region"]]
    if len(bad):
        raise ValueError(f"comuna: region prefix mismatch\n{bad}")
    orphans = set(provincia["id_region"]) - set(region["id_region"])
    if orphans:
        raise ValueError(f"provincia: regions not in directory {orphans}")
    orphans = set(comuna["id_provincia"]) - set(provincia["id_provincia"])
    if orphans:
        raise ValueError(f"comuna: provincias not in directory {orphans}")
    log.info("hierarchy: CUT prefixes and foreign keys consistent")


def validate_names(comuna):
    """Compare derived names against the official SUBDERE spellings.

    Reference only. The decree predates the Región de Ñuble and uses four older
    orthographies (Paiguano, Coihaique, Aisén, Treguaco), so unmatched names are
    reported rather than treated as errors.
    """
    try:
        import pdfplumber
    except ImportError:
        log.warning("--validate needs pdfplumber; skipping")
        return
    path = download(SUBDERE_PDF_URL, "subdere_cut.pdf")
    with pdfplumber.open(path) as pdf:
        text = "\n".join(p.extract_text() or "" for p in pdf.pages)
    official = {}
    for _code, raw in re.findall(r"\b(\d{5})\s+([A-ZÁÉÍÓÚÑÜ][^\n]*)", text):
        name = re.sub(r"\(\d\)", "", raw)
        name = re.sub(r"\s{2,}.*$", "", name).strip()
        name = re.sub(r"\s+", " ", name)
        if name:
            official.setdefault(fold(name), name)
    log.info("SUBDERE reference: %s comuna names", len(official))
    diffs = unmatched = 0
    for name in comuna["nombre"]:
        ref = official.get(fold(name))
        if ref is None:
            unmatched += 1
            log.info("  no decree entry for %r (newer INE orthography)", name)
        elif ref.replace("\u2019", "'") != name:
            diffs += 1
            log.warning("  derived %r vs decree %r", name, ref)
    log.info(
        "validation: %s comunas, %s spelling differences, %s unmatched",
        len(comuna),
        diffs,
        unmatched,
    )


def main():
    dpa = read_dpa()
    region = build_region(dpa)
    provincia = build_provincia(dpa)
    comuna = build_comuna(dpa)
    check_hierarchy(region, provincia, comuna)
    if "--validate" in sys.argv[1:]:
        validate_names(comuna)
    print("done")


if __name__ == "__main__":
    main()
