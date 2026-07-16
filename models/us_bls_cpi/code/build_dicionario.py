#!/usr/bin/env python3
"""
Build the us_bls_cpi `dicionario` table from the BLS dimension files.

Emits one row per (id_tabela, nome_coluna, chave, valor) for every coded column
(survey, seasonal_adjustment, area_id, item_id) across the three data tables.
Area/item labels are the union of the CPI-U and CPI-W dimension files (CPI-U wins
on overlap). Writes output/dicionario/data.parquet (flat, not partitioned).

Usage:
    uv run python models/us_bls_cpi/code/build_dicionario.py
"""

import logging
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

ROOT = Path(__file__).resolve().parents[1]
INPUT = ROOT / "input"
OUTPUT = ROOT / "output"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("dicionario")

DATA_TABLES = ["monthly", "annual", "semiannual"]
SCHEMA = pa.schema(
    [
        pa.field("id_tabela", pa.string()),
        pa.field("nome_coluna", pa.string()),
        pa.field("chave", pa.string()),
        pa.field("cobertura_temporal", pa.string()),
        pa.field("valor", pa.string()),
    ]
)

SURVEY = {
    "CPI-U": "All Urban Consumers",
    "CPI-W": "Urban Wage Earners and Clerical Workers",
}
SEASONAL = {"S": "Seasonally Adjusted", "U": "Not Seasonally Adjusted"}


def dim_map(fname, code_col, name_col):
    """Union CPI-U and CPI-W dimension files; CPI-U label wins on overlap."""
    out = {}
    for s in ("cw", "cu"):  # cu last so it overrides cw
        df = pd.read_csv(
            INPUT / s / f"{s}.{fname}", sep="\t", dtype=str, na_filter=False
        )
        df.columns = [c.strip() for c in df.columns]
        for _, r in df.iterrows():
            out[r[code_col].strip()] = r[name_col].strip()
    return out


def main():
    maps = {
        "survey": SURVEY,
        "seasonal_adjustment": SEASONAL,
        "area_id": dim_map("area", "area_code", "area_name"),
        "item_id": dim_map("item", "item_code", "item_name"),
    }
    rows = []
    for tbl in DATA_TABLES:
        for col, mapping in maps.items():
            for key, label in mapping.items():
                rows.append((tbl, col, key, None, label))
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
    pdir = OUTPUT / "dicionario"
    pdir.mkdir(parents=True, exist_ok=True)
    pq.write_table(
        pa.Table.from_pandas(df, schema=SCHEMA, preserve_index=False),
        pdir / "data.parquet",
        compression="snappy",
    )
    log.info(
        f"dicionario: {len(df):,} rows "
        f"({len(maps['area_id'])} areas, {len(maps['item_id'])} items) -> output/dicionario/"
    )


if __name__ == "__main__":
    main()
