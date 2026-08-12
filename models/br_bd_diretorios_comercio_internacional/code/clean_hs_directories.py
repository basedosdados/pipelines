"""Build per-revision Harmonized System directory tables from BACI product_codes files.

Each BACI revision ships product_codes_HS<rev>_V<version>.csv with columns `code,description`
(HS6 + English name). We derive the SH2/SH4 rollups and keep the English name. These tables
give each trade table (trade_hs<rev>) its own revision's exact HS6 code set, so the dbt
relationship test is exact (no tolerance).

Directory tables keep the Portuguese directory-family naming convention (id_sh*/nome_*),
even though BACI only ships English names — PT/ES are left for a later enrichment pass.

Input : ~/Downloads/world_cepii_baci_data/input/product_codes_HS<rev>_V202601.csv
Output: ~/Downloads/world_cepii_baci_data/output/<table>/data.parquet
"""

from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

DATA_ROOT = Path.home() / "Downloads" / "world_cepii_baci_data"
INPUT = DATA_ROOT / "input"
OUTPUT = DATA_ROOT / "output"
VERSION = "V202601"

# BACI revision code -> Data Basis directory table slug
REVISIONS = {"HS92": "hs1992", "HS17": "hs2017"}

SCHEMA = pa.schema(
    [
        pa.field("id_sh6", pa.string()),
        pa.field("id_sh4", pa.string()),
        pa.field("id_sh2", pa.string()),
        pa.field("nome_ingles", pa.string()),
    ]
)


def build_one(rev: str, table_slug: str) -> int:
    src = INPUT / f"product_codes_{rev}_{VERSION}.csv"
    df = pd.read_csv(src, dtype={"code": str})
    df["code"] = df["code"].str.strip().str.zfill(6)
    out = (
        pd.DataFrame(
            {
                "id_sh6": df["code"],
                "id_sh4": df["code"].str[:4],
                "id_sh2": df["code"].str[:2],
                "nome_ingles": df["description"].astype(str).str.strip(),
            }
        )
        .drop_duplicates(subset=["id_sh6"])
        .sort_values("id_sh6")
    )

    dest_dir = OUTPUT / table_slug
    dest_dir.mkdir(parents=True, exist_ok=True)
    table = pa.Table.from_pandas(out, schema=SCHEMA, preserve_index=False)
    pq.write_table(table, dest_dir / "data.parquet", compression="snappy")
    return len(out)


if __name__ == "__main__":
    for rev, slug in REVISIONS.items():
        n = build_one(rev, slug)
        print(f"{slug}: {n} HS6 codes -> {OUTPUT / slug / 'data.parquet'}")
