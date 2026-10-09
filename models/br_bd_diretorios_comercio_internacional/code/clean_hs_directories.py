"""Build per-revision Harmonized System directory tables from BACI product_codes files.

Each BACI revision ships product_codes_HS<rev>_V<version>.csv with columns `code,description`
(HS6 + English name). We derive the SH2/SH4 rollups and keep the English name. These tables
give each trade table (trade_hs<rev>) its own revision's exact HS6 code set, so the dbt
relationship test is exact (no tolerance).

Directory tables keep the Portuguese directory-family naming convention (id_sh*/nome_*),
even though BACI only ships English names — PT/ES are left for a later enrichment pass.

The product-codes file ships inside each revision's BACI zip, so it is read
straight out of the archive; a loose extracted copy is used when present, which
keeps older working directories valid.

Input : ~/Downloads/world_cepii_baci_data/input/BACI_HS<rev>_V202601.zip
        (or an extracted product_codes_HS<rev>_V202601.csv beside it)
Output: ~/Downloads/world_cepii_baci_data/output/<table>/data.parquet
"""

import io
import zipfile
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

DATA_ROOT = Path.home() / "Downloads" / "world_cepii_baci_data"
INPUT = DATA_ROOT / "input"
OUTPUT = DATA_ROOT / "output"
VERSION = "V202601"

# BACI revision code -> Data Basis directory table slug.
# One directory per revision, so each trade table's relationship test is exact:
# the same HS6 code means different goods across revisions (010111 is
# "pure-bred breeding horses" in HS92 but 010121 in HS17).
REVISIONS = {
    "HS92": "hs1992",
    "HS96": "hs1996",
    "HS02": "hs2002",
    "HS07": "hs2007",
    "HS12": "hs2012",
    "HS17": "hs2017",
    "HS22": "hs2022",
}

SCHEMA = pa.schema(
    [
        pa.field("id_sh6", pa.string()),
        pa.field("id_sh4", pa.string()),
        pa.field("id_sh2", pa.string()),
        pa.field("nome_ingles", pa.string()),
    ]
)


def _read_product_codes(rev: str) -> pd.DataFrame:
    """Read a revision's product-codes CSV, from the zip or a loose copy."""
    name = f"product_codes_{rev}_{VERSION}.csv"
    loose = INPUT / name
    if loose.exists():
        return pd.read_csv(loose, dtype={"code": str})
    zip_path = INPUT / f"BACI_{rev}_{VERSION}.zip"
    if not zip_path.exists():
        raise FileNotFoundError(f"Neither {loose} nor {zip_path} is present")
    with zipfile.ZipFile(zip_path) as zf:
        member = next(m for m in zf.namelist() if m.endswith(name))
        with zf.open(member) as fh:
            return pd.read_csv(
                io.TextIOWrapper(fh, encoding="utf-8"), dtype={"code": str}
            )


def build_one(rev: str, table_slug: str) -> int:
    df = _read_product_codes(rev)
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
