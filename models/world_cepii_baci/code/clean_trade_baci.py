"""Clean BACI bilateral trade CSVs into English-named, year-partitioned parquet.

BACI annual file columns: t,i,j,k,v,q
  t = year, i = exporter (M49 numeric), j = importer (M49 numeric),
  k = HS6 product code, v = value (thousand USD), q = quantity (metric tons, may be NA).

Output columns (English; trade dataset convention):
  year (INT64, partition), id_country_exporter, id_country_importer, product_code,
  value (thousand USD), quantity (metric tons).

Processed one year at a time from inside the zip to keep peak RAM ~ one annual file.

Usage: python clean_trade_baci.py HS92 trade_hs92
"""

import io
import sys
import zipfile
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

DATA_ROOT = Path.home() / "Downloads" / "world_cepii_baci_data"
INPUT = DATA_ROOT / "input"
OUTPUT = DATA_ROOT / "output"
VERSION = "V202601"

SCHEMA = pa.schema(
    [
        pa.field("id_country_exporter", pa.string()),
        pa.field("id_country_importer", pa.string()),
        pa.field("product_code", pa.string()),
        pa.field("value", pa.float64()),
        pa.field("quantity", pa.float64()),
    ]
)


def clean_revision(rev: str, table_slug: str) -> None:
    zip_path = INPUT / f"BACI_{rev}_{VERSION}.zip"
    dest_root = OUTPUT / table_slug
    dest_root.mkdir(parents=True, exist_ok=True)
    total = 0
    with zipfile.ZipFile(zip_path) as zf:
        members = sorted(
            m for m in zf.namelist() if m.startswith(f"BACI_{rev}_Y")
        )
        for m in members:
            year = int(m.split("_Y")[1][:4])
            with zf.open(m) as fh:
                df = pd.read_csv(
                    io.TextIOWrapper(fh, encoding="utf-8"),
                    dtype={"i": "string", "j": "string", "k": "string"},
                    na_values=["NA", ""],
                )
            out = pd.DataFrame(
                {
                    "id_country_exporter": df["i"].str.strip(),
                    "id_country_importer": df["j"].str.strip(),
                    "product_code": df["k"].str.strip().str.zfill(6),
                    "value": pd.to_numeric(df["v"], errors="coerce"),
                    "quantity": pd.to_numeric(df["q"], errors="coerce"),
                }
            )
            part_dir = dest_root / f"year={year}"
            part_dir.mkdir(parents=True, exist_ok=True)
            pq.write_table(
                pa.Table.from_pandas(out, schema=SCHEMA, preserve_index=False),
                part_dir / "data.parquet",
                compression="snappy",
            )
            total += len(out)
            print(f"  {table_slug} year={year}: {len(out):,} rows")
    print(f"{table_slug}: {total:,} total rows -> {dest_root}")


if __name__ == "__main__":
    clean_revision(sys.argv[1], sys.argv[2])
