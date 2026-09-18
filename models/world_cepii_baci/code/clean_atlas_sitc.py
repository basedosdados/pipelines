"""Clean Harvard Growth Lab Atlas SITC rev. 2 CSVs into year-partitioned parquet.

Three tables come out of this script:

  trade_sitc          bilateral trade, year x exporter x importer x SITC-4 product
                      (from sitc_country_country_product_year_4_*.csv)
  complexity_country  ECI, COI and diversity per country-year
                      (from sitc_country_year.csv)
  complexity_product  PCI per product-year
                      (from sitc_product_year_4.csv)

Source columns are documented in sitc_data_dictionary.csv, shipped with the DOI.
Values are in current USD -- unlike BACI, which reports thousands of USD, and
unlike BACI the bilateral file carries both an export and an import leg for the
same ordered pair.

Every output column is written as STRING: staging is all-STRING by house
convention and the dbt model safe_casts each column to its architecture type.
Reading the CSVs with dtype="string" keeps the source text verbatim, so leading
zeros survive and missing values stay null instead of becoming the literal "nan".

The bilateral source is split into year ranges, so a given year never spans two
files; per-year writers are therefore opened and closed within one file.

Usage: python clean_atlas_sitc.py [trade_sitc|complexity_country|complexity_product|all]
"""

import sys
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

DATA_ROOT = Path.home() / "Downloads" / "world_cepii_baci_data"
INPUT = DATA_ROOT / "input" / "atlas_sitc"
OUTPUT = DATA_ROOT / "output"

CHUNK_ROWS = 2_000_000

BILATERAL_FILES = [
    "sitc_country_country_product_year_4_1962_1969.csv",
    "sitc_country_country_product_year_4_1970_1979.csv",
    "sitc_country_country_product_year_4_1980_1989.csv",
    "sitc_country_country_product_year_4_1990_1999.csv",
    "sitc_country_country_product_year_4_2000_2009.csv",
    "sitc_country_country_product_year_4_2010_2019.csv",
    "sitc_country_country_product_year_4_2020_2024.csv",
]

TRADE_SCHEMA = pa.schema(
    [
        pa.field("id_country_exporter", pa.string()),
        pa.field("id_country_importer", pa.string()),
        pa.field("product_code", pa.string()),
        pa.field("export_value", pa.string()),
        pa.field("import_value", pa.string()),
    ]
)

COMPLEXITY_COUNTRY_SCHEMA = pa.schema(
    [
        pa.field("classification", pa.string()),
        pa.field("id_country", pa.string()),
        pa.field("eci", pa.string()),
        pa.field("coi", pa.string()),
        pa.field("diversity", pa.string()),
        pa.field("growth_projection", pa.string()),
        pa.field("export_value", pa.string()),
        pa.field("import_value", pa.string()),
    ]
)

COMPLEXITY_PRODUCT_SCHEMA = pa.schema(
    [
        pa.field("classification", pa.string()),
        pa.field("product_code", pa.string()),
        pa.field("pci", pa.string()),
        pa.field("export_value", pa.string()),
        pa.field("import_value", pa.string()),
    ]
)


def _write_year_partitions(
    df: pd.DataFrame, schema: pa.Schema, dest_root: Path
) -> None:
    """Write one parquet file per year=<YYYY> partition directory."""
    for year, part in df.groupby("year", sort=True):
        part_dir = dest_root / f"year={year}"
        part_dir.mkdir(parents=True, exist_ok=True)
        table = pa.Table.from_pandas(
            part.drop(columns=["year"]), schema=schema, preserve_index=False
        )
        pq.write_table(table, part_dir / "data.parquet", compression="snappy")


def clean_trade_sitc() -> int:
    dest_root = OUTPUT / "trade_sitc"
    dest_root.mkdir(parents=True, exist_ok=True)
    total = 0
    unresolved_note: set[str] = set()

    for filename in BILATERAL_FILES:
        path = INPUT / filename
        if not path.exists():
            raise FileNotFoundError(f"Missing bilateral file: {path}")
        writers: dict[str, pq.ParquetWriter] = {}
        file_rows = 0
        for chunk in pd.read_csv(
            path,
            dtype="string",
            chunksize=CHUNK_ROWS,
            usecols=[
                "country_id",
                "partner_country_id",
                "product_sitc_code",
                "year",
                "export_value",
                "import_value",
            ],
        ):
            out = pd.DataFrame(
                {
                    "year": chunk["year"].str.strip(),
                    "id_country_exporter": chunk["country_id"].str.strip(),
                    "id_country_importer": chunk[
                        "partner_country_id"
                    ].str.strip(),
                    "product_code": chunk["product_sitc_code"].str.strip(),
                    "export_value": chunk["export_value"].str.strip(),
                    "import_value": chunk["import_value"].str.strip(),
                }
            )
            unresolved_note.update(
                out["id_country_exporter"].dropna().unique()
            )
            unresolved_note.update(
                out["id_country_importer"].dropna().unique()
            )
            for year, part in out.groupby("year", sort=True):
                year_key = str(year)
                if year_key not in writers:
                    part_dir = dest_root / f"year={year_key}"
                    part_dir.mkdir(parents=True, exist_ok=True)
                    writers[year_key] = pq.ParquetWriter(
                        part_dir / "data.parquet",
                        TRADE_SCHEMA,
                        compression="snappy",
                    )
                writers[year_key].write_table(
                    pa.Table.from_pandas(
                        part.drop(columns=["year"]),
                        schema=TRADE_SCHEMA,
                        preserve_index=False,
                    )
                )
            file_rows += len(out)
        for writer in writers.values():
            writer.close()
        total += file_rows
        print(
            f"  {filename}: {file_rows:,} rows -> years {min(writers)}-{max(writers)}",
            flush=True,
        )

    print(f"trade_sitc: {total:,} total rows -> {dest_root}")
    print(f"  distinct country codes seen: {len(unresolved_note)}")
    return total


def clean_complexity_country() -> int:
    path = INPUT / "sitc_country_year.csv"
    df = pd.read_csv(path, dtype="string")
    out = pd.DataFrame(
        {
            "year": df["year"].str.strip(),
            "classification": "sitc",
            "id_country": df["country_id"].str.strip(),
            "eci": df["eci"].str.strip(),
            "coi": df["coi"].str.strip(),
            "diversity": df["diversity"].str.strip(),
            "growth_projection": df["growth_proj"].str.strip(),
            "export_value": df["export_value"].str.strip(),
            "import_value": df["import_value"].str.strip(),
        }
    )
    dest_root = OUTPUT / "complexity_country"
    dest_root.mkdir(parents=True, exist_ok=True)
    _write_year_partitions(out, COMPLEXITY_COUNTRY_SCHEMA, dest_root)
    print(f"complexity_country: {len(out):,} rows -> {dest_root}")
    return len(out)


def clean_complexity_product() -> int:
    path = INPUT / "sitc_product_year_4.csv"
    df = pd.read_csv(path, dtype="string")
    out = pd.DataFrame(
        {
            "year": df["year"].str.strip(),
            "classification": "sitc",
            "product_code": df["product_sitc_code"].str.strip(),
            "pci": df["pci"].str.strip(),
            "export_value": df["export_value"].str.strip(),
            "import_value": df["import_value"].str.strip(),
        }
    )
    dest_root = OUTPUT / "complexity_product"
    dest_root.mkdir(parents=True, exist_ok=True)
    _write_year_partitions(out, COMPLEXITY_PRODUCT_SCHEMA, dest_root)
    print(f"complexity_product: {len(out):,} rows -> {dest_root}")
    return len(out)


if __name__ == "__main__":
    target = sys.argv[1] if len(sys.argv) > 1 else "all"
    if target in ("complexity_country", "all"):
        clean_complexity_country()
    if target in ("complexity_product", "all"):
        clean_complexity_product()
    if target in ("trade_sitc", "all"):
        clean_trade_sitc()
