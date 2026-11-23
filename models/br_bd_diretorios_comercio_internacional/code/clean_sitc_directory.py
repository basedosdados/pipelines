"""Build the SITC rev. 2 directory table from the Harvard Growth Lab classification file.

product_sitc.csv (DOI 10.7910/DVN/3BAL1O) holds the three SITC levels in one
file: 10 sections (1 digit), 70 divisions (2 digits) and 788 items (4 digits).
The trade and complexity tables are at the 4-digit level, so that is the grain
here; the 2- and 1-digit rollups come along as columns.

The rollups are resolved through `product_parent_id`, not by slicing the code
string. They agree for every ordinary code, but the residual code `XXXX`
(unspecified products) has no digits to slice and only the parent chain places
it correctly under section 9.

Directory tables keep the Portuguese directory-family naming convention
(id_*/nome_*), as the HS directory tables do. Names are English-only, which is
what the source ships.

Input : ~/Downloads/world_cepii_baci_data/input/atlas_sitc/product_sitc.csv
Output: ~/Downloads/world_cepii_baci_data/output/sitc/data.parquet
"""

from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

DATA_ROOT = Path.home() / "Downloads" / "world_cepii_baci_data"
INPUT = DATA_ROOT / "input" / "atlas_sitc"
OUTPUT = DATA_ROOT / "output"

SCHEMA = pa.schema(
    [
        pa.field("id_sitc4", pa.string()),
        pa.field("id_sitc2", pa.string()),
        pa.field("id_sitc1", pa.string()),
        pa.field("nome_ingles", pa.string()),
        pa.field("nome_curto_ingles", pa.string()),
    ]
)


def build() -> int:
    df = pd.read_csv(INPUT / "product_sitc.csv", dtype="string")
    code_by_id = df.set_index("product_id")["product_sitc_code"]
    parent_by_id = df.set_index("product_id")["product_parent_id"]

    level4 = df[df["product_level"] == "4"].copy()
    level4["id_sitc2"] = level4["product_parent_id"].map(code_by_id)
    level4["id_sitc1"] = (
        level4["product_parent_id"].map(parent_by_id).map(code_by_id)
    )

    out = (
        pd.DataFrame(
            {
                "id_sitc4": level4["product_sitc_code"].str.strip(),
                "id_sitc2": level4["id_sitc2"].str.strip(),
                "id_sitc1": level4["id_sitc1"].str.strip(),
                "nome_ingles": level4["product_name"].str.strip(),
                "nome_curto_ingles": level4["product_name_short"].str.strip(),
            }
        )
        .drop_duplicates(subset=["id_sitc4"])
        .sort_values("id_sitc4")
    )

    dest_dir = OUTPUT / "sitc"
    dest_dir.mkdir(parents=True, exist_ok=True)
    pq.write_table(
        pa.Table.from_pandas(out, schema=SCHEMA, preserve_index=False),
        dest_dir / "data.parquet",
        compression="snappy",
    )
    return len(out)


if __name__ == "__main__":
    n = build()
    print(f"sitc: {n} SITC-4 codes -> {OUTPUT / 'sitc' / 'data.parquet'}")
