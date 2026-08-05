"""Clean ASGS allocation + correspondence files into per-table Parquet for
br_bd_diretorios_au.

- Column order/types come from the architecture CSVs in ./architecture/.
- Source->target mapping is driven by each column's `original_name`.
- Derived columns: abbreviation / abbreviation_state (from state code map).
- Non-ABS structures (lga/poa/suburb/ced/sed) arrive at MESH-BLOCK level and are
  deduped to one row per unit; area_albers_sqkm is SUMMED per unit.
- Aggregate structures (state/sa1..sa4/gccsa) are already one row per unit.
- Output: typed Parquet (codes/names STRING, area/ratio FLOAT64), one file per
  table at output/<table>.parquet.

Run:  python clean.py            # all tables whose source files are present
      python clean.py sa2_2021   # a subset
"""

import glob
import os
import sys

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

HERE = os.path.dirname(os.path.abspath(__file__))
ARCH = os.path.join(HERE, "architecture")
DS = os.path.dirname(HERE)  # models/br_bd_diretorios_au
INPUT = os.path.join(DS, "input")
OUTPUT = os.path.join(DS, "output")

# STATE_CODE -> sigla. 9 = Other Territories, Z = Outside Australia.
STATE_ABBR = {
    "1": "NSW",
    "2": "VIC",
    "3": "QLD",
    "4": "SA",
    "5": "WA",
    "6": "TAS",
    "7": "NT",
    "8": "ACT",
    "9": "OT",
    "Z": "ZZ",
}
STATE_NAME = {
    "1": "New South Wales",
    "2": "Victoria",
    "3": "Queensland",
    "4": "South Australia",
    "5": "Western Australia",
    "6": "Tasmania",
    "7": "Northern Territory",
    "8": "Australian Capital Territory",
    "9": "Other Territories",
    "Z": "Outside Australia",
}

FLOAT_COLS = {"area_albers_sqkm", "ratio"}

# table -> (source path relative to INPUT, level)
#   level: "aggregate" | "meshblock" | "correspondence"
REG = {
    "state": ("2021/STE_2021_AUST.xlsx", "aggregate"),
    # 2021 ABS main structure
    "sa1_2021": ("2021/SA1_2021_AUST.xlsx", "aggregate"),
    "sa2_2021": ("2021/SA2_2021_AUST.xlsx", "aggregate"),
    "sa3_2021": ("2021/SA3_2021_AUST.xlsx", "aggregate"),
    "sa4_2021": ("2021/SA4_2021_AUST.xlsx", "aggregate"),
    "gccsa_2021": ("2021/GCCSA_2021_AUST.xlsx", "aggregate"),
    # 2021 Non-ABS (mesh-block level -> dedup)
    "lga_2021": ("2021/LGA_2021_AUST.xlsx", "meshblock"),
    "postal_area_2021": ("2021/POA_2021_AUST.xlsx", "meshblock"),
    "suburb_2021": ("2021/SAL_2021_AUST.xlsx", "meshblock"),
    "commonwealth_electoral_division_2021": (
        "2021/CED_2021_AUST.xlsx",
        "meshblock",
    ),
    "state_electoral_division_2021": ("2021/SED_2021_AUST.xlsx", "meshblock"),
    # correspondences
    "correspondence_sa2_2016_2021": (
        "correspondences/CG_SA2_2016_SA2_2021.csv",
        "correspondence",
    ),
    "correspondence_lga_2016_2021": (
        "correspondences/CG_LGA_2016_LGA_2021.csv",
        "correspondence",
    ),
    # 2016 ABS main structure (aggregate, one row per unit)
    "sa1_2016": ("2016/csv/SA1_2016_AUST.csv", "aggregate"),
    "sa2_2016": ("2016/csv/SA2_2016_AUST.csv", "aggregate"),
    "sa3_2016": ("2016/csv/SA3_2016_AUST.csv", "aggregate"),
    "sa4_2016": ("2016/csv/SA4_2016_AUST.csv", "aggregate"),
    "gccsa_2016": ("2016/csv/GCCSA_2016_AUST.csv", "aggregate"),
    # 2016 Non-ABS (mesh-block or SA1 grain -> dedup by unit code, sum area)
    "lga_2016": (
        "2016/csv/LGA_2016_*.csv",
        "meshblock",
    ),  # per-state files, concat
    "postal_area_2016": ("2016/csv/POA_2016_AUST.csv", "meshblock"),
    "suburb_2016": ("2016/csv/SSC_2016_AUST.csv", "meshblock"),
    "commonwealth_electoral_division_2016": (
        "2016/csv/CED_2016_AUST.csv",
        "meshblock",
    ),
    "state_electoral_division_2016": (
        "2016/csv/SED_2016_AUST.csv",
        "meshblock",
    ),
}


# Per-table derivations run after source->target mapping, before dedup.
# CED 2016 source has no STATE column; derive it from the SA1 code (first char).
def _derive_ced_2016_state(out, src):
    if "SA1_MAINCODE_2016" in src.columns:
        out["id_state"] = (
            src["SA1_MAINCODE_2016"].astype(str).str[0].to_numpy()
        )
    return out


DERIVE = {"commonwealth_electoral_division_2016": _derive_ced_2016_state}


def read_arch(table):
    """Return list of (name, bigquery_type, original_name) in architecture order."""
    path = os.path.join(ARCH, f"{table}.csv")
    a = pd.read_csv(path, dtype=str).fillna("")
    return list(
        zip(a["name"], a["bigquery_type"], a["original_name"], strict=False)
    )


def load_source(rel, level):
    path = os.path.join(INPUT, rel)
    enc = "latin-1" if "/2016/" in path else "utf-8"
    if "*" in rel:  # glob -> concat (e.g. LGA per-state 2016)
        parts = sorted(glob.glob(path))
        if not parts:
            return None
        return pd.concat(
            [pd.read_csv(p, dtype=str, encoding=enc) for p in parts],
            ignore_index=True,
        )
    if path.endswith(".csv"):
        return pd.read_csv(path, dtype=str, encoding=enc)
    return pd.read_excel(path, dtype=str)


def build_table(table):
    rel, level = REG[table]
    src_path = os.path.join(INPUT, rel)
    present = (
        bool(glob.glob(src_path)) if "*" in rel else os.path.exists(src_path)
    )
    if not present:
        print(f"SKIP {table}: source not present ({rel})")
        return None
    cols = read_arch(table)
    src = load_source(rel, level)

    # PK is the first architecture column
    pk = cols[0][0]

    out = pd.DataFrame()
    for name, _btype, original in cols:
        if name == "abbreviation":
            out[name] = None  # filled after id_state resolves below
        elif name == "abbreviation_state":
            out[name] = None
        else:
            if original and original in src.columns:
                out[name] = src[original].to_numpy()
            else:
                out[name] = None  # e.g. derived / missing legacy shortcode

    # per-table derivations that need the raw source (e.g. CED 2016 state)
    if table in DERIVE:
        out = DERIVE[table](out, src)

    # correspondence: drop rows with a null/blank endpoint code (ABS suppresses
    # tiny BMOS slivers with a blank destination, plus an "Outside Australia"
    # sentinel) — a crosswalk row needs both endpoints to be usable.
    if level == "correspondence":
        id_from, id_to = cols[0][0], cols[2][0]
        for c in (id_from, id_to):
            s = out[c].astype("string").str.strip()
            out = out[s.notna() & (s != "")]
        out = out.reset_index(drop=True)

    # dedup mesh-block level -> one row per unit; sum area
    if level == "meshblock":
        agg = {
            c: "first"
            for c in out.columns
            if c != pk and c != "area_albers_sqkm"
        }
        if "area_albers_sqkm" in out.columns:
            out["area_albers_sqkm"] = pd.to_numeric(
                out["area_albers_sqkm"], errors="coerce"
            )
            agg["area_albers_sqkm"] = "sum"
        out = out.groupby(pk, as_index=False, dropna=False).agg(agg)
        # restore architecture column order
        out = out[[c[0] for c in cols]]

    # derived abbreviations from state code
    state_src = "id_state" if "id_state" in out.columns else None
    if "abbreviation" in out.columns and state_src:
        out["abbreviation"] = out[state_src].map(STATE_ABBR)
    if "abbreviation_state" in out.columns and state_src:
        out["abbreviation_state"] = out[state_src].map(STATE_ABBR)
    # name_state fallback when the source lacks it (e.g. CED 2016)
    if (
        "name_state" in out.columns
        and state_src
        and out["name_state"].isna().all()
    ):
        out["name_state"] = out[state_src].map(STATE_NAME)

    # types + null hygiene
    schema_fields = []
    for name, _btype, _ in cols:
        if name in FLOAT_COLS:
            out[name] = pd.to_numeric(out[name], errors="coerce")
            schema_fields.append(pa.field(name, pa.float64()))
        else:
            out[name] = (
                out[name].astype("object").where(out[name].notna(), None)
            )
            out[name] = out[name].map(lambda v: None if v is None else str(v))
            schema_fields.append(pa.field(name, pa.string()))

    tbl = pa.Table.from_pandas(
        out[[c[0] for c in cols]],
        schema=pa.schema(schema_fields),
        preserve_index=False,
    )
    return tbl


def write_table(table, tbl):
    os.makedirs(OUTPUT, exist_ok=True)
    path = os.path.join(OUTPUT, f"{table}.parquet")
    pq.write_table(tbl, path, compression="snappy")
    return path


def main():
    targets = sys.argv[1:] or list(REG.keys())
    summary = []
    for t in targets:
        tbl = build_table(t)
        if tbl is None:
            continue
        path = write_table(t, tbl)
        summary.append((t, tbl.num_rows, tbl.num_columns))
        print(
            f"OK  {t:42} rows={tbl.num_rows:>7}  cols={tbl.num_columns}  -> {os.path.relpath(path, DS)}"
        )
    print("\n=== SUMMARY ===")
    for t, r, c in summary:
        print(f"  {t:42} {r:>7} rows  {c} cols")


if __name__ == "__main__":
    main()
