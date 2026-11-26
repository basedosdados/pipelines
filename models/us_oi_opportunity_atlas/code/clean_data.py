"""Clean the Opportunity Insights Atlas and Migration Patterns files into parquet.

Sources (https://opportunityinsights.org/data/):
- Opportunity Atlas, "All outcomes" tables 4-6 (children born 1978-83):
  tract_outcomes_early.csv, county_outcomes.zip, cz_outcomes.csv
- Radius of Economic Opportunity / Migration Patterns:
  od_pooled.csv, od_race.csv, od_inc.csv, od.csv

The Atlas files have one row per place and one column per
outcome x race x gender x statistic (about 10,000 columns). Each is reshaped into
two tables:
- <geo>_outcome: one row per place x race x gender x parental income percentile,
  one column per outcome (estimate and standard error);
- <geo>_sample: one row per place x race x gender, with the sample size behind each
  outcome and the race-by-gender cell's parental rank and child counts.
Rows whose values are all missing are dropped (most race x gender cells in small
places are suppressed by the source).

The four migration files are stacked into one table, with "pooled" marking the
race or parental income quintile that a file does not split by.

Staging parquet is all-STRING (house convention); values are cast to their
architecture type first, so integers serialise as "12", not "12.0".

Usage:
    python models/us_oi_opportunity_atlas/code/clean_data.py [table ...]

Reads $US_OI_OPPORTUNITY_ATLAS_DATA_DIR/input (default
~/Library/Caches/us_oi_opportunity_atlas_data) and writes output/<table>/ plus
output/_manifest_<table>.json with the row count.
"""

import csv
import json
import os
import re
import shutil
import sys
import zipfile
from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.csv as pacsv
import pyarrow.parquet as pq

DATA_DIR = Path(
    os.environ.get(
        "US_OI_OPPORTUNITY_ATLAS_DATA_DIR",
        Path.home() / "Library" / "Caches" / "us_oi_opportunity_atlas_data",
    )
)
INPUT = DATA_DIR / "input"
OUTPUT = DATA_DIR / "output"
ARCH_DIR = Path(__file__).resolve().parent / "architecture"

RACES = {
    "pooled": "pooled",
    "white": "white",
    "black": "black",
    "hisp": "hispanic",
    "asian": "asian",
    "natam": "native_american",
    "other": "other",
}
GENDERS = ["pooled", "male", "female"]
# Source statistic suffix -> parent_income_percentile value. p10 is published
# but not listed in the codebooks.
STATS = {
    "p1": "1",
    "p10": "10",
    "p25": "25",
    "p50": "50",
    "p75": "75",
    "p100": "100",
    "mean": "mean",
}
RG = (
    r"(?P<race>pooled|white|black|hisp|asian|natam|other)"
    r"_(?P<gender>pooled|male|female)"
)
OUTCOME_RE = re.compile(
    rf"^(?P<outcome>.+?)_{RG}_(?P<stat>p1|p10|p25|p50|p75|p100|mean|n)(?P<se>_se)?$"
)
# Race x gender-level variables (not outcomes) -> sample-table column.
GROUP_RES = {
    "parent_rank_mean": re.compile(rf"^par_rank_{RG}_mean$"),
    "parent_rank_mean_se": re.compile(rf"^par_rank_{RG}_mean_se$"),
    "child_count_2000": re.compile(rf"^kid_{RG}_n$"),
    "child_below_median_count_2000": re.compile(rf"^kid_{RG}_blw_p50_n$"),
    "share_below_median": re.compile(rf"^frac_below_median_{RG}$"),
    "share_childhood_years": re.compile(rf"^frac_years_xw_{RG}$"),
}

ATLAS = {
    "tract": {
        "file": "tract_outcomes_early.csv",
        "keys": ["state_id", "county_id", "tract_id", "commuting_zone_id"],
    },
    "county": {
        "file": "county_outcomes.zip",
        "keys": ["state_id", "county_id", "commuting_zone_id"],
    },
    "commuting_zone": {
        "file": "cz_outcomes.csv",
        "keys": ["commuting_zone_id", "commuting_zone_name"],
    },
}
MIGRATION_FILES = ["od_pooled.csv", "od_race.csv", "od_inc.csv", "od.csv"]
MIGRATION_RACES = {
    "asian": "asian",
    "black": "black",
    "hispanic": "hispanic",
    "white": "white",
    "other": "other",
}

BATCH_BYTES = 64 << 20


def arch_types(table: str) -> dict[str, str]:
    """Return {column: bigquery_type} in architecture order."""
    with open(ARCH_DIR / f"{table}.csv", newline="", encoding="utf-8") as f:
        return {r["name"]: r["bigquery_type"] for r in csv.DictReader(f)}


def to_string_table(df: pd.DataFrame, types: dict[str, str]) -> pa.Table:
    """Cast each column to its architecture type, then to string via arrow."""
    arrays = []
    for name, bq_type in types.items():
        s = df[name]
        if bq_type == "INT64":
            f = pd.to_numeric(s, errors="raise")
            whole = f.dropna()
            if not np.allclose(whole, np.round(whole)):
                raise ValueError(
                    f"{name}: non-integer values in an INT64 column"
                )
            arr = pa.array(f.round().astype("Int64"), type=pa.int64())
        elif bq_type == "FLOAT64":
            arr = pa.array(pd.to_numeric(s, errors="raise"), type=pa.float64())
        else:
            arr = pa.array(s.astype("string"), type=pa.string())
        arrays.append(pc.cast(arr, pa.string()))
    return pa.Table.from_arrays(arrays, names=list(types))


class Writer:
    """Write a table as numbered parquet files under output/<table>/."""

    def __init__(self, table: str):
        self.table = table
        self.types = arch_types(table)
        self.dir = OUTPUT / table
        shutil.rmtree(self.dir, ignore_errors=True)
        self.dir.mkdir(parents=True)
        self.part = 0
        self.rows = 0

    def write(self, df: pd.DataFrame) -> None:
        if df.empty:
            return
        missing = set(self.types) - set(df.columns)
        if missing:
            raise KeyError(f"{self.table}: missing columns {sorted(missing)}")
        pq.write_table(
            to_string_table(df, self.types),
            self.dir / f"part-{self.part:05d}.parquet",
            compression="snappy",
        )
        self.part += 1
        self.rows += len(df)

    def close(self) -> None:
        (OUTPUT / f"_manifest_{self.table}.json").write_text(
            json.dumps({"rows": self.rows, "files": self.part})
        )
        print(
            f"  {self.table}: {self.rows:,} rows in {self.part} files",
            flush=True,
        )


def parse_header(cols: list[str]) -> tuple[dict, dict, list[str]]:
    """Split a wide Atlas header into outcome and race x gender-level columns.

    Returns (outcome_cols, group_cols, outcomes) where
    outcome_cols[(race, gender, stat)][outcome or outcome_se or outcome_n] = source
    column, and group_cols[(race, gender)][sample-table column] = source column.
    """
    outcome_cols: dict = {}
    group_cols: dict = {}
    outcomes: list[str] = []
    for c in cols:
        for target, rx in GROUP_RES.items():
            m = rx.match(c)
            if m:
                group_cols.setdefault((m["race"], m["gender"]), {})[target] = c
                break
        else:
            m = OUTCOME_RE.match(c)
            if not m:
                continue
            o, key = m["outcome"], (m["race"], m["gender"])
            if m["stat"] == "n":
                group_cols.setdefault(key, {})[f"{o}_n"] = c
            else:
                name = f"{o}_se" if m["se"] else o
                outcome_cols.setdefault((*key, m["stat"]), {})[name] = c
            if o not in outcomes:
                outcomes.append(o)
    return outcome_cols, group_cols, outcomes


def geo_keys(df: pd.DataFrame, geo: str) -> pd.DataFrame:
    """Build zero-padded FIPS identifiers from the source's numeric columns."""
    cols = {c.lower(): c for c in df.columns}
    out = pd.DataFrame(index=df.index)
    if geo in ("tract", "county"):
        st = df[cols["state"]].astype("int64")
        cty = df[cols["county"]].astype("int64")
        out["state_id"] = st.map("{:02d}".format)
        out["county_id"] = out["state_id"] + cty.map("{:03d}".format)
        if geo == "tract":
            out["tract_id"] = out["county_id"] + df[cols["tract"]].astype(
                "int64"
            ).map("{:06d}".format)
    cz = pd.to_numeric(df[cols["cz"]], errors="coerce").astype("Int64")
    out["commuting_zone_id"] = cz.astype("string")
    if geo == "commuting_zone":
        out["commuting_zone_name"] = df[cols["czname"]]
    return out


def reshape(
    df: pd.DataFrame, geo: str, header: tuple
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Reshape one batch of wide rows into outcome and sample rows."""
    outcome_cols, group_cols, outcomes = header
    keys = geo_keys(df, geo)
    value_names = [x for o in outcomes for x in (o, f"{o}_se")]
    sample_names = list(GROUP_RES) + [f"{o}_n" for o in outcomes]

    def stack(cells: dict, names: list[str], extra: dict) -> pd.DataFrame:
        block = pd.DataFrame(
            {
                n: df[cells[n]].to_numpy() if n in cells else np.nan
                for n in names
            },
            index=df.index,
        )
        block = block.apply(pd.to_numeric, errors="coerce")
        keep = block.notna().any(axis=1)
        part = pd.concat([keys, block], axis=1)[keep]
        for k, v in extra.items():
            part.insert(len(keys.columns), k, v)
        return part

    out_parts, sample_parts = [], []
    for race_src, race in RACES.items():
        for gender in GENDERS:
            for stat_src, stat in STATS.items():
                cells = outcome_cols.get((race_src, gender, stat_src))
                if cells:
                    out_parts.append(
                        stack(
                            cells,
                            value_names,
                            {
                                "race": race,
                                "gender": gender,
                                "parent_income_percentile": stat,
                            },
                        )
                    )
            cells = group_cols.get((race_src, gender))
            if cells:
                sample_parts.append(
                    stack(
                        cells, sample_names, {"race": race, "gender": gender}
                    )
                )
    # Writer reorders the columns to the architecture
    return pd.concat(out_parts), pd.concat(sample_parts)


def atlas_batches(geo: str):
    """Yield pandas batches of the wide source file."""
    path = INPUT / ATLAS[geo]["file"]
    if path.suffix == ".zip":
        with zipfile.ZipFile(path) as z:
            names = [n for n in z.namelist() if n.endswith(".csv")]
            if len(names) != 1:
                raise ValueError(f"{path}: expected one csv, found {names}")
            raw = z.read(names[0])
        header = raw[: raw.index(b"\n")].decode()
        source = pa.BufferReader(raw)
    else:
        with open(path, encoding="utf-8") as f:
            header = f.readline()
        source = str(path)
    # Declare every type: with ~8,000 sparse columns, inference from the first
    # block types all-empty columns as null and fails on a later block.
    cols = next(csv.reader([header.strip()]))
    types = {
        c: pa.string() if c.lower() in ("cz", "czname") else pa.float64()
        for c in cols
    }
    reader = pacsv.open_csv(
        source,
        read_options=pacsv.ReadOptions(block_size=BATCH_BYTES),
        convert_options=pacsv.ConvertOptions(
            column_types=types, strings_can_be_null=True
        ),
    )
    for batch in reader:
        yield batch.to_pandas()


def clean_atlas(geo: str) -> None:
    print(f"=== {geo} ===", flush=True)
    w_out = Writer(f"{geo}_outcome")
    w_sample = Writer(f"{geo}_sample")
    header = None
    for df in atlas_batches(geo):
        if header is None:
            header = parse_header(list(df.columns))
            print(
                f"  {len(df.columns):,} source columns, "
                f"{len(header[2])} outcomes",
                flush=True,
            )
        out, sample = reshape(df, geo, header)
        w_out.write(out)
        w_sample.write(sample)
    w_out.close()
    w_sample.close()


def clean_migration() -> None:
    print("=== commuting_zone_migration ===", flush=True)
    w = Writer("commuting_zone_migration")
    for name in MIGRATION_FILES:
        reader = pacsv.open_csv(
            str(INPUT / name),
            read_options=pacsv.ReadOptions(block_size=BATCH_BYTES),
            convert_options=pacsv.ConvertOptions(
                column_types={
                    "o_cz": pa.string(),
                    "d_cz": pa.string(),
                    "pool": pa.string(),
                }
            ),
        )
        n_file = 0
        for batch in reader:
            df = batch.to_pandas()
            race, quintile = zip(*df["pool"].map(split_pool), strict=True)
            out = pd.DataFrame(
                {
                    "origin_commuting_zone_id": df["o_cz"],
                    "origin_commuting_zone_name": df["o_cz_name"],
                    "origin_state_name": df["o_state_name"],
                    "destination_commuting_zone_id": df["d_cz"],
                    "destination_commuting_zone_name": df["d_cz_name"],
                    "destination_state_name": df["d_state_name"],
                    "race": race,
                    "parent_income_quintile": quintile,
                    "individuals": df["n"],
                    "origin_individuals": df["n_tot_o"],
                    "destination_individuals": df["n_tot_d"],
                    "probability_destination_given_origin": df["pr_d_o"],
                    "probability_origin_given_destination": df["pr_o_d"],
                }
            )
            w.write(out)
            n_file += len(out)
        print(f"  {name}: {n_file:,} rows", flush=True)
    w.close()


def split_pool(pool: str) -> tuple[str, str]:
    """Map the source's pool value to (race, parent_income_quintile)."""
    p = pool.strip()
    if p.lower() == "pooled":
        return "pooled", "pooled"
    if re.fullmatch(r"Q[1-5]", p):
        return "pooled", p[1]
    m = re.fullmatch(
        r"(?i)(asian|black|hispanic|white|other)(?:[_ ]?(Q[1-5]))?", p
    )
    if not m:
        raise ValueError(f"unexpected pool value {pool!r}")
    race = MIGRATION_RACES[m.group(1).lower()]
    return race, m.group(2)[1] if m.group(2) else "pooled"


TABLES = {
    "commuting_zone": lambda: clean_atlas("commuting_zone"),
    "county": lambda: clean_atlas("county"),
    "tract": lambda: clean_atlas("tract"),
    "commuting_zone_migration": clean_migration,
}


def main() -> None:
    only = sys.argv[1:] or list(TABLES)
    OUTPUT.mkdir(parents=True, exist_ok=True)
    for t in only:
        TABLES[t]()


if __name__ == "__main__":
    main()
