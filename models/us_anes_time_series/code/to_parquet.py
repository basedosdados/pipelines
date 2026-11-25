# ruff: noqa: SIM115
"""Clean the ANES CDF CSV into partitioned, all-STRING Parquet (staging
convention; dbt safe_casts to the architecture types).

- Drop the constant `Version` column.
- Rename VCF0004 -> year (partition).
- Missing/INAP in the source is a space ' ' -> NULL. Strip every cell; '' -> NULL.
- For numeric columns, NULL the documented missing sentinels (e.g. thermometer
  98/99, age 00) by integer match, so dbt safe_cast yields clean numbers.
- Output: output/cumulative/year=YYYY/data.parquet
"""

import json
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

ROOT = Path(__file__).resolve().parent.parent
CSV = ROOT / "input" / "anes_timeseries_cdf_csv_20260205.csv"
OUT = ROOT / "output" / "cumulative"
MAN = json.load(open(ROOT / "code" / "build" / "columns.json"))

order = [m["name"] for m in MAN]  # includes 'year' first
rename = {
    m["code"]: m["name"] for m in MAN
}  # VCF0004 -> year, others identity
num_sent = {
    m["name"]: set(int(s) for s in m["null_sentinels"])
    for m in MAN
    if m["null_sentinels"]
}

print(f"reading {CSV.name} ...")
df = pd.read_csv(
    CSV, dtype=str, keep_default_na=False, na_values=[], encoding="utf-8-sig"
)
print(f"  loaded {df.shape[0]} rows x {df.shape[1]} cols")

df = df.drop(columns=[c for c in df.columns if c == "Version"])
df = df.rename(columns=rename)

# strip whitespace everywhere; '' -> NA
df = df.apply(lambda s: s.str.strip())
df = df.replace("", pd.NA)

# NULL numeric missing sentinels (integer match handles '00' vs '0')
for col, sents in num_sent.items():
    if col not in df.columns:
        continue
    asint = pd.to_numeric(df[col], errors="coerce")
    df.loc[asint.isin(sents), col] = pd.NA

# reorder exactly to the architecture
df = df[order]

# partition key `year` lives in the path only — drop it from file content
file_cols = [c for c in order if c != "year"]
schema = pa.schema([(c, pa.string()) for c in file_cols])

OUT.mkdir(parents=True, exist_ok=True)

# Fail fast: the partition key must be present and numeric for every row, or
# rows would be silently dropped from every partition.
bad_year = df.loc[
    df["year"].notna() & ~df["year"].str.fullmatch(r"\d+"), "year"
]
if len(bad_year):
    raise SystemExit(
        f"non-numeric year values: {sorted(bad_year.unique())[:10]}"
    )
n_null_year = int(df["year"].isna().sum())
if n_null_year:
    raise SystemExit(f"{n_null_year} rows have a NULL year (partition key)")

years = sorted(df["year"].unique(), key=lambda x: int(x))
total = 0
for y in years:
    part = df[df["year"] == y][file_cols]
    tbl = pa.Table.from_pandas(part, schema=schema, preserve_index=False)
    d = OUT / f"year={int(y)}"
    d.mkdir(parents=True, exist_ok=True)
    pq.write_table(tbl, d / "data.parquet", compression="snappy")
    total += len(part)
if total != len(df):
    raise SystemExit(f"row loss: wrote {total} of {len(df)}")
print(f"wrote {len(years)} year partitions, {total} rows to {OUT}")

# ---- dicionario table (unpartitioned, all-STRING) --------------------------
DIC_CSV = ROOT / "code" / "build" / "dicionario_data.csv"
dic = pd.read_csv(DIC_CSV, dtype=str, keep_default_na=False)
dic_schema = pa.schema(
    [
        (c, pa.string())
        for c in [
            "id_tabela",
            "nome_coluna",
            "chave",
            "cobertura_temporal",
            "valor",
        ]
    ]
)
dic_out = ROOT / "output" / "dicionario"
dic_out.mkdir(parents=True, exist_ok=True)
pq.write_table(
    pa.Table.from_pandas(
        dic.replace("", pd.NA), schema=dic_schema, preserve_index=False
    ),
    dic_out / "data.parquet",
    compression="snappy",
)
print(f"wrote dicionario: {len(dic)} rows to {dic_out}")
