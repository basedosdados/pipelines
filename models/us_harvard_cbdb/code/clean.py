"""Clean CBDB SQLite -> typed Parquet for us_harvard_cbdb.

One-shot onboarding upload keeps TYPED parquet (per bigquery-conventions).
Tables are unpartitioned (directory-like): output/<table>/data.parquet.

Env:
  CBDB_DB   path to the SQLite file (default ~/Downloads/us_harvard_cbdb_data/input/cbdb_20260801.sqlite3)
  CBDB_OUT  output root            (default ~/Downloads/us_harvard_cbdb_data/output)
"""

import os
import sqlite3

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from schema_spec import DICT_SOURCES, TABLE_ORDER, TABLES

HOME = os.path.expanduser("~")
DB = os.environ.get(
    "CBDB_DB",
    f"{HOME}/Downloads/us_harvard_cbdb_data/input/cbdb_20260801.sqlite3",
)
OUT = os.environ.get(
    "CBDB_OUT", f"{HOME}/Downloads/us_harvard_cbdb_data/output"
)

PA_TYPE = {"STRING": pa.string(), "INT64": pa.int64(), "FLOAT64": pa.float64()}


def to_str(v):
    if v is None:
        return None
    if isinstance(v, float):
        if pd.isna(v):
            return None
        if v.is_integer():
            return str(int(v))
        return str(v)
    if isinstance(v, int):
        return str(v)
    s = str(v).strip()
    return s if s != "" else None


def clean_numeric(series, nz, is_float):
    s = pd.to_numeric(series, errors="coerce")
    if nz == "year":
        s = s.where(s > 0)
    elif nz == "count":
        s = s.where(s != -9999)
    elif nz == "coord":
        s = s.where(s != 0)
    if is_float:
        return s.astype("float64")
    return s.astype("Int64")


def build_table_df(con, spec):
    src_cols = [c["src"] for c in spec["columns"] if c.get("src")]
    col_list = ", ".join('"' + c + '"' for c in src_cols)
    q = "SELECT " + col_list + ' FROM "' + spec["source"] + '"'
    raw = pd.read_sql_query(q, con)
    out = pd.DataFrame()
    for c in spec["columns"]:
        if not c.get("src"):
            continue
        col = raw[c["src"]]
        if c["type"] == "STRING":
            out[c["name"]] = col.map(to_str)
        elif c["type"] == "INT64":
            out[c["name"]] = clean_numeric(
                col, c.get("nz", ""), is_float=False
            )
        else:  # FLOAT64
            out[c["name"]] = clean_numeric(col, c.get("nz", ""), is_float=True)
    return out


def build_dicionario(con):
    rows = []
    # coded vocabularies from CBDB code tables
    for src, key, en, chn, targets in DICT_SOURCES:
        df = pd.read_sql_query(
            f'SELECT "{key}" k, "{en}" en, "{chn}" chn FROM "{src}"', con
        )
        for _, r in df.iterrows():
            chave = to_str(r["k"])
            if chave is None:
                continue
            valor = to_str(r["en"]) or to_str(r["chn"])
            if valor is None:
                continue
            for tbl, col in targets:
                rows.append((tbl, col, chave, "", valor))
    # sex literal
    for tbl, col in [("person", "sex")]:
        for chave, valor in [("0", "male"), ("1", "female")]:
            rows.append((tbl, col, chave, "", valor))
    return pd.DataFrame(
        rows,
        columns=[
            "id_tabela",
            "nome_coluna",
            "chave",
            "cobertura_temporal",
            "valor",
        ],
    )


def arrow_schema(spec):
    return pa.schema(
        [(c["name"], PA_TYPE[c["type"]]) for c in spec["columns"]]
    )


def write_table(name, df, spec):
    d = os.path.join(OUT, name)
    os.makedirs(d, exist_ok=True)
    # enforce column order
    df = df[[c["name"] for c in spec["columns"]]]
    tbl = pa.Table.from_pandas(
        df, schema=arrow_schema(spec), preserve_index=False
    )
    path = os.path.join(d, "data.parquet")
    pq.write_table(tbl, path, compression="snappy")
    return path, len(df)


def main():
    con = sqlite3.connect(DB)
    summary = []
    for name in TABLE_ORDER:
        spec = TABLES[name]
        if name == "dicionario":
            df = build_dicionario(con)
        else:
            df = build_table_df(con, spec)
        path, n = write_table(name, df, spec)
        summary.append((name, n, path))
        print(f"[{name:16}] {n:>9,} rows -> {path}")
    con.close()
    print("\nDONE")
    for name, n, _ in summary:
        print(f"  {name:16} {n:>9,}")


if __name__ == "__main__":
    main()
