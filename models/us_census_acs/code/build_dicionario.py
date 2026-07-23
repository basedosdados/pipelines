#!/usr/bin/env python3
"""Build the DB-standard `dicionario` for PUMS coded columns.

For every covered_by_dictionary=yes column in the person/household architecture,
collect its value->label pairs from the CSV data dictionaries (union across
years; categorical labels are stable, so cobertura_temporal is left blank =
same as table). Output: output/dicionario/data.parquet.

Standard schema: id_tabela, nome_coluna, chave, cobertura_temporal, valor.
"""

import csv
import glob
import os

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

PUMS = "/Users/rdahis/acs_data/pums"
ARCH = os.path.join(os.path.dirname(__file__), "architecture")
OUT = "/Users/rdahis/acs_data/output/dicionario"
RENAME = {"ST": "STATE", "BDS": "BDSP", "RMS": "RMSP", "VAL": "VALP"}

# covered columns: original_name(upper) -> (id_tabela, name)
covered = {}
for kind in ("microdata_person", "microdata_household"):
    with open(f"{ARCH}/{kind}.csv") as fh:
        for r in csv.DictReader(fh):
            if r["covered_by_dictionary"] == "yes" and r["original_name"]:
                covered[r["original_name"].upper()] = (kind, r["name"])

# collect (id_tabela, nome_coluna, chave, valor) from VAL rows
seen = {}  # (tabela, col, chave, valor) -> True
rows = []
for path in sorted(glob.glob(f"{PUMS}/dict/PUMS_Data_Dictionary_*.csv")):
    with open(path, newline="", encoding="latin-1") as f:
        for row in csv.reader(f):
            if not row or row[0].strip() != "VAL" or len(row) < 7:
                continue
            var = RENAME.get(row[1].strip().upper(), row[1].strip().upper())
            if var not in covered:
                continue
            minv, maxv, label = row[4].strip(), row[5].strip(), row[6].strip()
            if not label or label.lower() in ("n/a", ""):
                continue
            chave = minv if minv == maxv else f"{minv}-{maxv}"
            tabela, col = covered[var]
            key = (tabela, col, chave, label)
            if key in seen:
                continue
            seen[key] = True
            rows.append(
                {
                    "id_tabela": tabela,
                    "nome_coluna": col,
                    "chave": chave,
                    "cobertura_temporal": "",
                    "valor": label,
                }
            )

df = pd.DataFrame(rows).astype("string")
os.makedirs(OUT, exist_ok=True)
schema = pa.schema(
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
pq.write_table(
    pa.Table.from_pandas(
        df[
            [
                "id_tabela",
                "nome_coluna",
                "chave",
                "cobertura_temporal",
                "valor",
            ]
        ],
        schema=schema,
        preserve_index=False,
    ),
    f"{OUT}/data.parquet",
    compression="snappy",
)
print(
    f"dicionario: {len(df):,} rows across {df.nome_coluna.nunique()} columns "
    f"({df[df.id_tabela == 'microdata_person'].nome_coluna.nunique()} person, "
    f"{df[df.id_tabela == 'microdata_household'].nome_coluna.nunique()} household)"
)
print("covered columns expected:", len(covered))
print(df.head(6).to_string())
# report covered columns that got NO labels (need legacy .txt dicts)
got = set(df.nome_coluna.unique())
missing = [(v, covered[v][1]) for v in covered if covered[v][1] not in got]
print(
    f"\ncovered cols with NO labels from CSV dicts ({len(missing)}): {[m[1] for m in missing][:40]}"
)
