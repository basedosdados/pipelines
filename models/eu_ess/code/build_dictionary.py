#!/usr/bin/env python3
"""Build the eu_ess `dicionario` table from the ESS Stata value labels.

For every column flagged covered_by_dictionary=yes in a round's architecture,
emit one row per coded value: (id_tabela, nome_coluna, chave, cobertura_temporal,
valor). `chave` is the stored code as a string (matching clean_data's output);
Stata extended-missing markers (single letters a-z, read as null in the data) are
skipped. Output is a single non-partitioned parquet: output/dicionario/data.parquet.

Usage: uv run python models/eu_ess/code/build_dictionary.py
"""

import csv
import glob
import re
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pyreadstat

ROOT = Path(__file__).resolve().parents[1]
INPUT_DIR = ROOT / "input"
OUTPUT_DIR = ROOT / "output"
ARCH_DIR = Path(__file__).resolve().parent / "architecture"

YEAR = {
    1: 2002,
    2: 2004,
    3: 2006,
    4: 2008,
    5: 2010,
    6: 2012,
    7: 2014,
    8: 2016,
    9: 2018,
    10: 2020,
    11: 2023,
}

COLS = ["id_tabela", "nome_coluna", "chave", "cobertura_temporal", "valor"]


def value_labels(round_n):
    matches = sorted(glob.glob(str(INPUT_DIR / f"ESS{round_n}e*.dta")))
    if not matches:
        return None
    _, meta = pyreadstat.read_dta(matches[0], metadataonly=True)
    return {k.lower(): v for k, v in meta.variable_value_labels.items()}


def coded_columns(round_n):
    path = ARCH_DIR / f"round_{round_n:02d}.csv"
    with open(path, newline="", encoding="utf-8") as f:
        rows = [r for r in csv.DictReader(f) if r["name"].strip()]
    return [
        (r["name"].strip(), r["original_name"].strip())
        for r in rows
        if r["covered_by_dictionary"].strip() == "yes"
        and r["original_name"].strip()
    ]


def chave_of(code):
    if isinstance(code, bool):
        return None
    if isinstance(code, int):
        return str(code)
    if isinstance(code, float):
        return str(int(code)) if code.is_integer() else None
    s = str(code).strip()
    if re.fullmatch(
        r"[a-z]", s
    ):  # Stata extended-missing marker -> null in data
        return None
    return s or None


def main():
    out = []
    missing_labels = 0
    for n in range(1, 12):
        vl = value_labels(n)
        if vl is None:
            print(f"round {n:02d}: no .dta — skipping")
            continue
        yr = str(YEAR[n])
        cols = coded_columns(n)
        for name, mn in cols:
            labels = vl.get(mn, {})
            if not labels:
                missing_labels += 1
                continue
            for code, label in labels.items():
                ch = chave_of(code)
                if ch is None:
                    continue
                out.append(
                    {
                        "id_tabela": f"round_{n:02d}",
                        "nome_coluna": name,
                        "chave": ch,
                        "cobertura_temporal": yr,
                        "valor": str(label).strip(),
                    }
                )

    df = (
        pd.DataFrame(out, columns=COLS)
        .drop_duplicates(
            subset=["id_tabela", "nome_coluna", "chave"], keep="first"
        )
        .reset_index(drop=True)
    )

    schema = pa.schema([(c, pa.string()) for c in COLS])
    table = pa.Table.from_pandas(df, schema=schema, preserve_index=False)
    table = table.replace_schema_metadata(None)
    dest = OUTPUT_DIR / "dicionario"
    dest.mkdir(parents=True, exist_ok=True)
    pq.write_table(table, dest / "data.parquet", compression="snappy")

    print(
        f"dicionario: {len(df):,} rows -> {dest.relative_to(ROOT)}/data.parquet"
    )
    print(
        f"  distinct columns decoded: {df['nome_coluna'].nunique()} | "
        f"per round: {df.groupby('id_tabela').size().to_dict()}"
    )
    print(
        f"  dict=yes columns with NO value labels in .dta (skipped): {missing_labels}"
    )


if __name__ == "__main__":
    main()
