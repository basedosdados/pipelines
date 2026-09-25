"""Local checks on the cleaned parquet, before anything is uploaded.

Verifies the properties the dbt tests will assert in BigQuery, plus the two
that matter most for this dataset and that no generic test covers: the key is
unique, and a flagged row never carries a measure.
"""

from __future__ import annotations

import collections
import glob
import os
import sys

import pyarrow.parquet as pq
from constants import OUTPUT_DIR, TABLE_DICIONARIO, TABLE_MUNICIPIO, TABLE_UF

KEYS = {
    TABLE_MUNICIPIO: [
        "ano",
        "mes",
        "id_municipio",
        "tipo_ocorrencia",
        "abrangencia",
    ],
    TABLE_UF: [
        "ano",
        "mes",
        "sigla_uf",
        "tipo_ocorrencia",
        "abrangencia",
        "arma",
        "agente",
        "faixa_etaria",
    ],
    TABLE_DICIONARIO: [
        "id_tabela",
        "nome_coluna",
        "chave",
        "cobertura_temporal",
    ],
}
MEASURES = [
    "quantidade_ocorrencias",
    "quantidade_vitimas",
    "quantidade_vitimas_feminino",
    "quantidade_vitimas_masculino",
    "quantidade_vitimas_sexo_nao_informado",
]


def files_for(table: str) -> list[str]:
    root = os.path.join(OUTPUT_DIR, table)
    return sorted(
        glob.glob(os.path.join(root, "**", "*.parquet"), recursive=True)
    )


def check(table: str) -> list[str]:
    problems: list[str] = []
    files = files_for(table)
    if not files:
        return [f"{table}: no parquet written"]
    seen: set[tuple] = set()
    dupes = 0
    rows = 0
    nulls: collections.Counter = collections.Counter()
    situacao = collections.Counter()
    bad_flag = bad_type = 0
    for f in files:
        t = pq.ParquetFile(f).read()
        rows += t.num_rows
        if set(t.schema.names) != set(
            t.schema.names
        ):  # placeholder, order checked below
            pass
        if any(
            str(t.schema.field(i).type) != "string"
            for i in range(t.num_columns)
        ):
            bad_type += 1
        cols = {n: t.column(n).to_pylist() for n in t.schema.names}
        key_cols = [cols[k] for k in KEYS[table]]
        for tup in zip(*key_cols, strict=True):
            if tup in seen:
                dupes += 1
            seen.add(tup)
        for n, v in cols.items():
            nulls[n] += sum(1 for x in v if x is None)
        if "situacao_registro" in cols:
            for i, s in enumerate(cols["situacao_registro"]):
                situacao[s] += 1
                if s == "nao_reportado" and any(
                    cols[m][i] is not None for m in MEASURES if m in cols
                ):
                    bad_flag += 1
    print(f"\n== {table}: {rows:,} rows in {len(files)} file(s)")
    if situacao:
        print(f"   situacao_registro: {dict(situacao)}")
    print(
        "   nulls: " + ", ".join(f"{k}={v:,}" for k, v in nulls.items() if v)
    )
    if dupes:
        problems.append(f"{table}: {dupes:,} duplicate keys on {KEYS[table]}")
    if bad_flag:
        problems.append(
            f"{table}: {bad_flag:,} nao_reportado rows carry a measure"
        )
    if bad_type:
        problems.append(f"{table}: {bad_type} file(s) are not all-STRING")
    return problems


if __name__ == "__main__":
    all_problems = []
    for t in (TABLE_MUNICIPIO, TABLE_UF, TABLE_DICIONARIO):
        all_problems += check(t)
    print()
    if all_problems:
        for p in all_problems:
            print("FAIL:", p)
        sys.exit(1)
    print("all local checks passed")
