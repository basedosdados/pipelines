#!/usr/bin/env python3
"""Build local architecture CSVs for af_afrobarometer_survey from meta_cache.json.

One CSV per round table (round1..round9) plus dicionario.csv, each with the
Data Basis architecture columns:

    name, bigquery_type, description, temporal_coverage, covered_by_dictionary,
    directory_column, measurement_unit, has_sensitive_data, observations,
    original_name

Descriptions are the SPSS variable labels (English, the source language — the
same convention as world_oecd_pisa / world_iea_pirls). Column order matches the
cleaned Parquet exactly (architecture table is the source of truth downstream).

Usage:
    .venv/bin/python models/af_afrobarometer_survey/code/build_architecture.py
"""

from __future__ import annotations

import csv
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from common import ARCH_DIR, META_CACHE, ROUNDS

ARCH_COLUMNS = [
    "name",
    "bigquery_type",
    "description",
    "temporal_coverage",
    "covered_by_dictionary",
    "directory_column",
    "measurement_unit",
    "has_sensitive_data",
    "observations",
    "original_name",
]

# Standard Data Basis dicionario schema (5 fixed columns).
DICIONARIO_ROWS = [
    ("id_tabela", "STRING", "Nome da tabela à qual a coluna pertence."),
    ("nome_coluna", "STRING", "Nome da coluna coberta pelo dicionário."),
    (
        "chave",
        "STRING",
        "Valor codificado (chave) presente na tabela de dados.",
    ),
    ("cobertura_temporal", "STRING", "Cobertura temporal da chave."),
    ("valor", "STRING", "Significado (rótulo) correspondente à chave."),
]


def write_csv(path: Path, rows: list[dict]) -> None:
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=ARCH_COLUMNS)
        w.writeheader()
        for r in rows:
            w.writerow(r)


def build_round(rm: dict) -> int:
    rows = []
    for col in rm["columns"]:
        rows.append(
            {
                "name": col["name"],
                "bigquery_type": col["bigquery_type"],
                "description": col["description"],
                "temporal_coverage": "",  # same as table
                "covered_by_dictionary": col["covered_by_dictionary"],
                "directory_column": col.get("directory_column", ""),
                "measurement_unit": "",
                "has_sensitive_data": "no",
                "observations": "",
                "original_name": col["original_name"],
            }
        )
    write_csv(ARCH_DIR / f"{rm['slug']}.csv", rows)
    return len(rows)


def build_dicionario() -> int:
    rows = []
    for name, btype, desc in DICIONARIO_ROWS:
        rows.append(
            {
                "name": name,
                "bigquery_type": btype,
                "description": desc,
                "temporal_coverage": "",
                "covered_by_dictionary": "no",
                "directory_column": "",
                "measurement_unit": "",
                "has_sensitive_data": "no",
                "observations": "",
                "original_name": "",
            }
        )
    write_csv(ARCH_DIR / "dicionario.csv", rows)
    return len(rows)


def main():
    cache = json.loads(META_CACHE.read_text())
    ARCH_DIR.mkdir(parents=True, exist_ok=True)
    lines = [
        "# Architecture — af_afrobarometer_survey",
        "",
        "| table | columns | covered_by_dictionary |",
        "|-------|---------|-----------------------|",
    ]
    total = 0
    for r in ROUNDS:
        rm = cache.get(r["slug"])
        if not rm:
            continue
        n = build_round(rm)
        cov = sum(
            1 for c in rm["columns"] if c["covered_by_dictionary"] == "yes"
        )
        total += n
        lines.append(f"| {r['slug']} | {n} | {cov} |")
    nd = build_dicionario()
    lines.append(f"| dicionario | {nd} | 0 |")
    lines.append(f"| **total** | {total + nd} | |")
    (ARCH_DIR / "COVERAGE.md").write_text("\n".join(lines) + "\n")
    print("\n".join(lines))


if __name__ == "__main__":
    main()
