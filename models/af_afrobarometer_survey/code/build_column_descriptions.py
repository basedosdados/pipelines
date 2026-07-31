#!/usr/bin/env python3
"""Build column_descriptions.json: per-table trilingual (en/pt/es) descriptions
for every column, from the architecture CSVs + translations.json.

Round-table descriptions are English (SPSS labels) with pt/es from
translations.json. The 5 dicionario columns carry hand-written trilingual text
(their architecture description is Portuguese, not an English source string).

Usage: .venv/bin/python models/af_afrobarometer_survey/code/build_column_descriptions.py
"""

from __future__ import annotations

import csv
import json
from pathlib import Path

CODE = Path(__file__).resolve().parent
ARCH = CODE / "architecture"
TRANS = json.loads((CODE / "translations.json").read_text(encoding="utf-8"))

ROUND_SLUGS = [f"round{i}" for i in range(1, 10)]

DICIONARIO = {
    "id_tabela": {
        "en": "Name of the table the column belongs to.",
        "pt": "Nome da tabela à qual a coluna pertence.",
        "es": "Nombre de la tabla a la que pertenece la columna.",
    },
    "nome_coluna": {
        "en": "Name of the column covered by the dictionary.",
        "pt": "Nome da coluna coberta pelo dicionário.",
        "es": "Nombre de la columna cubierta por el diccionario.",
    },
    "chave": {
        "en": "Coded value (key) present in the data table.",
        "pt": "Valor codificado (chave) presente na tabela de dados.",
        "es": "Valor codificado (clave) presente en la tabla de datos.",
    },
    "cobertura_temporal": {
        "en": "Temporal coverage of the key.",
        "pt": "Cobertura temporal da chave.",
        "es": "Cobertura temporal de la clave.",
    },
    "valor": {
        "en": "Meaning (label) corresponding to the key.",
        "pt": "Significado (rótulo) correspondente à chave.",
        "es": "Significado (etiqueta) correspondiente a la clave.",
    },
}


def rows(table):
    with open(ARCH / f"{table}.csv", newline="", encoding="utf-8") as f:
        return [r for r in csv.DictReader(f) if r["name"].strip()]


def main():
    out = {}
    missing = []
    for slug in ROUND_SLUGS:
        cols = []
        for r in rows(slug):
            en = r["description"].strip()
            t = TRANS.get(en)
            if not t:
                missing.append((slug, r["name"], en))
                pt = es = en
            else:
                pt, es = t["pt"], t["es"]
            cols.append(
                {
                    "name": r["name"].strip(),
                    "en": en,
                    "pt": pt,
                    "es": es,
                    "covered_by_dictionary": r["covered_by_dictionary"].strip()
                    == "yes",
                    "directory_column": r["directory_column"].strip(),
                }
            )
        out[slug] = cols
    # dicionario: explicit trilingual, no dictionary/directory links
    out["dicionario"] = [
        {
            "name": r["name"].strip(),
            **DICIONARIO[r["name"].strip()],
            "covered_by_dictionary": False,
            "directory_column": "",
        }
        for r in rows("dicionario")
    ]

    total = sum(len(v) for v in out.values())
    path = CODE / "column_descriptions.json"
    path.write_text(json.dumps(out, ensure_ascii=False, indent=1))
    print(
        f"wrote {path.name}: {total} columns across {len(out)} tables; "
        f"missing translations: {len(missing)}"
    )
    if missing:
        print("  MISSING:", missing[:10])


if __name__ == "__main__":
    main()
