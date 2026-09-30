"""Emit columns_json payloads for bulk_upsert_columns, from the architecture CSVs.

bulk_upsert_columns matches by column NAME and sets the type, descriptions,
dictionary coverage, measurement unit, directory link and observations, so no
Google Sheet and no column UUIDs are needed. It does NOT set is_partition --
that still takes a per-column update_column call.

Descriptions are written in all three languages. Spanish is the source language
here (the data and INDEC's documentation are Spanish), so description_es carries
the original and the Portuguese and English are translations.
"""

import csv
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from constants import ARCH_DIR, CODE_DIR, TABLES


def payload(table: str) -> list[dict]:
    with open(ARCH_DIR / f"{table}.csv", encoding="utf-8") as handle:
        rows = list(csv.DictReader(handle))
    out = []
    for row in rows:
        entry = {
            "name": row["name"],
            "bigquery_type": row["bigquery_type"],
            "description_pt": row["description_pt"],
            "description_en": row["description_en"],
            "description_es": row["description_es"],
            "covered_by_dictionary": row["covered_by_dictionary"],
            "measurement_unit": row["measurement_unit"],
            "has_sensitive_data": row["has_sensitive_data"],
            "observations_pt": row["observations_pt"],
            "observations_en": row["observations_en"],
            "observations_es": row["observations_es"],
            "temporal_coverage": row["temporal_coverage"],
            "directory_column": row["directory_column"],
            "original_name": row["original_name"],
        }
        out.append({k: v for k, v in entry.items() if v != ""})
    return out


def main() -> int:
    for table in TABLES:
        rows = payload(table)
        path = CODE_DIR / f"columns_{table}.json"
        path.write_text(
            json.dumps(rows, ensure_ascii=False, indent=1), encoding="utf-8"
        )
        print(f"{table}: {len(rows)} columns -> {path.name}")
    dicionario = [
        {
            "name": "id_tabela",
            "bigquery_type": "STRING",
            "description_es": "Nombre de la tabla de microdatos a la que se refiere la fila",
            "description_pt": "Nome da tabela de microdados a que se refere a linha",
            "description_en": "Name of the microdata table the row refers to",
        },
        {
            "name": "nome_coluna",
            "bigquery_type": "STRING",
            "description_es": "Nombre de la columna codificada",
            "description_pt": "Nome da coluna codificada",
            "description_en": "Name of the coded column",
        },
        {
            "name": "chave",
            "bigquery_type": "STRING",
            "description_es": "Codigo almacenado en la columna",
            "description_pt": "Codigo armazenado na coluna",
            "description_en": "Code stored in the column",
        },
        {
            "name": "cobertura_temporal",
            "bigquery_type": "STRING",
            "description_es": "Periodo de vigencia del codigo",
            "description_pt": "Periodo de vigencia do codigo",
            "description_en": "Period over which the code applies",
        },
        {
            "name": "valor",
            "bigquery_type": "STRING",
            "description_es": "Etiqueta del codigo, en espanol, tal como la publica el INDEC",
            "description_pt": "Rotulo do codigo, em espanhol, tal como o INDEC publica",
            "description_en": "The code's label, in Spanish, as INDEC publishes it",
        },
    ]
    path = CODE_DIR / "columns_dicionario.json"
    path.write_text(
        json.dumps(dicionario, ensure_ascii=False, indent=1), encoding="utf-8"
    )
    print(f"dicionario: {len(dicionario)} columns -> {path.name}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
