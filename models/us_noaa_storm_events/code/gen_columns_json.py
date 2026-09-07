"""Write the bulk_upsert_columns payload for each table, from the architecture TSVs.

The backend metadata is generated from the same TSVs as the transform and the dbt
models, so the three cannot describe different schemas.

Run: uv run python models/us_noaa_storm_events/code/gen_columns_json.py [out_dir]
"""

import json
import sys
from pathlib import Path

from common import DATA_TABLES, load_cols

DICIONARIO_COLUMNS = [
    {
        "name": "id_tabela",
        "bigquery_type": "STRING",
        "description_pt": "Nome da tabela a que a coluna codificada pertence",
        "description_en": "Name of the table the coded column belongs to",
        "description_es": "Nombre de la tabla a la que pertenece la columna codificada",
    },
    {
        "name": "nome_coluna",
        "bigquery_type": "STRING",
        "description_pt": "Nome da coluna codificada",
        "description_en": "Name of the coded column",
        "description_es": "Nombre de la columna codificada",
    },
    {
        "name": "chave",
        "bigquery_type": "STRING",
        "description_pt": "Valor armazenado na coluna",
        "description_en": "Value stored in the column",
        "description_es": "Valor almacenado en la columna",
    },
    {
        "name": "cobertura_temporal",
        "bigquery_type": "STRING",
        "description_pt": (
            "Anos em que o valor aparece, na notação início(intervalo)fim"
        ),
        "description_en": (
            "Years over which the value appears, in start(interval)end notation"
        ),
        "description_es": (
            "Años en que aparece el valor, en la notación inicio(intervalo)fin"
        ),
    },
    {
        "name": "valor",
        "bigquery_type": "STRING",
        "description_pt": "Significado do valor armazenado",
        "description_en": "Meaning of the stored value",
        "description_es": "Significado del valor almacenado",
    },
]


def payload(table: str) -> list[dict]:
    out = []
    for c in load_cols(table):
        entry = {
            "name": c.name,
            "bigquery_type": c.bq_type,
            "description_pt": c.description_pt,
            "description_en": c.description_en,
            "description_es": c.description_es,
            "covered_by_dictionary": c.covered_by_dictionary,
            "has_sensitive_data": c.has_sensitive_data,
        }
        if c.directory_column:
            entry["directory_column"] = c.directory_column
        if c.measurement_unit:
            entry["measurement_unit"] = c.measurement_unit
        if c.observations_pt:
            entry["observations_pt"] = c.observations_pt
            entry["observations_en"] = c.observations_en
            entry["observations_es"] = c.observations_es
        out.append(entry)
    return out


def main() -> None:
    out_dir = Path(sys.argv[1]) if len(sys.argv) > 1 else Path(".")
    out_dir.mkdir(parents=True, exist_ok=True)
    for table in DATA_TABLES:
        cols = payload(table)
        path = out_dir / f"columns_{table}.json"
        path.write_text(json.dumps(cols, ensure_ascii=False, indent=1))
        print(f"wrote {path} ({len(cols)} columns)")
    path = out_dir / "columns_dicionario.json"
    path.write_text(
        json.dumps(DICIONARIO_COLUMNS, ensure_ascii=False, indent=1)
    )
    print(f"wrote {path} ({len(DICIONARIO_COLUMNS)} columns)")


if __name__ == "__main__":
    main()
