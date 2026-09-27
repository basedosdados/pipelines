#!/usr/bin/env python3
"""Emit columns_json payloads for mcp__databasis__bulk_upsert_columns, one per table.

Reads the architecture CSVs (English descriptions + type/dictionary/unit flags)
and attaches Portuguese and Spanish translations from TRANSLATIONS below, so
columns can be registered directly (no Google Sheet). Writes
code/columns_json/<table>.json.

Usage:
    python models/us_epu_gpr/code/build_columns_json.py
"""

import csv
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
ARCH = ROOT / "code" / "architecture"
OUT = ROOT / "code" / "columns_json"

# name (or name:table) -> (description_pt, description_es). English comes from
# the architecture CSV.
TRANSLATIONS = {
    "year": (
        "Ano de referência da observação",
        "Año de referencia de la observación",
    ),
    "month": (
        "Mês de referência da observação, de 1 a 12",
        "Mes de referencia de la observación, de 1 a 12",
    ),
    "day": (
        "Dia do mês de referência, de 1 a 31",
        "Día del mes de referencia, de 1 a 31",
    ),
    "date": (
        "Data de calendário da observação",
        "Fecha de calendario de la observación",
    ),
    "country_id": (
        "Código ISO 3166-1 alfa-3 do país a que a série se refere; nulo para séries globais ou agregadas",
        "Código ISO 3166-1 alfa-3 del país al que se refiere la serie; nulo para series globales o agregadas",
    ),
    "index_family": (
        "Família do índice: epu (Incerteza da Política Econômica) ou gpr (Risco Geopolítico)",
        "Familia del índice: epu (Incertidumbre de la Política Económica) o gpr (Riesgo Geopolítico)",
    ),
    "index_name": (
        "Identificador codificado da série específica do índice; os rótulos estão na tabela dicionario",
        "Identificador codificado de la serie específica del índice; las etiquetas están en la tabla dicionario",
    ),
    "value:index_monthly": (
        "Valor do índice para a série e o período. Pontos de índice (base = 100 na janela de referência da série) para EPU e GPR global; participação de artigos com foco geopolítico em porcentagem para o GPR nacional",
        "Valor del índice para la serie y el período. Puntos de índice (base = 100 en la ventana de referencia de la serie) para EPU y GPR global; participación de artículos con enfoque geopolítico en porcentaje para el GPR nacional",
    ),
    "value:index_daily": (
        "Valor do índice para a série e a data. Pontos de índice (base = 100 na janela de referência da série)",
        "Valor del índice para la serie y la fecha. Puntos de índice (base = 100 en la ventana de referencia de la serie)",
    ),
    "id_tabela": (
        "Slug da tabela de us_epu_gpr que a entrada do dicionário descreve",
        "Slug de la tabla de us_epu_gpr que describe la entrada del diccionario",
    ),
    "nome_coluna": (
        "Nome da coluna que a entrada do dicionário descreve",
        "Nombre de la columna que describe la entrada del diccionario",
    ),
    "chave": (
        "Valor codificado (chave) exatamente como armazenado nos dados",
        "Valor codificado (clave) exactamente como se almacena en los datos",
    ),
    "cobertura_temporal": (
        "Cobertura temporal da chave",
        "Cobertura temporal de la clave",
    ),
    "valor": (
        "Rótulo legível correspondente ao valor codificado",
        "Etiqueta legible correspondiente al valor codificado",
    ),
}


def tr(name, table):
    return TRANSLATIONS.get(f"{name}:{table}") or TRANSLATIONS[name]


def main():
    OUT.mkdir(parents=True, exist_ok=True)
    for csv_path in sorted(ARCH.glob("*.csv")):
        table = csv_path.stem
        cols = []
        with open(csv_path, newline="") as fh:
            rows = list(csv.DictReader(fh))
        for r in rows:
            pt, es = tr(r["name"], table)
            col = {
                "name": r["name"],
                "bigquery_type": r["bigquery_type"],
                "description_pt": pt,
                "description_en": r["description"],
                "description_es": es,
                "covered_by_dictionary": r["covered_by_dictionary"]
                .strip()
                .lower()
                == "yes",
                "has_sensitive_data": r["has_sensitive_data"].strip().lower()
                == "yes",
            }
            if r["directory_column"].strip():
                col["directory_column"] = r["directory_column"].strip()
            if r["measurement_unit"].strip():
                col["measurement_unit"] = r["measurement_unit"].strip()
            cols.append(col)
        (OUT / f"{table}.json").write_text(
            json.dumps(cols, ensure_ascii=False, indent=2)
        )
        print(
            f"{table}: {len(cols)} columns -> code/columns_json/{table}.json"
        )


if __name__ == "__main__":
    main()
