#!/usr/bin/env python3
"""
Emit columns_json payloads for mcp__databasis__bulk_upsert_columns, one per table.

Reads the architecture CSVs (English descriptions + type/dictionary/unit flags)
and attaches Portuguese and Spanish translations from TRANSLATIONS below, so
columns can be registered directly (no Google Sheet). Writes
code/columns_json/<table>.json.

Usage:
    python models/us_bls_cpi/code/build_columns_json.py
"""

import csv
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
ARCH = ROOT / "code" / "architecture"
OUT = ROOT / "code" / "columns_json"

# name -> (description_pt, description_es). English comes from the architecture CSV.
TRANSLATIONS = {
    "year": (
        "Ano de referência da observação do índice",
        "Año de referencia de la observación del índice",
    ),
    "month": (
        "Mês de referência da observação do índice, de 1 a 12",
        "Mes de referencia de la observación del índice, de 1 a 12",
    ),
    "half": (
        "Semestre do ano para séries semestrais, 1 para o primeiro semestre e 2 para o segundo semestre",
        "Semestre del año para series semestrales, 1 para el primer semestre y 2 para el segundo semestre",
    ),
    "survey": (
        "População da pesquisa do IPC: CPI-U para todos os consumidores urbanos ou CPI-W para assalariados urbanos e trabalhadores administrativos",
        "Población de la encuesta del IPC: CPI-U para todos los consumidores urbanos o CPI-W para asalariados urbanos y trabajadores administrativos",
    ),
    "seasonal_adjustment": (
        "Indica se a série é ajustada sazonalmente (S) ou não ajustada sazonalmente (U)",
        "Indica si la serie está ajustada estacionalmente (S) o no ajustada estacionalmente (U)",
    ),
    "area_id": (
        "Código de área do IPC do BLS que identifica a cobertura geográfica da série",
        "Código de área del IPC del BLS que identifica la cobertura geográfica de la serie",
    ),
    "item_id": (
        "Código de item do IPC do BLS que identifica a categoria de despesa da série",
        "Código de ítem del IPC del BLS que identifica la categoría de gasto de la serie",
    ),
    "base_period": (
        "Período base de referência do índice no qual o índice é igual a 100, que varia por série",
        "Período base de referencia del índice en el que el índice es igual a 100, que varía según la serie",
    ),
    "index_value:monthly": (
        "Nível mensal do Índice de Preços ao Consumidor para o item, área e período, relativo ao período base",
        "Nivel mensual del Índice de Precios al Consumidor para el ítem, área y período, relativo al período base",
    ),
    "index_value:annual": (
        "Nível médio anual do Índice de Preços ao Consumidor para o item, área e ano, relativo ao período base",
        "Nivel promedio anual del Índice de Precios al Consumidor para el ítem, área y año, relativo al período base",
    ),
    "index_value:semiannual": (
        "Nível semestral do Índice de Preços ao Consumidor para o item, área e semestre, relativo ao período base",
        "Nivel semestral del Índice de Precios al Consumidor para el ítem, área y semestre, relativo al período base",
    ),
    "monthly_change": (
        "Variação percentual do índice em relação ao mês anterior, calculada pela Data Basis",
        "Variación porcentual del índice respecto al mes anterior, calculada por Data Basis",
    ),
    "twelve_month_change": (
        "Variação percentual do índice em relação ao mesmo mês do ano anterior, calculada pela Data Basis",
        "Variación porcentual del índice respecto al mismo mes del año anterior, calculada por Data Basis",
    ),
    "annual_change": (
        "Variação percentual da média anual do índice em relação ao ano anterior, calculada pela Data Basis",
        "Variación porcentual del promedio anual del índice respecto al año anterior, calculada por Data Basis",
    ),
    "semiannual_change": (
        "Variação percentual do índice em relação ao semestre anterior, calculada pela Data Basis",
        "Variación porcentual del índice respecto al semestre anterior, calculada por Data Basis",
    ),
    "id_tabela": (
        "Slug da tabela de us_bls_cpi que a entrada do dicionário descreve",
        "Slug de la tabla de us_bls_cpi que describe la entrada del diccionario",
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
