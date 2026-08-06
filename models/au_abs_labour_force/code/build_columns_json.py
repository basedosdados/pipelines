#!/usr/bin/env python3
"""
Emit columns_json payloads for mcp__databasis__bulk_upsert_columns, one per table.

Reads the architecture CSVs (English descriptions + type/dictionary/unit flags)
and attaches Portuguese and Spanish translations from TRANSLATIONS below, so
columns register directly with all three languages (no Google Sheet — the sheet
path drops description_en for English-source datasets). Writes
code/columns_json/<table>.json.

Keys in TRANSLATIONS are the column name, or "name:table" when the same column
name carries a different meaning across tables.

Usage:
    python models/au_abs_labour_force/code/build_columns_json.py
"""

import csv
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
ARCH = ROOT / "code" / "architecture"
OUT = ROOT / "code" / "columns_json"

# name (or name:table) -> (description_pt, description_es). English from the CSV.
TRANSLATIONS = {
    "year": (
        "Ano de referência da observação",
        "Año de referencia de la observación",
    ),
    "month": (
        "Mês de referência da observação, de 1 a 12",
        "Mes de referencia de la observación, de 1 a 12",
    ),
    "geography": (
        "Área geográfica: Austrália (nacional) ou um dos oito estados e territórios",
        "Área geográfica: Australia (nacional) o uno de los ocho estados y territorios",
    ),
    "sex": (
        "Sexo: pessoas (total), homens ou mulheres",
        "Sexo: personas (total), hombres o mujeres",
    ),
    "age_group": (
        "Faixa etária em anos; total abrange pessoas com 15 anos ou mais",
        "Grupo de edad en años; total abarca personas de 15 años o más",
    ),
    "adjustment_type": (
        "Ajuste da série temporal: original, ajustada sazonalmente ou tendência",
        "Ajuste de la serie temporal: original, ajustada estacionalmente o tendencia",
    ),
    # labour_force_status measures
    "employed_total": (
        "Número de pessoas ocupadas",
        "Número de personas ocupadas",
    ),
    "employed_full_time": (
        "Número de pessoas ocupadas em tempo integral",
        "Número de personas ocupadas a tiempo completo",
    ),
    "employed_part_time": (
        "Número de pessoas ocupadas em tempo parcial",
        "Número de personas ocupadas a tiempo parcial",
    ),
    "unemployed_total": (
        "Número de pessoas desempregadas",
        "Número de personas desempleadas",
    ),
    "unemployed_looked_for_full_time": (
        "Número de pessoas desempregadas que procuraram trabalho em tempo integral",
        "Número de personas desempleadas que buscaron trabajo a tiempo completo",
    ),
    "unemployed_looked_for_part_time": (
        "Número de pessoas desempregadas que procuraram apenas trabalho em tempo parcial",
        "Número de personas desempleadas que buscaron solo trabajo a tiempo parcial",
    ),
    "labour_force_total": (
        "Força de trabalho total, ocupados mais desempregados",
        "Fuerza laboral total, ocupados más desempleados",
    ),
    "not_in_labour_force": (
        "Número de pessoas fora da força de trabalho",
        "Número de personas fuera de la fuerza laboral",
    ),
    "civilian_population_15_over": (
        "População civil com 15 anos ou mais",
        "Población civil de 15 años o más",
    ),
    "unemployment_rate": (
        "Pessoas desempregadas como porcentagem da força de trabalho",
        "Personas desempleadas como porcentaje de la fuerza laboral",
    ),
    "unemployment_rate_looked_for_full_time": (
        "Taxa de desemprego entre quem procurou trabalho em tempo integral",
        "Tasa de desempleo entre quienes buscaron trabajo a tiempo completo",
    ),
    "unemployment_rate_looked_for_part_time": (
        "Taxa de desemprego entre quem procurou apenas trabalho em tempo parcial",
        "Tasa de desempleo entre quienes buscaron solo trabajo a tiempo parcial",
    ),
    "participation_rate": (
        "Força de trabalho como porcentagem da população civil com 15 anos ou mais",
        "Fuerza laboral como porcentaje de la población civil de 15 años o más",
    ),
    "employment_to_population_ratio": (
        "Pessoas ocupadas como porcentagem da população civil com 15 anos ou mais",
        "Personas ocupadas como porcentaje de la población civil de 15 años o más",
    ),
    # hours_worked
    "hours_band": (
        "Faixa de horas efetivamente trabalhadas em todos os empregos na semana de referência",
        "Rango de horas efectivamente trabajadas en todos los empleos en la semana de referencia",
    ),
    "employed_persons": (
        "Número de pessoas ocupadas na faixa de horas trabalhadas",
        "Número de personas ocupadas en el rango de horas trabajadas",
    ),
    "hours_worked": (
        "Total de horas efetivamente trabalhadas em todos os empregos",
        "Total de horas efectivamente trabajadas en todos los empleos",
    ),
    "hours_per_person": (
        "Média de horas efetivamente trabalhadas por pessoa ocupada",
        "Promedio de horas efectivamente trabajadas por persona ocupada",
    ),
    # status_in_employment
    "status_in_employment": (
        "Posição na ocupação do trabalho principal, como empregado, empregador ou trabalhador familiar auxiliar",
        "Situación en el empleo del trabajo principal, como asalariado, empleador o trabajador familiar auxiliar",
    ),
    "employed_total:status_in_employment": (
        "Número de pessoas ocupadas com esta posição na ocupação",
        "Número de personas ocupadas con esta situación en el empleo",
    ),
    "employed_full_time:status_in_employment": (
        "Número de pessoas ocupadas em tempo integral com esta posição na ocupação",
        "Número de personas ocupadas a tiempo completo con esta situación en el empleo",
    ),
    "employed_part_time:status_in_employment": (
        "Número de pessoas ocupadas em tempo parcial com esta posição na ocupação",
        "Número de personas ocupadas a tiempo parcial con esta situación en el empleo",
    ),
    # underutilisation
    "underemployed_total": (
        "Número de pessoas subocupadas",
        "Número de personas subempleadas",
    ),
    "underemployment_ratio": (
        "Pessoas subocupadas como porcentagem das pessoas ocupadas",
        "Personas subempleadas como porcentaje de las personas ocupadas",
    ),
    "underemployment_rate": (
        "Pessoas subocupadas como porcentagem da força de trabalho",
        "Personas subempleadas como porcentaje de la fuerza laboral",
    ),
    "underutilisation_rate": (
        "Soma da taxa de desemprego e da taxa de subocupação",
        "Suma de la tasa de desempleo y la tasa de subempleo",
    ),
}


def tr(name, table):
    if f"{name}:{table}" in TRANSLATIONS:
        return TRANSLATIONS[f"{name}:{table}"]
    if name not in TRANSLATIONS:
        raise KeyError(f"missing translation for {name!r} (table {table})")
    return TRANSLATIONS[name]


def main():
    OUT.mkdir(parents=True, exist_ok=True)
    for csv_path in sorted(ARCH.glob("*.csv")):
        table = csv_path.stem
        cols = []
        with csv_path.open() as fh:
            for r in csv.DictReader(fh):
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
                    "has_sensitive_data": r["has_sensitive_data"]
                    .strip()
                    .lower()
                    == "yes",
                }
                if r["directory_column"].strip():
                    col["directory_column"] = r["directory_column"].strip()
                if r["measurement_unit"].strip():
                    col["measurement_unit"] = r["measurement_unit"].strip()
                cols.append(col)
        out_path = OUT / f"{table}.json"
        out_path.write_text(json.dumps(cols, ensure_ascii=False, indent=2))
        print(f"wrote {out_path}  ({len(cols)} columns)")


if __name__ == "__main__":
    main()
