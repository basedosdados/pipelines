#!/usr/bin/env python3
"""Emit columns_json payloads for mcp__databasis__bulk_upsert_columns, one per table.

Reads the architecture CSVs (English descriptions + type/dictionary/unit flags)
and attaches Portuguese and Spanish translations from TRANSLATIONS below, so
columns register without a Google Sheet. Writes code/columns_json/<table>.json.

Usage: uv run python models/au_nsw_bocsar_crime/code/build_columns_json.py
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
    "date": (
        "Data de calendário dos incidentes registrados",
        "Fecha de calendario de los incidentes registrados",
    ),
    "offence_category": (
        "Categoria de crime do BOCSAR, o nível superior da classificação de crimes",
        "Categoría de delito del BOCSAR, el nivel superior de la clasificación de delitos",
    ),
    "offence_subcategory": (
        "Subcategoria de crime do BOCSAR, o tipo detalhado de crime dentro da categoria",
        "Subcategoría de delito del BOCSAR, el tipo detallado de delito dentro de la categoría",
    ),
    "offence_category:criminal_incidents_daily": (
        "Categoria de crime do BOCSAR mapeada a partir do rótulo de crime publicado",
        "Categoría de delito del BOCSAR asignada a partir de la etiqueta de delito publicada",
    ),
    "offence_subcategory:criminal_incidents_daily": (
        "Rótulo de crime como publicado no arquivo diário, folha da hierarquia de crimes",
        "Etiqueta de delito según se publica en el archivo diario, hoja de la jerarquía de delitos",
    ),
    "incidents": (
        "Número de incidentes criminais registrados pela Polícia de Nova Gales do Sul",
        "Número de incidentes delictivos registrados por la Policía de Nueva Gales del Sur",
    ),
    "sa4_name": (
        "Nome da Área Estatística de Nível 4 (ASGS) em Nova Gales do Sul",
        "Nombre del Área Estadística de Nivel 4 (ASGS) en Nueva Gales del Sur",
    ),
    "lga_name": (
        "Nome da Área de Governo Local (LGA) em Nova Gales do Sul",
        "Nombre del Área de Gobierno Local (LGA) en Nueva Gales del Sur",
    ),
    "postcode": (
        "Código postal australiano dos incidentes registrados",
        "Código postal australiano de los incidentes registrados",
    ),
    "suburb": (
        "Nome do subúrbio em Nova Gales do Sul",
        "Nombre del suburbio en Nueva Gales del Sur",
    ),
    "financial_year": (
        "Ano fiscal australiano de referência, de 1 de julho a 30 de junho, por exemplo 2010-11",
        "Año fiscal australiano de referencia, del 1 de julio al 30 de junio, por ejemplo 2010-11",
    ),
    "age_group": (
        "Faixa etária da pessoa de interesse (10 a 17 anos, ou adulto)",
        "Grupo de edad de la persona de interés (10 a 17 años, o adulto)",
    ),
    "legal_proceeding": (
        "Método de procedimento legal (encaminhamento alternativo ou processo judicial)",
        "Método de procedimiento legal (derivación alternativa o proceso judicial)",
    ),
    "detailed_legal_proceeding": (
        "Método detalhado de procedimento legal dentro do método mais amplo",
        "Método detallado de procedimiento legal dentro del método más amplio",
    ),
    "poi_count": (
        "Número de pessoas de interesse legalmente processadas pela Polícia de Nova Gales do Sul",
        "Número de personas de interés legalmente procesadas por la Policía de Nueva Gales del Sur",
    ),
    "custody_system": (
        "Sistema prisional a que o registro se refere (adulto ou juvenil)",
        "Sistema penitenciario al que se refiere el registro (adulto o juvenil)",
    ),
    "legal_status": (
        "Situação legal da pessoa sob custódia (prisão preventiva ou condenada)",
        "Situación legal de la persona bajo custodia (prisión preventiva o condenada)",
    ),
    "aboriginality": (
        "Condição indígena da pessoa (aborígene, não aborígene ou desconhecida)",
        "Condición aborigen de la persona (aborigen, no aborigen o desconocida)",
    ),
    "sex": (
        "Sexo da pessoa (feminino ou masculino)",
        "Sexo de la persona (femenino o masculino)",
    ),
    "most_serious_offence": (
        "Crime mais grave associado à pessoa sob custódia",
        "Delito más grave asociado a la persona bajo custodia",
    ),
    "people": (
        "Número de pessoas sob custódia no último dia do mês",
        "Número de personas bajo custodia el último día del mes",
    ),
    "reception_status": (
        "Situação legal na entrada em custódia (prisão preventiva, condenada ou desconhecida)",
        "Situación legal al ingreso en custodia (prisión preventiva, condenada o desconocida)",
    ),
    "receptions": (
        "Número de entradas em custódia durante o mês",
        "Número de ingresos en custodia durante el mes",
    ),
    "discharge_type": (
        "Tipo de custódia da qual a pessoa foi liberada (prisão preventiva ou condenada)",
        "Tipo de custodia de la que la persona fue liberada (prisión preventiva o condenada)",
    ),
    "discharge_type:custody_remand_to_sentenced": (
        "Transição registrada, de prisão preventiva para custódia condenada",
        "Transición registrada, de prisión preventiva a custodia condenada",
    ),
    "discharge_type_breakdown": (
        "Destino ou motivo detalhado da liberação",
        "Destino o motivo detallado de la liberación",
    ),
    "discharges": (
        "Número de liberações de custódia durante o mês",
        "Número de liberaciones de custodia durante el mes",
    ),
    "transitions": (
        "Número de pessoas que passaram de prisão preventiva para custódia condenada durante o mês",
        "Número de personas que pasaron de prisión preventiva a custodia condenada durante el mes",
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
