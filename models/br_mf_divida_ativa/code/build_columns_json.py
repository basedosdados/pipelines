#!/usr/bin/env python3
"""Emit columns_json payloads for mcp__databasis__bulk_upsert_columns, one per
table. Portuguese description comes from the architecture CSV (this is a
Brazilian dataset); English and Spanish come from TRANSLATIONS below, so columns
register without a Google Sheet. Writes code/columns_json/<table>.json.

Usage:
    python models/br_mf_divida_ativa/code/build_columns_json.py
"""

import csv
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
ARCH = ROOT / "code" / "architecture"
OUT = ROOT / "code" / "columns_json"

# name -> (description_en, description_es). Portuguese comes from the arch CSV.
TRANSLATIONS = {
    "ano": (
        "Reference year of the data extraction quarter",
        "Año de referencia del trimestre de extracción de los datos",
    ),
    "trimestre": (
        "Reference quarter of the data extraction, from 1 to 4",
        "Trimestre de referencia de la extracción de los datos, de 1 a 4",
    ),
    "sigla_uf": (
        "Abbreviation of the debtor's federative unit (state)",
        "Sigla de la unidad federativa (estado) del deudor",
    ),
    "cpf_cnpj": (
        "Debtor's CPF (partially masked) or CNPJ",
        "CPF (parcialmente enmascarado) o CNPJ del deudor",
    ),
    "tipo_pessoa": (
        "Type of person of the debtor, natural or legal",
        "Tipo de persona del deudor, física o jurídica",
    ),
    "tipo_devedor": (
        "Type of debtor, such as principal, co-responsible or jointly liable",
        "Tipo de deudor, como principal, corresponsable o solidario",
    ),
    "nome_devedor": (
        "Name of the debtor",
        "Nombre del deudor",
    ),
    "unidade_responsavel": (
        "PGFN unit responsible for collecting the registration",
        "Unidad de la PGFN responsable del cobro de la inscripción",
    ),
    "entidade_responsavel": (
        "Entity responsible for the registration",
        "Entidad responsable de la inscripción",
    ),
    "unidade_inscricao": (
        "Unit where the registration was made",
        "Unidad en la que se realizó la inscripción",
    ),
    "numero_inscricao": (
        "Active debt registration number",
        "Número de inscripción en deuda activa",
    ),
    "tipo_situacao_inscricao": (
        "Type of registration status, such as under collection, tax benefit or guarantee",
        "Tipo de situación de la inscripción, como en cobro, beneficio fiscal o garantía",
    ),
    "situacao_inscricao": (
        "Specific status of the active debt registration",
        "Situación específica de la inscripción en deuda activa",
    ),
    "receita_principal": (
        "Main revenue or nature of the credit of the registration",
        "Ingreso o naturaleza del crédito principal de la inscripción",
    ),
    "data_inscricao": (
        "Date of registration in active debt",
        "Fecha de inscripción en deuda activa",
    ),
    "indicador_ajuizado": (
        "Indicates whether the registration is under litigation (SIM) or not (NAO)",
        "Indica si la inscripción está judicializada (SIM) o no (NAO)",
    ),
    "valor_consolidado": (
        "Consolidated amount of the registration in reais",
        "Monto consolidado de la inscripción en reales",
    ),
}


def main() -> None:
    """Write one columns_json file per architecture CSV.

    Reads each ``architecture/<table>.csv`` for the Portuguese description,
    type, and flags, attaches the English/Spanish translations from
    ``TRANSLATIONS``, and writes ``columns_json/<table>.json`` for
    ``bulk_upsert_columns``.
    """
    OUT.mkdir(parents=True, exist_ok=True)
    for csv_path in sorted(ARCH.glob("*.csv")):
        table = csv_path.stem
        with open(csv_path, newline="", encoding="utf-8") as fh:
            rows = list(csv.DictReader(fh))
        cols = []
        for r in rows:
            en, es = TRANSLATIONS[r["name"]]
            col = {
                "name": r["name"],
                "bigquery_type": r["bigquery_type"],
                "description_pt": r["description"],
                "description_en": en,
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
