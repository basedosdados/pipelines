"""Parse the IBGE public layout into architecture CSVs and a dicionario.

Usage:
    uv run python models/br_ibge_censo_demografico/code/build_architecture.py
"""

from __future__ import annotations

import csv
import re
from pathlib import Path

import openpyxl

from models.br_ibge_censo_demografico.code import constants

ARCH_FIELDS = [
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


def parse_nome(nome: object) -> tuple[str, list[tuple[str, str]]]:
    """Split the layout NOME cell into a one-line description + value labels."""
    if not nome:
        return "", []
    text = str(nome).replace("\xa0", " ").strip()
    lines = [ln.strip() for ln in text.split("\n") if ln.strip()]
    desc = re.sub(r"\s+", " ", lines[0]).rstrip(".") if lines else ""
    labels: list[tuple[str, str]] = []
    for line in lines[1:]:
        match = re.match(r"^(\d+)\s*[-" + "\u2013" + r"]\s*(.+)$", line)
        if match:
            labels.append((match.group(1), match.group(2).strip().rstrip(".")))
    return desc, labels


def classify(
    original: str, desc: str, tipo: str, dec: object, labels: list
) -> tuple[str, str, str, str]:
    """Return (bq_type, covered_by_dictionary, unit, observations)."""
    tipo = (tipo or "").strip().upper()[:1]
    dec_n = int(dec or 0)
    desc_l = desc.lower()
    is_imputation = original.startswith(("MD", "MP", "MF", "MM"))
    is_id = original.endswith(("0100", "0101")) or original in {
        "D0100",
        "P0100",
        "P0101",
        "F0100",
        "F0101",
        "M0100",
        "M0101",
    }
    is_peso = "peso amostral" in desc_l
    is_geo_uf = original.endswith("0020")
    is_regiao = original.endswith("0010")

    if is_peso:
        return (
            "FLOAT64",
            "no",
            "",
            "Peso amostral adimensional do arquivo público",
        )
    if is_geo_uf:
        return (
            "STRING",
            "no",
            "",
            "Convertido do código IBGE de 2 dígitos para sigla_uf",
        )
    if is_regiao:
        return "STRING", "yes", "", ""
    if is_id:
        return "STRING", "no", "", "Identificador; aritmética sem sentido"
    if is_imputation:
        return "STRING", "yes", "", "Marca de imputação do IBGE"
    if tipo in {"C", "A"}:
        covered = "yes" if labels else "no"
        return "STRING", covered, "", ""
    if "rendimento" in desc_l or "valor do rendimento" in desc_l:
        return "FLOAT64" if dec_n else "INT64", "no", "BRL", ""
    if "minuto" in desc_l:
        return "INT64", "no", "minute", ""
    if "ano de fixação" in desc_l:
        return "INT64", "no", "year", ""
    if "tempo de moradia" in desc_l:
        return "INT64", "no", "year", ""
    if dec_n > 0:
        return "FLOAT64", "no", "person", ""
    if any(
        token in desc_l
        for token in (
            "número",
            "total de",
            "moradores",
            "cômodos",
            "banheiros",
            "filhos",
            "filhas",
            "integrantes",
            "homens",
            "mulheres",
            "crianças",
        )
    ):
        unit = (
            "room" if "cômodo" in desc_l or "banheiro" in desc_l else "person"
        )
        return "INT64", "no", unit, ""
    return "STRING", "yes" if labels else "no", "", ""


def directory_for(name: str) -> str:
    if name == "sigla_uf":
        return "br_bd_diretorios_brasil.uf:sigla"
    if name == "ano":
        return "br_bd_diretorios_data_tempo.ano:ano"
    return ""


def bd_name(sheet: str, original: str) -> str:
    return constants.RENAMES[sheet].get(original, original.lower())


def iter_layout(sheet: str) -> list[dict]:
    path = constants.DOCS_DIR / constants.LAYOUT_XLSX_NAME
    workbook = openpyxl.load_workbook(path, data_only=True)
    rows = []
    for raw in workbook[sheet].iter_rows(min_row=3, values_only=True):
        var, nome, _ini, _fim, _inte, dec, tipo = raw
        if not var:
            continue
        original = str(var).strip()
        desc, labels = parse_nome(nome)
        name = bd_name(sheet, original)
        bq_type, covered, unit, obs = classify(
            original, desc, str(tipo or ""), dec, labels
        )
        rows.append(
            {
                "name": name,
                "bigquery_type": bq_type,
                "description": desc[:500],
                "temporal_coverage": "",
                "covered_by_dictionary": covered,
                "directory_column": directory_for(name),
                "measurement_unit": unit,
                "has_sensitive_data": "no",
                "observations": obs,
                "original_name": original,
                "labels": labels,
            }
        )
    workbook.close()
    return rows


def write_architecture(sheet: str, rows: list[dict]) -> None:
    constants.ARCHITECTURE_DIR.mkdir(parents=True, exist_ok=True)
    slug = constants.TABLES[sheet]["slug"]
    path = constants.ARCHITECTURE_DIR / f"{slug}.csv"
    # Partition columns first (ano is hive-only; listed for dbt/metadata).
    ano_row = {
        "name": "ano",
        "bigquery_type": "INT64",
        "description": "Ano de referência do Censo Demográfico",
        "temporal_coverage": "",
        "covered_by_dictionary": "no",
        "directory_column": directory_for("ano"),
        "measurement_unit": "",
        "has_sensitive_data": "no",
        "observations": "Coluna de partição; só no path hive, não no parquet",
        "original_name": "",
    }
    ordered = [ano_row]
    by_name = {row["name"]: row for row in rows}
    for first in (
        "sigla_uf",
        "id_regiao",
        "controle",
        "numero_ordem",
        "peso_amostral",
    ):
        if first in by_name:
            ordered.append({k: by_name[first][k] for k in ARCH_FIELDS})
    seen = {row["name"] for row in ordered}
    for row in rows:
        if row["name"] in seen:
            continue
        ordered.append({k: row[k] for k in ARCH_FIELDS})
        seen.add(row["name"])
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=ARCH_FIELDS)
        writer.writeheader()
        writer.writerows(ordered)
    print(f"wrote {path} ({len(ordered)} cols)")


def write_dicionario(all_rows: dict[str, list[dict]]) -> Path:
    path = constants.ARCHITECTURE_DIR / "dicionario.csv"
    fieldnames = [
        "id_tabela",
        "nome_coluna",
        "chave",
        "cobertura_temporal",
        "valor",
    ]
    out = []
    for sheet, rows in all_rows.items():
        slug = constants.TABLES[sheet]["slug"]
        for row in rows:
            if row["covered_by_dictionary"] != "yes":
                continue
            for chave, valor in row["labels"]:
                out.append(
                    {
                        "id_tabela": slug,
                        "nome_coluna": row["name"],
                        "chave": chave,
                        "cobertura_temporal": "2022",
                        "valor": valor,
                    }
                )
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(out)
    print(f"wrote {path} ({len(out)} rows)")
    return path


def main() -> None:
    all_rows = {}
    for sheet in constants.TABLES:
        rows = iter_layout(sheet)
        write_architecture(sheet, rows)
        all_rows[sheet] = rows
    write_dicionario(all_rows)


if __name__ == "__main__":
    main()
