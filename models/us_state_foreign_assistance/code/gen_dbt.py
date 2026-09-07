"""Generate the dbt models (.sql) and schema.yml for us_state_foreign_assistance.

Column names, order, types and Portuguese descriptions come from the
architecture CSVs under ``architecture/`` (written by
``architecture/gen_architecture.py``). Re-run after editing the spec.
"""

import csv
from pathlib import Path

HERE = Path(__file__).resolve().parent
MODEL_DIR = HERE.parent
DATASET = "us_state_foreign_assistance"

CAST = {
    "STRING": "string",
    "INT64": "int64",
    "FLOAT64": "float64",
    "DATE": "date",
}

PARTITION = {
    "transaction": (1946, 2031),
    "budget": (2004, 2029),
}

DESCRIPTIONS = {
    "transaction": (
        "Obrigações (empenhos) e desembolsos de assistência externa do governo "
        "dos Estados Unidos em nível de atividade, por país receptor, agência "
        "gestora e financiadora, conta orçamentária, parceiro implementador, "
        "setor OCDE/CAD e tipo de ajuda, do ano fiscal de 1946 até o presente, "
        "conforme publicado no ForeignAssistance.gov. Antes de 2001 os dados "
        "não contêm detalhe de atividade nem desembolsos. Valores em dólares "
        "correntes e constantes de 2025; valores negativos são desobrigações "
        "ou correções."
    ),
    "budget": (
        "Requisições orçamentárias do Presidente e dotações apropriadas e "
        "planejadas de assistência externa dos Estados Unidos por ano fiscal, "
        "país receptor, unidade operacional, conta orçamentária e setor, do "
        "ano fiscal de 2004 até o presente, conforme publicado no "
        "ForeignAssistance.gov."
    ),
    "dicionario": (
        "Dicionário com os rótulos de todas as colunas codificadas das tabelas "
        "transaction e budget (tipo de transação, país, região, grupo de "
        "renda, agências, contas, parceiros implementadores, setores e tipos "
        "de ajuda)."
    ),
}

# Dictionary-coverage checks: a handful of the coded columns per table.
DICT_COVERAGE = {
    "transaction": [
        "transaction_type_id",
        "country_id",
        "managing_agency_id",
        "funding_account_id",
        "international_purpose_code",
        "us_sector_id",
        "aid_type_id",
    ],
    "budget": [
        "transaction_type_id",
        "country_id",
        "funding_account_id",
        "international_purpose_code",
        "us_sector_id",
    ],
}

UNIQUE = {
    "transaction": [
        "fiscal_period",
        "transaction_date",
        "transaction_type_id",
        "country_id",
        "managing_subagency_id",
        "funding_agency_id",
        "funding_account_id",
        "implementing_partner_id",
        "international_purpose_code",
        "us_sector_id",
        "aid_type_id",
        "activity_id",
        "current_amount",
    ],
    "budget": [
        "year",
        "transaction_type_id",
        "country_id",
        "managing_subagency_id",
        "operating_unit",
        "funding_agency_id",
        "funding_account_id",
        "international_purpose_code",
        "us_sector_id",
        "oco_flag",
        "activity_id",
    ],
    "dicionario": ["id_tabela", "nome_coluna", "chave"],
}

NOT_NULL = {
    "transaction": [
        "year",
        "fiscal_period",
        "transaction_type_id",
        "country_id",
        "activity_id",
        "current_amount",
    ],
    "budget": ["year", "transaction_type_id", "country_id", "current_amount"],
    "dicionario": ["id_tabela", "nome_coluna", "chave"],
}

# Legitimately sparse columns, excluded from the not-null proportion test.
SPARSE = {
    "transaction": [],
    "budget": ["operating_unit", "oco_flag"],
    "dicionario": ["cobertura_temporal"],
}


def read_arch(table: str) -> list[dict]:
    path = HERE / "architecture" / f"{DATASET}__{table}.csv"
    with open(path, newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def gen_sql(table: str, arch: list[dict]) -> str:
    lines = []
    if table in PARTITION:
        start, end = PARTITION[table]
        cfg = (
            f'        schema="{DATASET}",\n'
            f'        alias="{table}",\n'
            '        materialized="table",\n'
            "        partition_by={\n"
            '            "field": "year",\n'
            '            "data_type": "int64",\n'
            f'            "range": {{"start": {start}, "end": {end}, "interval": 1}},\n'
            "        },\n"
        )
    else:
        cfg = (
            f'        schema="{DATASET}",\n'
            f'        alias="{table}",\n'
            '        materialized="table",\n'
        )
    lines.append("{{\n    config(\n" + cfg + "    )\n}}\n\n")
    lines.append("select\n")
    casts = [
        f"    safe_cast({a['name']} as {CAST[a['bigquery_type']]}) {a['name']}"
        for a in arch
    ]
    lines.append(",\n".join(casts) + "\n")
    lines.append(
        f'from {{{{ set_datalake_project("{DATASET}_staging.{table}") }}}} as t\n'
    )
    return "".join(lines)


def yaml_block(text: str, indent: int) -> str:
    """Render a description as a folded block scalar."""
    pad = " " * indent
    words = text.split()
    out, line = [], ""
    for w in words:
        if len(line) + len(w) + 1 > 78 - indent:
            out.append(line)
            line = w
        else:
            line = f"{line} {w}" if line else w
    if line:
        out.append(line)
    return ">\n" + "\n".join(pad + ln for ln in out)


def gen_schema(arches: dict[str, list[dict]]) -> str:
    y = ["---", "version: 2", "models:"]
    for table, arch in arches.items():
        model = f"{DATASET}__{table}"
        y.append(f"  - name: {model}")
        y.append(f"    description: {yaml_block(DESCRIPTIONS[table], 6)}")
        y.append("    tests:")
        y.append("      - dbt_utils.unique_combination_of_columns:")
        y.append(
            "          combination_of_columns: ["
            + ", ".join(UNIQUE[table])
            + "]"
        )
        y.append("      - not_null_proportion_multiple_columns:")
        y.append("          at_least: 0.05")
        if SPARSE[table]:
            y.append("          ignore_values:")
            for c in SPARSE[table]:
                y.append(f"            - {c}")
        if DICT_COVERAGE.get(table):
            y.append("      - custom_dictionary_coverage:")
            y.append("          columns_covered_by_dictionary:")
            for c in DICT_COVERAGE[table]:
                y.append(f"            - {c}")
            y.append(
                f"          dictionary_model: ref('{DATASET}__dicionario')"
            )
        y.append("    columns:")
        for a in arch:
            name = a["name"]
            y.append(f"      - name: {name}")
            y.append(
                f"        description: {yaml_block(a['description'], 10)}"
            )
            tests = []
            if name in NOT_NULL[table]:
                tests.append("not_null")
            if name == "year" and table in PARTITION:
                tests.append(
                    "relationships:\n"
                    "              to: ref('br_bd_diretorios_data_tempo__ano')\n"
                    "              field: ano.ano"
                )
            if name == "country_iso3_code":
                tests.append(
                    "custom_relationships:\n"
                    "              to: ref('br_bd_diretorios_mundo__pais')\n"
                    "              field: sigla_iso3\n"
                    "              config:\n"
                    "                where: country_iso3_code is not null"
                )
            if tests:
                y.append("        tests:")
                for t in tests:
                    y.append(f"          - {t}")
    return "\n".join(y) + "\n"


def main() -> None:
    arches = {t: read_arch(t) for t in ["transaction", "budget", "dicionario"]}
    for table, arch in arches.items():
        path = MODEL_DIR / f"{DATASET}__{table}.sql"
        path.write_text(gen_sql(table, arch), encoding="utf-8")
        print(f"wrote {path.name}")
    (MODEL_DIR / "schema.yml").write_text(gen_schema(arches), encoding="utf-8")
    print("wrote schema.yml")


if __name__ == "__main__":
    main()
