"""Generate the br_pncp dbt models and schema.yml from the architecture CSVs.

The architecture is the source of truth for column order, types and
descriptions, so the SQL and the YAML are derived from it rather than
hand-maintained alongside it.
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from utils import read_architecture

DATASET = "br_pncp"
MODELS_DIR = Path(__file__).resolve().parents[1]

TABLES = [
    "contratacao",
    "contrato",
    "ata_registro_preco",
    "instrumento_cobranca",
    "plano_contratacao_anual",
    "dicionario",
]

PARTITIONED = {
    "contratacao": (2021, 2031),
    "contrato": (2021, 2031),
    "ata_registro_preco": (2021, 2031),
    "instrumento_cobranca": (2023, 2031),
    "plano_contratacao_anual": (2023, 2031),
}

# Uniqueness key per model, used by dbt_utils.unique_combination_of_columns.
UNIQUE_KEYS = {
    "contratacao": ["ano", "id_contratacao_pncp"],
    "contrato": ["ano", "id_contrato_pncp"],
    "ata_registro_preco": ["ano", "id_ata_pncp"],
    "instrumento_cobranca": [
        "ano",
        "cnpj_orgao",
        "ano_contrato",
        "sequencial_contrato",
        "sequencial_instrumento_cobranca",
    ],
    "plano_contratacao_anual": ["ano", "id_pca_pncp", "numero_item"],
    "dicionario": ["id_tabela", "nome_coluna", "chave"],
}

NOT_NULL = {
    "contratacao": ["ano", "id_contratacao_pncp"],
    "contrato": ["ano", "id_contrato_pncp"],
    "ata_registro_preco": ["ano", "id_ata_pncp"],
    "instrumento_cobranca": ["ano", "cnpj_orgao"],
    "plano_contratacao_anual": ["ano", "id_pca_pncp"],
    "dicionario": ["id_tabela", "nome_coluna", "chave", "valor"],
}

DESCRIPTIONS = {
    "contratacao": (
        "Contratações públicas (licitações, dispensas e inexigibilidades) divulgadas no "
        "Portal Nacional de Contratações Públicas, abrangendo os três níveis de governo. "
        "Uma linha por contratação, identificada pelo número de controle no PNCP."
    ),
    "contrato": (
        "Contratos e empenhos decorrentes de contratações públicas divulgados no Portal "
        "Nacional de Contratações Públicas, abrangendo os três níveis de governo. Uma linha "
        "por contrato ou empenho; a coluna id_tipo_contrato distingue os dois."
    ),
    "ata_registro_preco": (
        "Atas de registro de preços divulgadas no Portal Nacional de Contratações Públicas. "
        "Uma linha por ata, identificada pelo número de controle no PNCP."
    ),
    "instrumento_cobranca": (
        "Instrumentos de cobrança (notas fiscais e faturas) vinculados a contratos "
        "divulgados no Portal Nacional de Contratações Públicas. Uma linha por instrumento "
        "de cobrança dentro de um contrato."
    ),
    "plano_contratacao_anual": (
        "Itens dos planos de contratações anuais publicados pelos órgãos no Portal Nacional "
        "de Contratações Públicas. Uma linha por item de plano, com a classificação do item "
        "no catálogo de materiais e serviços."
    ),
    "dicionario": (
        "Dicionário de valores codificados das demais tabelas do conjunto, com o significado "
        "de cada chave por tabela e coluna."
    ),
}


def sql_for(table: str) -> str:
    cols = read_architecture(table)
    if table in PARTITIONED:
        start, end = PARTITIONED[table]
        partition = (
            "        partition_by={\n"
            '            "field": "ano",\n'
            '            "data_type": "int64",\n'
            f'            "range": {{"start": {start}, "end": {end}, "interval": 1}},\n'
            "        },\n"
        )
    else:
        partition = ""

    selects = ",\n".join(
        f"    safe_cast({c['name']} as {c['bigquery_type'].lower()}) {c['name']}"
        for c in cols
    )
    return (
        "{{\n"
        "    config(\n"
        f'        schema="{DATASET}",\n'
        f'        alias="{table}",\n'
        '        materialized="table",\n'
        f"{partition}"
        "    )\n"
        "}}\n\n\n"
        "select\n"
        f"{selects}\n"
        "from\n"
        f'    {{{{ set_datalake_project("{DATASET}_staging.{table}") }}}}\n'
        "    as t\n"
    )


def yaml_block(indent: str, text: str, width: int = 74) -> str:
    """Emit a folded block scalar, safe for descriptions containing colons."""
    words, lines, cur = text.split(), [], ""
    for word in words:
        if cur and len(cur) + 1 + len(word) > width:
            lines.append(cur)
            cur = word
        else:
            cur = f"{cur} {word}".strip()
    if cur:
        lines.append(cur)
    return "\n".join(f"{indent}{line}" for line in lines)


def schema_yaml() -> str:
    out = ["---", "version: 2", "models:"]
    for table in TABLES:
        cols = read_architecture(table)
        model = f"{DATASET}__{table}"
        out.append(f"  - name: {model}")
        out.append("    description: >")
        out.append(yaml_block("      ", DESCRIPTIONS[table]))
        # Scope the table-level tests to the newest partition. Unscoped,
        # not_null_proportion_multiple_columns compiles a scan of *every* column
        # across every partition, which on these 40+ column, multi-million-row
        # tables is enough to burn the BigQuery daily byte quota on its own.
        scoped = table in PARTITIONED

        out.append("    tests:")
        out.append("      - dbt_utils.unique_combination_of_columns:")
        out.append(
            "          combination_of_columns: ["
            + ", ".join(UNIQUE_KEYS[table])
            + "]"
        )
        if scoped:
            out.append("          config:")
            out.append("            where: __most_recent_year__")
        out.append("      - not_null_proportion_multiple_columns:")
        out.append("          at_least: 0.05")
        if scoped:
            out.append("          config:")
            out.append("            where: __most_recent_year__")

        # Columns that are legitimately sparse across the whole table: optional
        # free text, subrogation (rare), and fields only populated late in the
        # procurement lifecycle.
        sparse = [
            c["name"]
            for c in cols
            if c["name"].endswith(
                ("_subrogado", "_subrogada", "_subcontratado")
            )
            or c["name"]
            in {
                "justificativa_presencial",
                "informacao_complementar",
                "valor_total_homologado",
                "valor_acumulado",
                "data_cancelamento",
                "observacao",
                "link_sistema_origem",
                "data_abertura_proposta",
                "data_encerramento_proposta",
                "id_ata_pncp",
                "codigo_pais_fornecedor",
                "data_desejada",
                "valor_orcamento_exercicio",
            }
        ]
        if sparse:
            out.append("          ignore_values:")
            out.extend(f"            - {name}" for name in sorted(set(sparse)))

        out.append("    columns:")
        for c in cols:
            out.append(f"      - name: {c['name']}")
            out.append("        description: >")
            out.append(yaml_block("          ", c["description"]))
            tests = []
            if c["name"] in NOT_NULL[table]:
                tests.append("not_null")
            if c["directory_column"]:
                dataset_table, field = c["directory_column"].split(":")
                ref = dataset_table.split(".")[1]
                # The time directory binds through a STRUCT; addressing the leaf
                # as `<col>.<col>` is required or the test never passes.
                target_field = (
                    f"{field}.{field}"
                    if "data_tempo" in dataset_table
                    else field
                )
                out.append("        tests:")
                if tests:
                    out.append(f"          - {tests[0]}")
                out.append("          - relationships:")
                out.append(
                    f"              to: ref('br_bd_diretorios_{'brasil' if 'brasil' in dataset_table else 'data_tempo'}__{ref}')"
                )
                out.append(f"              field: {target_field}")
                if table in PARTITIONED:
                    out.append("              config:")
                    out.append("                where: __most_recent_year__")
                continue
            if tests:
                out.append(f"        tests: [{', '.join(tests)}]")
    return "\n".join(out) + "\n"


def main() -> None:
    for table in TABLES:
        path = MODELS_DIR / f"{DATASET}__{table}.sql"
        path.write_text(sql_for(table), encoding="utf-8")
        print(f"wrote {path.name}")
    schema = MODELS_DIR / "schema.yml"
    schema.write_text(schema_yaml(), encoding="utf-8")
    print(f"wrote {schema.name}")


if __name__ == "__main__":
    main()
