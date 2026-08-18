"""Generate the us_sec_edgar dbt models and schema.yml from the architecture CSVs.

The architecture is the source of truth for column names, order, types and
descriptions, so the models and their schema are generated rather than
hand-kept in sync. Re-run after editing any `code/architecture/*.csv`:

    uv run python models/us_sec_edgar/code/gen_dbt.py

Run pre-commit afterwards: yamlfix normalizes the generated schema.yml (short
lists collapse to flow style), so the committed file differs cosmetically from
this script's raw output.
"""

import os

from pipelines.datasets.us_sec_edgar.utils import (
    DICTIONARY_COLUMNS,
    architecture_columns,
)

HERE = os.path.dirname(os.path.abspath(__file__))
MODEL_DIR = os.path.abspath(os.path.join(HERE, ".."))
DATASET = "us_sec_edgar"
TABLES = ["submission", "numeric_fact", "tag", "presentation", "dicionario"]

PARTITION = {"start": 2009, "end": 2031}

# Highest-cardinality join key per table, so that the common filters
# (one filing, one tag) prune blocks within a year partition.
CLUSTER = {
    "submission": ["cik"],
    "numeric_fact": ["accession_number", "tag"],
    "tag": ["tag"],
    "presentation": ["accession_number"],
}

DESCRIPTIONS = {
    "submission": (
        "Uma linha por submissão XBRL protocolada na SEC cujos valores foram "
        "renderizados nas demonstrações financeiras principais, com os dados "
        "cadastrais do registrante (CIK, nome, setor SIC, endereços, país e "
        "estado de constituição) e os atributos do protocolo (formulário, "
        "período, ano e período fiscal, datas de protocolo e aceitação). Chave: "
        "número de adesão dentro do trimestre de divulgação."
    ),
    "numeric_fact": (
        "Uma linha por valor numérico das demonstrações financeiras principais "
        "renderizadas pela SEC, conforme protocolado. Cada linha identifica a "
        "submissão, o marcador (tag) da taxonomia XBRL e sua versão, o período, "
        "a unidade de medida, o eixo dimensional (segments) e o corregistrante."
    ),
    "tag": (
        "Uma linha por marcador (tag) da taxonomia XBRL usado nas submissões do "
        "trimestre, padrão ou customizado pelo declarante, com tipo de dado, "
        "natureza temporal, saldo contábil natural, rótulo e documentação. Os "
        "marcadores se repetem a cada trimestre de divulgação em que são usados; "
        "para obter a dimensão única, aplique select distinct sobre tag e version."
    ),
    "presentation": (
        "Uma linha por linha das demonstrações financeiras principais tal como "
        "apresentadas pelo declarante, indicando a demonstração, o relatório, a "
        "ordem da linha, o marcador atribuído e o rótulo exibido. Um mesmo "
        "marcador pode aparecer em mais de uma demonstração da mesma submissão."
    ),
    "dicionario": (
        "Dicionário de valores codificados das demais tabelas do conjunto: uma "
        "linha por combinação de tabela, coluna, chave e rótulo legível."
    ),
}

UNIQUE_KEYS = {
    "submission": ["year", "quarter", "accession_number"],
    "numeric_fact": [
        "year",
        "quarter",
        "accession_number",
        "tag",
        "version",
        "period_end_date",
        "quantity_quarters",
        "unit_of_measure",
        "segments",
        "coregistrant",
    ],
    "tag": ["year", "quarter", "tag", "version"],
    "presentation": ["year", "quarter", "accession_number", "report", "line"],
    "dicionario": ["id_tabela", "nome_coluna", "chave"],
}

# Columns legitimately empty for the large majority of rows. Measured across the
# full 2009Q1-2026Q1 export: additional_ciks 1.8% non-null, footnote 0.2%,
# coregistrant 4.6%.
SPARSE_COLUMNS = {
    "submission": ["additional_ciks"],
    "numeric_fact": ["footnote", "coregistrant"],
    "tag": [],
    "presentation": [],
}

NOT_NULL = {
    "submission": ["year", "quarter", "accession_number", "cik"],
    "numeric_fact": ["year", "quarter", "accession_number", "tag", "version"],
    "tag": ["year", "quarter", "tag", "version"],
    "presentation": ["year", "quarter", "accession_number", "report", "line"],
    "dicionario": ["id_tabela", "nome_coluna", "chave", "valor"],
}

# accession_number is a foreign key onto `submission`, verified orphan-free.
FOREIGN_KEYS = {
    "numeric_fact": {"accession_number": ("submission", "accession_number")},
    "presentation": {"accession_number": ("submission", "accession_number")},
}

NEWLINE = chr(10)


def render_model(table: str) -> str:
    columns = architecture_columns(table)
    config = [
        '        schema="' + DATASET + '",',
        '        alias="' + table + '",',
        '        materialized="table",',
    ]
    if table != "dicionario":
        config.append("        partition_by={")
        config.append('            "field": "year",')
        config.append('            "data_type": "int64",')
        config.append(
            '            "range": {"start": '
            + str(PARTITION["start"])
            + ', "end": '
            + str(PARTITION["end"])
            + ', "interval": 1},'
        )
        config.append("        },")
        config.append(
            "        cluster_by=["
            + ", ".join('"' + c + '"' for c in CLUSTER[table])
            + "],"
        )
    casts = [
        "    safe_cast("
        + c["name"]
        + " as "
        + c["bigquery_type"].lower()
        + ") "
        + c["name"]
        for c in columns
    ]
    return (
        "{{"
        + NEWLINE
        + "    config("
        + NEWLINE
        + NEWLINE.join(config)
        + NEWLINE
        + "    )"
        + NEWLINE
        + "}}"
        + NEWLINE * 3
        + "select"
        + NEWLINE
        + ("," + NEWLINE).join(casts)
        + NEWLINE
        + 'from {{ set_datalake_project("'
        + DATASET
        + "_staging."
        + table
        + '") }} as t'
        + NEWLINE
    )


def block(text: str, indent: str) -> str:
    """Wrap a description as a YAML `>` block scalar."""
    lines, current = [], ""
    for word in text.split():
        if len(current) + len(word) + 1 > 74:
            lines.append(current)
            current = word
        else:
            current = (current + " " + word).strip()
    lines.append(current)
    return ">" + NEWLINE + NEWLINE.join(indent + line for line in lines)


def render_schema() -> str:
    out = ["---", "version: 2", "models:"]
    for table in TABLES:
        out.append("  - name: " + DATASET + "__" + table)
        out.append("    description: " + block(DESCRIPTIONS[table], "      "))
        out.append("    tests:")
        out.append("      - dbt_utils.unique_combination_of_columns:")
        out.append("          combination_of_columns:")
        for key in UNIQUE_KEYS[table]:
            out.append("            - " + key)
        if table != "dicionario":
            out.append("      - not_null_proportion_multiple_columns:")
            out.append("          at_least: 0.05")
            if SPARSE_COLUMNS[table]:
                out.append(
                    "          ignore_values: ["
                    + ", ".join(SPARSE_COLUMNS[table])
                    + "]"
                )
        if DICTIONARY_COLUMNS.get(table):
            out.append("      - custom_dictionary_coverage:")
            out.append("          columns_covered_by_dictionary:")
            for column in DICTIONARY_COLUMNS[table]:
                out.append("            - " + column)
            out.append(
                "          dictionary_model: ref('"
                + DATASET
                + "__dicionario')"
            )
        out.append("    columns:")
        for column in architecture_columns(table):
            name = column["name"]
            out.append("      - name: " + name)
            out.append(
                "        description: "
                + block(column["description"], "          ")
            )
            not_null = name in NOT_NULL[table]
            foreign = FOREIGN_KEYS.get(table, {}).get(name)
            if not_null and not foreign:
                out.append("        tests: [not_null]")
            elif not_null or foreign:
                out.append("        tests:")
                if not_null:
                    out.append("          - not_null")
                if foreign:
                    target, field = foreign
                    out.append("          - relationships:")
                    out.append(
                        "              to: ref('"
                        + DATASET
                        + "__"
                        + target
                        + "')"
                    )
                    out.append("              field: " + field)
    return NEWLINE.join(out) + NEWLINE


def main() -> None:
    for table in TABLES:
        path = os.path.join(MODEL_DIR, DATASET + "__" + table + ".sql")
        with open(path, "w", encoding="utf-8") as fh:
            fh.write(render_model(table))
        print("wrote " + os.path.relpath(path))
    schema_path = os.path.join(MODEL_DIR, "schema.yml")
    with open(schema_path, "w", encoding="utf-8") as fh:
        fh.write(render_schema())
    print("wrote " + os.path.relpath(schema_path))


if __name__ == "__main__":
    main()
