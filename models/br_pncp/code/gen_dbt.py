"""Generate the br_pncp dbt models and schema.yml from the architecture CSVs.

The architecture is the source of truth for column order, types and
descriptions, so the SQL and the YAML are derived from it rather than
hand-maintained alongside it.

Run the repo's formatters afterwards, or the next commit will show a diff
that looks like a regeneration bug and is not one -- sqlfmt and yamlfix
reshape this output, and pre-commit.ci will do it for you on the PR if you
do not do it here:

    uv run python models/br_pncp/code/gen_dbt.py
    uv run pre-commit run sqlfmt  --files models/br_pncp/*.sql
    uv run pre-commit run yamlfix --files models/br_pncp/schema.yml

That sequence is idempotent: it reproduces the committed files exactly.
"""

from __future__ import annotations

import sys
from pathlib import Path

# The pure transform lives in the pipeline package, which is the canonical
# home; this script is the one-shot onboarding front end for it.
sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

from pipelines.datasets.br_pncp.utils import DEDUP_KEYS, read_architecture

DATASET = "br_pncp"
MODELS_DIR = Path(__file__).resolve().parents[1]

# plano_contratacao_anual is deliberately absent: it is deferred to a
# follow-up backfill (see constants.DEFERRED_TABLES), and shipping a dbt model
# whose staging table does not exist would abort table-approve for the whole
# PR. Re-add it here and re-run this script when its data lands.
TABLES = [
    "contratacao",
    "contrato",
    "ata_registro_preco",
    "instrumento_cobranca",
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

# Values that are real and meaningful but deliberately absent from the BD
# directories, so their relationship test must exempt them rather than the
# data being nulled to make the test green.
#
# 'EX' (and its paired municipality code 9097071) is the Brazilian
# government's code for *Exterior* -- a procuring unit located abroad, such
# as an embassy or consulate. 7 rows of contratacao's 4,003,718 carry it, all
# in 2026. Dropping that would silently discard the fact that those
# procurements happened outside Brazil.
DIRECTORY_EXEMPTIONS = {
    ("contratacao", "sigla_uf"): "sigla_uf != 'EX'",
    ("contratacao", "id_municipio"): "id_municipio != '9097071'",
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
    key_cols, recency_col = DEDUP_KEYS.get(table, (None, None))

    selects = ",\n".join(
        f"    safe_cast({c['name']} as {c['bigquery_type'].lower()}) {c['name']}"
        for c in cols
    )

    if table not in PARTITIONED:
        # dicionario is small and derived; a full rebuild each run is cheaper
        # than the machinery to make it incremental.
        return (
            "{{\n"
            "    config(\n"
            f'        schema="{DATASET}",\n'
            f'        alias="{table}",\n'
            '        materialized="table",\n'
            "    )\n"
            "}}\n\n\n"
            "select\n"
            f"{selects}\n"
            "from\n"
            f'    {{{{ set_datalake_project("{DATASET}_staging.{table}") }}}}\n'
            "    as t\n"
        )

    start, end = PARTITIONED[table]
    if key_cols is None or recency_col is None:
        # Every partitioned model dedups; a missing entry is a bug here,
        # not a table that legitimately keeps duplicates.
        raise KeyError(f"{table} is partitioned but has no DEDUP_KEYS entry")
    key = ", ".join(key_cols)

    # Staging is append-only: each pipeline run adds the records it harvested,
    # so the same PNCP control number appears once per run that touched it. The
    # QUALIFY collapses those to the most recently updated version.
    #
    # The incremental filter scopes the run to the partitions the flow reports
    # having touched, via the `pncp_years` var. It deliberately filters on the
    # PARTITION rather than on data_atualizacao: insert_overwrite replaces each
    # partition wholesale, so the SELECT must yield every row belonging to that
    # year, not only the rows this run happened to harvest. Filtering on
    # recency instead would overwrite the partition with just the delta and
    # silently drop the rest of the year.
    return (
        "{{\n"
        "    config(\n"
        f'        schema="{DATASET}",\n'
        f'        alias="{table}",\n'
        '        materialized="incremental",\n'
        '        incremental_strategy="insert_overwrite",\n'
        "        partition_by={\n"
        '            "field": "ano",\n'
        '            "data_type": "int64",\n'
        f'            "range": {{"start": {start}, "end": {end}, "interval": 1}},\n'
        "        },\n"
        "    )\n"
        "}}\n\n\n"
        "select\n"
        f"{selects}\n"
        "from\n"
        f'    {{{{ set_datalake_project("{DATASET}_staging.{table}") }}}}\n'
        "    as t\n"
        # Falling back to no filter when the var is absent keeps a manual
        # `dbt run` correct (a full rebuild) instead of emitting `in ()`.
        "{% if is_incremental() and var('pncp_years', '') %}\n"
        "    where\n"
        "        safe_cast(ano as int64) in (\n"
        "            {{ var('pncp_years') }}\n"
        "        )\n"
        "{% endif %}\n"
        "qualify\n"
        "    row_number() over (\n"
        f"        partition by {key}\n"
        f"        order by safe_cast({recency_col} as date) desc\n"
        "    )\n"
        "    = 1\n"
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
                # Measured at >=95% NULL in the most recent year: PNCP only
                # populates these for the small minority of contracts funded by
                # a congressional amendment or arising from an ata adhesion.
                "indicador_emenda_parlamentar",
                "indicador_fruto_adesao",
            }
        ]
        if sparse:
            out.append("          ignore_values:")
            out.extend(f"            - {name}" for name in sorted(set(sparse)))

        # Every value in a dictionary-covered column must have a `chave` in
        # the dicionario. Without this the dicionario can ship incomplete and
        # nothing notices: it is DERIVED from the fact tables, so building it
        # before a table is harvested silently yields zero keys for that
        # table's codes, and the other tests all still pass.
        covered = [
            c["name"]
            for c in cols
            if (c.get("covered_by_dictionary") or "").strip().lower() == "yes"
        ]
        if covered:
            out.append("      - custom_dictionary_coverage:")
            out.append(
                f"          dictionary_model: ref('{DATASET}__dicionario')"
            )
            out.append(
                "          columns_covered_by_dictionary: ["
                + ", ".join(covered)
                + "]"
            )
            if scoped:
                out.append("          config:")
                out.append("            where: __most_recent_year__")

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
                    exempt = DIRECTORY_EXEMPTIONS.get((table, c["name"]))
                    clause = "__most_recent_year__"
                    if exempt:
                        # get_where_subquery string-replaces the placeholder
                        # and wraps the whole clause, so an extra condition
                        # composes cleanly.
                        clause += f" and {exempt}"
                    out.append("              config:")
                    out.append(f"                where: {clause}")
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
    print(
        "\nNow run the formatters, or these files will differ from what is "
        "committed:\n"
        "  uv run pre-commit run sqlfmt  --files models/br_pncp/*.sql\n"
        "  uv run pre-commit run yamlfix --files models/br_pncp/schema.yml"
    )


if __name__ == "__main__":
    main()
