"""Generate the us_census_trade dbt models and schema.yml from the architecture.

Column order, types and Portuguese descriptions all come from the architecture
CSVs, so the models cannot drift from the registered schema.

Run: ``python models/us_census_trade/code/build_dbt.py``, then re-run the
formatters, which own the final layout::

    uv run pre-commit run --files models/us_census_trade/*.sql \
        models/us_census_trade/schema.yml

sqlfmt and yamlfix reflow what this script emits, so the committed files are
the formatted ones. Do not hand-edit the generated .sql or schema.yml -- change
the architecture CSVs or this script and regenerate.
"""

from __future__ import annotations

import csv
from pathlib import Path

HERE = Path(__file__).resolve().parent
ARCH = HERE / "architecture"
OUT = HERE.parent
DATASET = "us_census_trade"

PARTITION_START = 2010
PARTITION_END = 2035

GRAIN = {
    "import": ["year", "month", "country_code", "district_code", "hs6_code"],
    "export": [
        "year",
        "month",
        "country_code",
        "district_code",
        "hs6_code",
        "domestic_foreign_code",
    ],
    "import_port": ["year", "month", "country_code", "port_code", "hs6_code"],
    "export_port": ["year", "month", "country_code", "port_code", "hs6_code"],
    "import_state": [
        "year",
        "month",
        "country_code",
        "state_abbreviation",
        "hs6_code",
    ],
    "export_state": [
        "year",
        "month",
        "country_code",
        "state_abbreviation",
        "hs6_code",
    ],
    "dicionario": ["id_tabela", "nome_coluna", "chave"],
}

# Columns that are sparse by the source's own structure, so the 5% non-null
# floor does not apply to them.
#
# DECLARED FROM THE SOURCE DOCUMENTATION, NOT MEASURED -- the API needs a key
# that is not available locally, so no row has been counted yet. Re-measure
# against the parquet after the first dev run and correct this list; a column
# sitting just above 5% today will cross the floor later.
SPARSE = {
    "general_quantity_2",  # second quantity: only commodities with two units
    "consumption_quantity_2",
    "quantity_2",
    "quantity_2_unit",
    "state_id",  # null for STATE codes that are not a real state
}

# Plain `relationships` where the join is exact.
RELATIONSHIPS = {
    "state_id": ("br_bd_diretorios_us__state", "id_state"),
}

# `custom_relationships` where a measured, explained set of values cannot join.
#
# country_iso2_code: 237 of Schedule C's 241 ISO2 codes are present in
# br_bd_diretorios_mundo.pais. The four that are not were checked one by one
# against the directory:
#
#   GZ  Gaza Strip   -- not a country in ISO 3166-1
#   WE  West Bank    -- not a country in ISO 3166-1
#   KV  Kosovo       -- Census's own code; ISO 3166-1 assigns Kosovo no
#                       official alpha-2 (XK is user-assigned)
#   NA  Namibia      -- IS a valid ISO 3166-1 code, and IS missing from the
#                       directory: pais.sigla_iso2 is NULL for Namibia while
#                       sigla_iso3 is 'NAM'. The literal "NA" was read as a
#                       null sentinel when the directory was built. That is a
#                       defect in br_bd_diretorios_mundo, not in this dataset,
#                       and it is excluded here rather than worked around,
#                       because fixing a shared directory does not belong in a
#                       dataset PR. Remove NA from this list once the directory
#                       is corrected.
CUSTOM_RELATIONSHIPS = {
    "country_iso2_code": (
        "br_bd_diretorios_mundo__pais",
        "sigla_iso2",
        ["GZ", "KV", "NA", "WE"],
    ),
}

DESCRIPTIONS = {
    "import": """
        Importações mensais de mercadorias dos Estados Unidos por código de
        seis dígitos do Sistema Harmonizado, país parceiro e distrito
        aduaneiro de entrada, publicadas pelo U.S. Census Bureau. Cada linha
        traz o valor das importações gerais e o valor das importações para
        consumo, que são medidas distintas e sobrepostas e não devem ser
        somadas, além de valor CIF, despesas, imposto calculado, duas
        quantidades com suas unidades e a repartição por modal aéreo e
        marítimo. É a única das seis tabelas de fatos que traz quantidade e
        imposto. Complementa world_cepii_baci em vez de repeti-la: o BACI é o
        painel mundial anual reconciliado, enquanto esta tabela é a fonte
        nacional norte-americana em frequência mensal e com detalhe de
        distrito aduaneiro
    """,
    "export": """
        Exportações mensais de mercadorias dos Estados Unidos por código de
        seis dígitos do Sistema Harmonizado, país parceiro e distrito
        aduaneiro de saída, publicadas pelo U.S. Census Bureau. O valor é FAS,
        medido no porto de saída e sem frete e seguro internacionais. A coluna
        domestic_foreign_code separa exportações de mercadorias produzidas nos
        Estados Unidos das reexportações de mercadorias estrangeiras e é uma
        dimensão da linha, de modo que o total exportado é a soma das duas
        categorias. Complementa world_cepii_baci: painel mundial anual
        reconciliado lá, fonte nacional mensal com detalhe de distrito aqui
    """,
    "import_port": """
        Importações mensais de mercadorias dos Estados Unidos por código de
        seis dígitos do Sistema Harmonizado, país parceiro e porto de entrada,
        publicadas pelo U.S. Census Bureau. O endpoint por porto publica
        apenas as importações gerais e a repartição por modal: valor para
        consumo, imposto e quantidade existem somente na tabela import, por
        distrito aduaneiro. Os dois primeiros dígitos de port_code são o
        distrito aduaneiro, o que permite reconciliar as duas tabelas
    """,
    "export_port": """
        Exportações mensais de mercadorias dos Estados Unidos por código de
        seis dígitos do Sistema Harmonizado, país parceiro e porto de saída,
        publicadas pelo U.S. Census Bureau. O endpoint por porto não publica
        quantidade nem a separação entre exportação doméstica e reexportação,
        que existem somente na tabela export, por distrito aduaneiro. Os dois
        primeiros dígitos de port_code são o distrito aduaneiro
    """,
    "import_state": """
        Importações mensais de mercadorias dos Estados Unidos por código de
        seis dígitos do Sistema Harmonizado, país parceiro e estado de destino
        declarado, publicadas pelo U.S. Census Bureau. O endpoint por estado
        não publica quantidade nem imposto. O estado de destino é o declarado
        na entrada e não necessariamente onde a mercadoria é consumida
    """,
    "export_state": """
        Exportações mensais de mercadorias dos Estados Unidos por código de
        seis dígitos do Sistema Harmonizado, país parceiro e estado de origem
        do movimento, publicadas pelo U.S. Census Bureau. O estado de origem do
        movimento é a localização do exportador e não necessariamente onde a
        mercadoria foi produzida, ressalva que o próprio Census Bureau faz. O
        endpoint por estado não publica quantidade
    """,
    "dicionario": """
        Dicionário com as traduções dos códigos usados nas seis tabelas de
        fatos de us_census_trade. Construído a partir das listas de códigos
        publicadas pelo Census Bureau — a Schedule C para países e a Schedule D
        para distritos aduaneiros e portos — e não a partir dos valores
        observados numa janela de meses, de modo que a cobertura é completa e
        não se limita ao que foi negociado nos meses baixados por último
    """,
}


def read_arch(table: str) -> list[dict]:
    with (ARCH / f"{table}.csv").open(encoding="utf-8") as fh:
        return list(csv.DictReader(fh))


def wrap(text: str, indent: str, width: int = 78) -> list[str]:
    words = " ".join(text.split()).split(" ")
    lines, cur = [], indent
    for w in words:
        if len(cur) + len(w) + 1 > width and cur.strip():
            lines.append(cur.rstrip())
            cur = indent
        cur += w + " "
    if cur.strip():
        lines.append(cur.rstrip())
    return lines


def sql_model(table: str) -> str:
    arch = read_arch(table)
    casts = ",\n".join(
        f"    safe_cast({a['name']} as {a['bigquery_type'].lower()}) {a['name']}"
        for a in arch
    )
    if table == "dicionario":
        config = (
            f'        schema="{DATASET}",\n'
            f'        alias="{table}",\n'
            f'        materialized="table",\n'
        )
    else:
        config = (
            f'        schema="{DATASET}",\n'
            f'        alias="{table}",\n'
            f'        materialized="table",\n'
            f"        partition_by={{\n"
            f'            "field": "year",\n'
            f'            "data_type": "int64",\n'
            f'            "range": {{\n'
            f'                "start": {PARTITION_START},\n'
            f'                "end": {PARTITION_END},\n'
            f'                "interval": 1,\n'
            f"            }},\n"
            f"        }},\n"
        )
    return (
        "{{\n    config(\n"
        + config
        + "    )\n}}\n\n\nselect\n"
        + casts
        + "\nfrom\n"
        + f'    {{{{ set_datalake_project("{DATASET}_staging.{table}") }}}}\n'
        + "    as t\n"
    )


def schema_yaml() -> str:
    out = ["---", "version: 2", "models:"]
    for table in GRAIN:
        arch = read_arch(table)
        names = {a["name"] for a in arch}
        out.append(f"  - name: {DATASET}__{table}")
        # `>-` and not `>`: a folded scalar with `>` appends a trailing newline,
        # so the BigQuery description stops matching the registered one.
        out.append("    description: >-")
        out.extend(wrap(DESCRIPTIONS[table], "      "))
        out.append("    tests:")
        out.append("      - dbt_utils.unique_combination_of_columns:")
        out.append(
            "          combination_of_columns: ["
            + ", ".join(GRAIN[table])
            + "]"
        )
        if table != "dicionario":
            out.append("      - not_null_proportion_multiple_columns:")
            out.append("          at_least: 0.05")
            out.append("          config:")
            # The proportion test scans every column at compile time, which is
            # expensive on a table this wide and this long. Scope it to the
            # newest year; the uniqueness test above stays unscoped, because
            # a duplicate key in an older year has to fail the build.
            out.append("            where: __most_recent_year_en__")
            sparse = sorted(names & SPARSE)
            if sparse:
                out.append("          # Sparse by the source's structure, NOT")
                out.append("          # yet measured -- see build_dbt.py.")
                out.append("          ignore_values:")
                out.extend(f"            - {c}" for c in sparse)
            covered = sorted(
                a["name"] for a in arch if a["covered_by_dictionary"] == "yes"
            )
            if covered:
                out.append("      - custom_dictionary_coverage:")
                out.append(
                    f"          dictionary_model: ref('{DATASET}__dicionario')"
                )
                out.append("          columns_covered_by_dictionary:")
                out.extend(f"            - {c}" for c in covered)
        out.append("    columns:")
        for a in arch:
            out.append(f"      - name: {a['name']}")
            out.append("        description: >-")
            out.extend(wrap(a["description_pt"], "          "))
            tests = []
            if a["name"] in GRAIN[table]:
                tests.append("not_null")
            if table != "dicionario" and (
                a["name"] in RELATIONSHIPS or a["name"] in CUSTOM_RELATIONSHIPS
            ):
                out.append("        tests:")
                if tests:
                    out.append(f"          - {tests[0]}")
                if a["name"] in RELATIONSHIPS:
                    model, field = RELATIONSHIPS[a["name"]]
                    out.append("          - relationships:")
                    out.append(f"              to: ref('{model}')")
                    out.append(f"              field: {field}")
                else:
                    model, field, ignore = CUSTOM_RELATIONSHIPS[a["name"]]
                    out.append("          - custom_relationships:")
                    out.append(f"              to: ref('{model}')")
                    out.append(f"              field: {field}")
                    out.append(
                        "              # Measured against the directory;"
                    )
                    out.append(
                        "              # see build_dbt.py for each code."
                    )
                    out.append("              ignore_values: [")
                    out.append(
                        "                "
                        + ", ".join(f"'{v}'" for v in ignore)
                        + ","
                    )
                    out.append("              ]")
                continue
            if tests:
                out.append("        tests: [" + ", ".join(tests) + "]")
    return "\n".join(out) + "\n"


def main():
    for table in GRAIN:
        path = OUT / f"{DATASET}__{table}.sql"
        path.write_text(sql_model(table), encoding="utf-8")
        print(f"wrote {path.name}")
    (OUT / "schema.yml").write_text(schema_yaml(), encoding="utf-8")
    print("wrote schema.yml")


if __name__ == "__main__":
    main()
