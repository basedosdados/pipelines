"""Write the dbt models and schema.yml for us_cms_hcris.

    python gen_dbt.py

Four models are written to ``models/us_cms_hcris/``:

* ``report`` and ``report_value`` read the all-STRING staging tables and
  ``safe_cast`` every column to its architecture type.
* ``hospital_financial`` pivots ``report_value`` against the published
  worksheet-line-column mapping in ``measures.py``. It reads the *model*, not
  staging, so the mapping is applied to typed values once and the provenance of
  every measure is literally the SQL.
* ``dicionario`` is a literal value list from CMS's own documentation. Unlike a
  dataset whose facts carry both a code and its label, HCRIS publishes codes
  only, so there is nothing to derive the labels from — they come from
  ``HCRIS_DataDictionary.csv`` and CMS Pub. 15-2 via ``codes.py``.

Nothing here is hand-edited: the models are regenerated from ``schema.py``,
``measures.py`` and ``codes.py``, so a renamed column or a corrected cell
address changes exactly one place.
"""

import csv
import json
from pathlib import Path

from codes import CODES, COVERED
from measures import MEASURES
from schema import TABLES

CODE_DIR = Path(__file__).resolve().parent
MODEL_DIR = CODE_DIR.parent
ARCH = CODE_DIR / "architecture"
DATASET = "us_cms_hcris"

PARTITION = {
    "field": "year",
    "data_type": "int64",
    "range": {"start": 1994, "end": 2031, "interval": 1},
}

# report_value is scanned by the hospital_financial pivot with a predicate on
# the cell address, so it is clustered on that address. report and
# hospital_financial are clustered on the hospital, which is how they are read.
CLUSTER = {
    "report": ["provider_ccn"],
    "report_value": ["worksheet_code", "line_number", "column_number"],
    "hospital_financial": ["provider_ccn"],
}

SQL_ESCAPE = str.maketrans({"'": "\\'"})


def read_arch(table: str) -> list[dict[str, str]]:
    """Read one architecture CSV.

    Args:
        table: Table slug.

    Returns:
        One dict per column, in architecture order.
    """
    with (ARCH / f"{table}.csv").open() as fh:
        return list(csv.DictReader(fh))


def config(table: str, partitioned: bool = True) -> str:
    """Render a model's dbt config block.

    Args:
        table: Table slug.
        partitioned: Whether to emit a partition_by clause.

    Returns:
        The ``{{ config(...) }}`` block, newline-terminated.
    """
    lines = [
        "{{",
        "    config(",
        f'        schema="{DATASET}",',
        f'        alias="{table}",',
        '        materialized="table",',
    ]
    if partitioned:
        lines += [
            "        partition_by={",
            f'            "field": "{PARTITION["field"]}",',
            f'            "data_type": "{PARTITION["data_type"]}",',
            f'            "range": {{"start": {PARTITION["range"]["start"]}, '
            f'"end": {PARTITION["range"]["end"]}, "interval": 1}},',
            "        },",
        ]
    if table in CLUSTER:
        cols = ", ".join(f'"{c}"' for c in CLUSTER[table])
        lines.append(f"        cluster_by=[{cols}],")
    lines += ["    )", "}}", ""]
    return "\n".join(lines)


def staging_model(table: str) -> str:
    """Render a model that safe_casts one all-STRING staging table.

    Args:
        table: Table slug.

    Returns:
        The complete .sql file contents.
    """
    cols = read_arch(table)
    casts = ",\n".join(
        f"    safe_cast({c['name']} as {c['bigquery_type'].lower()}) {c['name']}"
        for c in cols
    )
    return (
        config(table)
        + "\nselect\n"
        + casts
        + "\nfrom "
        + f'{{{{ set_datalake_project("{DATASET}_staging.{table}") }}}} as t\n'
    )


def cell_predicate(cells, form: str) -> str:
    """Render the SQL matching one measure's cells on one form version.

    ``form_version`` is included even though it is redundant today — every
    2552-96 column number is four characters and every 2552-10 one is five, so
    the two address spaces cannot collide. Naming the form anyway keeps the
    predicate correct if CMS ever reuses a width, and keeps the SQL readable as
    a statement about a specific form.

    Args:
        cells: The measure's ``Cell`` list for that form.
        form: Form version.

    Returns:
        A boolean SQL expression.
    """
    parts = []
    for c in cells:
        line = (
            f"line_number between '{c.line}' and '{c.line_end}'"
            if c.line_end
            else f"line_number = '{c.line}'"
        )
        parts.append(
            f"worksheet_code = '{c.worksheet}' and {line} "
            f"and column_number = '{c.column}'"
        )
    joined = " or ".join(f"({p})" for p in parts)
    return f"form_version = '{form}' and ({joined})"


def measure_expression(measure) -> str:
    """Render one measure as an aggregate over the cell table.

    Numeric measures are summed, which is what a line run or a multi-line
    total requires and which reduces to the value itself for a single cell —
    the cell key (report, worksheet, line, column) is unique, so nothing is
    double counted. Text measures take the single value present.

    Args:
        measure: A ``measures.Measure``.

    Returns:
        The ``<aggregate> as <name>`` fragment.
    """
    preds = " or ".join(
        f"({cell_predicate(cells, form)})"
        for form, cells in sorted(measure.by_form.items())
    )
    if measure.kind == "alpha":
        return f"        max(case when {preds} then alpha_value end)\n            as {measure.name}"
    value = "abs(numeric_value)" if measure.absolute else "numeric_value"
    return f"        sum(case when {preds} then {value} end)\n            as {measure.name}"


def hospital_financial_model() -> str:
    """Render the curated model as a pivot of report_value.

    Returns:
        The complete .sql file contents.
    """
    worksheets = sorted(
        {
            c.worksheet
            for m in MEASURES
            for cells in m.by_form.values()
            for c in cells
        }
    )
    ws_list = ", ".join(f"'{w}'" for w in worksheets)
    carried = [
        "year",
        "report_id",
        "provider_ccn",
        "state_id",
        "state_abbreviation",
        "form_version",
        "fiscal_year_begin_date",
        "fiscal_year_end_date",
        "fiscal_year_days",
        "report_status_code",
    ]
    # Built outside the f-string: Python 3.11 rejects a backslash inside an
    # f-string expression, and every one of these joins on a newline.
    aggregates = ",\n".join(measure_expression(m) for m in MEASURES)
    carried_sql = ",\n".join(f"    r.{c}" for c in carried)
    measures_sql = ",\n".join(f"    p.{m.name}" for m in MEASURES)
    body = f"""
-- One row per cost report, with the named measures the published
-- worksheet-line-column mappings define. Built from the report_value model
-- rather than from staging so the mapping is applied to typed values, and so
-- the provenance of every measure below is the SQL itself.
--
-- The grain is the report, not the hospital-year. CMS states plainly that one
-- hospital can file two or more reports for the same year -- a fiscal year
-- change, or a change of ownership -- and the collapse rules the published
-- research mappings use for that disagree with each other. Publishing the
-- report grain leaves that choice to the user; fiscal_year_begin_date,
-- fiscal_year_end_date and fiscal_year_days are the columns to make it with.
with
    cells as (
        select
            report_id,
            form_version,
            worksheet_code,
            line_number,
            column_number,
            numeric_value,
            alpha_value
        from {{{{ ref("{DATASET}__report_value") }}}}
        -- Only the {len(worksheets)} worksheets the mapping reads, out of the 884 HCRIS
        -- publishes. With the model clustered on the cell address this prunes
        -- most of the table before the pivot.
        where worksheet_code in ({ws_list})
    ),
    pivoted as (
        select
        form_version,
        report_id,
{aggregates}
        from cells
        -- Grouped by the real key. report_id alone would merge the 107 ids CMS
        -- reused across the two form versions into single rows holding two
        -- unrelated hospitals' values.
        group by form_version, report_id
    )

select
{carried_sql},
{measures_sql}
from {{{{ ref("{DATASET}__report") }}}} as r
left join
    pivoted as p
    on p.report_id = r.report_id and p.form_version = r.form_version
"""
    return config("hospital_financial") + body


def dicionario_model() -> str:
    """Render the dictionary as a literal value list.

    Returns:
        The complete .sql file contents.
    """
    rows = []
    for table, columns in COVERED.items():
        for column in columns:
            for key, (pt, _en, _es) in CODES[column].items():
                rows.append(
                    "    select "
                    f"'{table}' as id_tabela, "
                    f"'{column}' as nome_coluna, "
                    f"'{key}' as chave, "
                    f"'' as cobertura_temporal, "
                    f"'{pt.translate(SQL_ESCAPE)}' as valor"
                )
    body = (
        "\n-- CMS publishes codes without labels, so unlike a dataset whose facts\n"
        "-- carry both there is nothing here to derive the meanings from. Every\n"
        "-- value below is transcribed from HCRIS_DataDictionary.csv or from CMS\n"
        "-- Publication 15-2 section 4004.1; see code/codes.py for which.\n"
        "select id_tabela, nome_coluna, chave, cobertura_temporal, valor\n"
        "from (\n" + "\n    union all\n".join(rows) + "\n)\n"
    )
    return config("dicionario", partitioned=False) + body


def main() -> None:
    """Write the four models and schema.yml."""
    for table in ("report", "report_value"):
        path = MODEL_DIR / f"{DATASET}__{table}.sql"
        path.write_text(staging_model(table))
        print(f"wrote {path.relative_to(MODEL_DIR.parent)}")
    for table, render in (
        ("hospital_financial", hospital_financial_model),
        ("dicionario", dicionario_model),
    ):
        path = MODEL_DIR / f"{DATASET}__{table}.sql"
        path.write_text(render())
        print(f"wrote {path.relative_to(MODEL_DIR.parent)}")
    ignore = json.loads((CODE_DIR / "null_proportions.json").read_text())
    path = MODEL_DIR / "schema.yml"
    path.write_text(schema_yml(ignore))
    print(f"wrote {path.relative_to(MODEL_DIR.parent)}")


# --------------------------------------------------------------------------
# schema.yml
# --------------------------------------------------------------------------

# Uniqueness key per model. It is (form_version, report_id), NOT report_id:
# CMS restarted the RPT_REC_NUM sequence for Form CMS-2552-10, so 107 ids are
# reused between the two forms, for unrelated hospitals and unrelated years.
# Caught by this very test on its first run against all 182,903 reports.
UNIQUE = {
    "report": ["form_version", "report_id"],
    "report_value": [
        "form_version",
        "report_id",
        "worksheet_code",
        "line_number",
        "column_number",
    ],
    "hospital_financial": ["form_version", "report_id"],
    "dicionario": ["id_tabela", "nome_coluna", "chave"],
}

NOT_NULL = {
    "report": ["year", "report_id", "provider_ccn"],
    "report_value": [
        "year",
        "report_id",
        "worksheet_code",
        "line_number",
        "column_number",
    ],
    "hospital_financial": ["year", "report_id", "provider_ccn"],
    "dicionario": ["id_tabela", "nome_coluna", "chave", "valor"],
}

# Directory table and field for each foreign key. The time directory needs
# `ano.ano` rather than `ano`: dbt quotes the path in parts, so the trailing
# `ano` becomes a BigQuery range variable and an unqualified `ano` binds to the
# whole row STRUCT instead of the column, and the test can never pass.
# See [[reference_dbt_time_directory_relationships_broken]].
RELATIONSHIPS = {
    "year": ("br_bd_diretorios_data_tempo__ano", "ano.ano"),
    "state_id": ("br_bd_diretorios_us__state", "id_state"),
}

TABLE_DESCRIPTION = {
    "report": (
        "Índice dos relatórios de custos do Medicare (HCRIS) apresentados por "
        "hospitais certificados pelo Medicare nos Estados Unidos, com uma linha "
        "por relatório: prestador, período do exercício fiscal, situação, versão "
        "do formulário e datas de processamento. Corresponde ao arquivo RPT dos "
        "extratos trimestrais da CMS para os formulários CMS-2552-96 e "
        "CMS-2552-10. A chave report_id une esta tabela a report_value e a "
        "hospital_financial."
    ),
    "report_value": (
        "Todos os valores declarados nos relatórios de custos do Medicare, em "
        "formato longo: uma linha por célula (relatório, planilha, linha, "
        "coluna), com o valor numérico e o valor de texto em colunas separadas. "
        "Corresponde aos arquivos NMRC e ALPHA dos extratos trimestrais da CMS, "
        "unidos pela chave da célula. É a forma bruta e completa do formulário; "
        "as medidas nomeadas de uso corrente estão em hospital_financial."
    ),
    "hospital_financial": (
        "Medidas financeiras e operacionais nomeadas de cada relatório de custos "
        "do Medicare, uma linha por relatório: receitas, despesas, cobranças, "
        "leitos, altas, atendimento não remunerado e balanço patrimonial. Cada "
        "coluna é derivada de um endereço de planilha, linha e coluna do "
        "formulário da CMS, registrado no campo observations da coluna. O grão é "
        "o relatório e não o hospital-ano: a CMS documenta que um hospital pode "
        "apresentar dois ou mais relatórios para o mesmo ano."
    ),
    "dicionario": (
        "Dicionário dos códigos usados nas colunas codificadas deste conjunto, "
        "transcrito da documentação da própria CMS."
    ),
}


def yaml_block(text: str, indent: str) -> str:
    """Render a description as a folded YAML block scalar.

    A bare scalar breaks on any colon in a continuation line, which several of
    these descriptions have. The chomping indicator matters: plain ``>`` keeps
    one trailing newline, which dbt then writes into the BigQuery column
    description, so the published description differs from the API's by a
    trailing ``\n``. The `check-metadata` CI job compares exactly those two and
    flagged every column of this dataset. ``>-`` strips it. 61 of the repo's
    schema.yml files already use ``>-``.

    Args:
        text: The description.
        indent: Leading whitespace for the body.

    Returns:
        ``>``-style block, newline-terminated.
    """
    words, lines, cur = text.split(), [], ""
    for w in words:
        if len(cur) + len(w) + 1 > 78:
            lines.append(cur)
            cur = w
        else:
            cur = f"{cur} {w}".strip()
    lines.append(cur)
    body = "\n".join(f"{indent}{line}" for line in lines)
    return ">-\n" + body + "\n"


def schema_yml(ignore: dict[str, list[str]]) -> str:
    """Render schema.yml for all four models.

    Args:
        ignore: Per-table columns to exempt from the non-null proportion test,
            measured by ``null_proportions.py``.

    Returns:
        The complete schema.yml contents.
    """
    out = ["---", "version: 2", "models:"]
    for table, cols in TABLES.items():
        arch = {c["name"]: c for c in read_arch(table)}
        covered = sorted(set(COVERED.get(table, [])))
        out.append(f"  - name: {DATASET}__{table}")
        out.append(
            "    description: "
            + yaml_block(TABLE_DESCRIPTION[table], "      ").rstrip("\n")
        )
        out.append("    tests:")
        out.append("      - dbt_utils.unique_combination_of_columns:")
        out.append(
            f"          combination_of_columns: [{', '.join(UNIQUE[table])}]"
        )
        out.append("      - not_null_proportion_multiple_columns:")
        out.append("          at_least: 0.05")
        if ignore.get(table):
            out.append("          ignore_values:")
            out += [f"            - {c}" for c in ignore[table]]
        if covered:
            out.append("      - custom_dictionary_coverage:")
            out.append(
                f"          dictionary_model: ref('{DATASET}__dicionario')"
            )
            out.append(
                f"          columns_covered_by_dictionary: [{', '.join(covered)}]"
            )
        out.append("    columns:")
        for col in cols:
            out.append(f"      - name: {col.name}")
            desc = arch[col.name]["description_pt"]
            out.append(
                "        description: "
                + yaml_block(desc, "          ").rstrip("\n")
            )
            tests = []
            if col.name in NOT_NULL[table]:
                tests.append("          - not_null")
            if col.name in RELATIONSHIPS and col.directory:
                model, field = RELATIONSHIPS[col.name]
                tests += [
                    "          - relationships:",
                    f"              to: ref('{model}')",
                    f"              field: {field}",
                ]
            if tests:
                out.append("        tests:")
                out += tests
    return "\n".join(out) + "\n"


if __name__ == "__main__":
    main()
