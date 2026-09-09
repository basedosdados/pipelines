"""Write the dbt models and schema.yml for world_oecd_education.

One model per cube, reading the all-STRING staging external table and
``safe_cast``ing every column to its architecture type. Column order follows the
architecture CSV, which is the source of truth.

The uniqueness test uses the SDMX key -- ``year`` plus every dimension -- because
that is exactly what identifies an observation in a cube. It is a wide key (16
columns for ``student``), but a narrower one would not be unique: these are long
fact tables where the same measure recurs across every dimension combination.

Run ``python gen_dbt.py``, then ``pre-commit run sqlfmt --files models/world_oecd_education/*.sql``
-- the generator writes readable SQL but not sqlfmt-canonical SQL, and the hook
rewrites it.
"""

import csv
import json

from common import ARCH_DIR, CODE_DIR, DATASET_ID, OUTPUT, REPO_ROOT
from tables import TABLES

MODELS = REPO_ROOT / "models" / DATASET_ID

# Attributes describe an observation rather than identify it, so they are not
# part of the key even though they come from the source.
NON_KEY = {
    "obs_value",
    "source_flow",
    "source_flow_version",
    "country_iso3_code",
}


def sparse_columns(slug):
    """Columns under the 5% non-null threshold, measured by verify_parquet.py.

    These are exemptions from not_null_proportion_multiple_columns, and each is a
    measured fact rather than a guess. Most are SDMX attributes the DSD declares
    but the OECD never populates for that cube -- verified against the raw CSV
    for the student cube, where all seven are 0% populated at source too, so the
    emptiness is the source's and not the cleaner's. finance_subnational's
    country_iso3_code is empty by construction: that cube's areas are NUTS-style
    subnational entities, none of which is a country.
    """
    path = CODE_DIR / "measured.json"
    if not path.exists():
        raise FileNotFoundError(
            "run verify_parquet.py first to measure sparsity"
        )
    return json.loads(path.read_text())[slug]["sparse"]


def arch(slug):
    with (ARCH_DIR / f"{slug}.csv").open() as f:
        return list(csv.DictReader(f))


def dimensions(slug, spec):
    """The cube's dimension columns -- the part of the key that is not the year."""
    import xml.etree.ElementTree as ET

    from common import STRUCTURE

    s = "{http://www.sdmx.org/resources/sdmxml/schemas/v2_1/structure}"
    root = ET.parse(
        STRUCTURE / f"dsd_{spec['dsd']}_{spec['dsd_version']}.xml"
    ).getroot()
    dsd = next(root.iter(f"{s}DataStructure"))
    dim_list = dsd.find(f"{s}DataStructureComponents/{s}DimensionList")
    sdmx_dims = {
        d.get("id") for d in dim_list.iter(f"{s}Dimension") if d.get("id")
    }
    return [r["name"] for r in arch(slug) if r["original_name"] in sdmx_dims]


def cast(row):
    t = row["bigquery_type"].lower()
    return f"    safe_cast({row['name']} as {t}) {row['name']},"


def year_range(slug):
    """The partition range, read from the cleaned output rather than declared.

    BigQuery drops rows outside a range partition's bounds, so the range has to
    come from the data. The end is padded per the house convention.
    """
    years = sorted(
        int(p.name.split("=")[1]) for p in (OUTPUT / slug).glob("year=*")
    )
    if not years:
        raise FileNotFoundError(
            f"{slug} has no cleaned partitions; run clean.py first"
        )
    return years[0], years[-1] + 5


def model_sql(slug, spec, columns):
    start, end = year_range(slug)
    body = "\n".join(cast(r) for r in columns).rstrip(",")
    return f'''{{{{
    config(
        alias="{slug}",
        schema="{DATASET_ID}",
        materialized="table",
        partition_by={{
            "field": "year",
            "data_type": "int64",
            "range": {{"start": {start}, "end": {end}, "interval": 1}},
        }},
        cluster_by=["country_iso3_code"],
    )
}}}}


select
{body}
from
    {{{{ set_datalake_project("{DATASET_ID}_staging.{slug}") }}}}
    as t
'''


def schema_entry(slug, spec, columns, key):
    lines = [f"  - name: {DATASET_ID}__{slug}"]
    lines.append("    description: >-")
    for part in _wrap(spec["description_pt"]):
        lines.append(f"      {part}")
    lines.append("    tests:")
    lines.append("      - dbt_utils.unique_combination_of_columns:")
    lines.append(f"          combination_of_columns: [{', '.join(key)}]")
    lines.append("      - not_null_proportion_multiple_columns:")
    lines.append("          at_least: 0.05")
    sparse = sparse_columns(slug)
    if sparse:
        lines.append("          ignore_values:")
        for name in sparse:
            lines.append(f"            - {name}")
    coded = [r["name"] for r in columns if r["covered_by_dictionary"] == "yes"]
    if coded:
        # The facts carry codes only -- the labels are SDMX structure metadata --
        # so this test is what guarantees the dicionario describes every value
        # actually present, rather than the dictionary being derivable from the
        # facts the way a source that ships code and label together would allow.
        lines.append("      - custom_dictionary_coverage:")
        lines.append(
            f"          dictionary_model: ref('{DATASET_ID}__dicionario')"
        )
        lines.append("          columns_covered_by_dictionary:")
        for name in coded:
            lines.append(f"            - {name}")
    lines.append("    columns:")
    for r in columns:
        lines.append(f"      - name: {r['name']}")
        lines.append("        description: >-")
        for part in _wrap(r["description_pt"]):
            lines.append(f"          {part}")
        tests = []
        if r["name"] == "year":
            tests.append("          - not_null")
        if r["directory_column"]:
            ds_tbl, field = r["directory_column"].split(":")
            ref = ds_tbl.replace(".", "__")
            # A relationships test against the time directory needs the field
            # qualified as "<table>.<column>". dbt quotes the path in parts, so
            # the trailing `ano` becomes an implicit BigQuery range variable and
            # an unqualified `ano` binds to the whole STRUCT row rather than the
            # column -- the test then cannot pass. ~76 datasets in this repo
            # carry the broken form.
            if ref.endswith(("__ano", "__mes")):
                field = f"{ref.rsplit('__', 1)[1]}.{field}"
            tests.append("          - relationships:")
            tests.append(f"              to: ref('{ref}')")
            tests.append(f"              field: {field}")
        if tests:
            lines.append("        tests:")
            lines.extend(tests)
    return "\n".join(lines)


def _wrap(text, width=76):
    out, line = [], ""
    for word in text.split():
        if len(line) + len(word) + 1 > width:
            out.append(line)
            line = word
        else:
            line = f"{line} {word}".strip()
    if line:
        out.append(line)
    return out


def _dicionario_entry():
    """schema.yml entry for the dictionary model."""
    cols = {
        "id_tabela": (
            "Tabela deste conjunto a que a entrada se refere",
            "Table of this dataset the entry refers to",
            "Tabla de este conjunto a la que se refiere la entrada",
        ),
        "nome_coluna": (
            "Coluna codificada a que a entrada se refere",
            "Coded column the entry refers to",
            "Columna codificada a la que se refiere la entrada",
        ),
        "chave": (
            "Código tal como aparece na coluna",
            "Code exactly as it appears in the column",
            "Código tal como aparece en la columna",
        ),
        "cobertura_temporal": (
            "Cobertura temporal da entrada, vazia quando igual à da tabela",
            "Temporal coverage of the entry, empty when the same as the table's",
            "Cobertura temporal de la entrada, vacía cuando es igual a la de la tabla",
        ),
        "valor": (
            "Rótulo que a OCDE publica para o código",
            "Label the OECD publishes for the code",
            "Etiqueta que la OCDE publica para el código",
        ),
    }
    lines = [f"  - name: {DATASET_ID}__dicionario"]
    lines.append("    description: >-")
    for part in _wrap(
        "Rótulos das colunas codificadas deste conjunto, extraídos das listas de "
        "códigos SDMX publicadas pela OCDE. As tabelas de fatos carregam apenas "
        "códigos: os rótulos são metadados de estrutura, não dados."
    ):
        lines.append(f"      {part}")
    lines.append("    tests:")
    lines.append("      - dbt_utils.unique_combination_of_columns:")
    lines.append(
        "          combination_of_columns: [id_tabela, nome_coluna, chave]"
    )
    lines.append("    columns:")
    for name, (pt, _en, _es) in cols.items():
        lines.append(f"      - name: {name}")
        lines.append("        description: >-")
        for part in _wrap(pt):
            lines.append(f"          {part}")
        if name != "cobertura_temporal":
            lines.append("        tests: [not_null]")
    return "\n".join(lines)


def main():
    MODELS.mkdir(parents=True, exist_ok=True)
    entries = []
    for slug, spec in TABLES.items():
        columns = arch(slug)
        key = ["year"] + [
            d for d in dimensions(slug, spec) if d not in NON_KEY
        ]
        (MODELS / f"{DATASET_ID}__{slug}.sql").write_text(
            model_sql(slug, spec, columns)
        )
        entries.append(schema_entry(slug, spec, columns, key))
        print(f"  {slug:22s} {len(columns):2d} columns, key of {len(key)}")
    entries.append(_dicionario_entry())
    (MODELS / "schema.yml").write_text(
        "---\nversion: 2\nmodels:\n" + "\n".join(entries) + "\n"
    )
    print(f"{len(TABLES)} models + schema.yml in {MODELS}")


if __name__ == "__main__":
    main()
