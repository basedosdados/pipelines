"""Generate models/us_usda_nass/schema.yml from the architecture CSVs.

One entry per fact table (per-grain) plus the dicionario, with:
- dbt_utils.unique_combination_of_columns on the grain's natural key,
- not_null_proportion_multiple_columns (ignoring the reliably-sparse columns),
- custom_dictionary_coverage on value_suppression_flag,
- not_null on year, the grain's key geography column(s), commodity, statistic_category.

Run: ``uv run python models/us_usda_nass/code/build_schema_yml.py``
"""

import csv
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
ARCH = ROOT / "code" / "architecture"

FACT_TABLES = [
    "survey_national",
    "survey_state",
    "survey_agricultural_district",
    "survey_county",
    "census_of_agriculture_national",
    "census_of_agriculture_state",
    "census_of_agriculture_county",
]

KEY_SET = {
    "year",
    "state_fips",
    "agricultural_district_code",
    "county_fips",
    "commodity",
    "commodity_class",
    "production_practice",
    "utilization_practice",
    "statistic_category",
    "unit",
    "domain",
    "domain_category",
    "reference_period",
}
NOT_NULL = {
    "year",
    "state_fips",
    "agricultural_district_code",
    "county_fips",
    "commodity",
    "statistic_category",
}
SPARSE_IGNORE = ["value_suppression_flag", "coefficient_of_variation"]

# Grain phrase for the table description.
GRAIN_PT = {
    "national": "no nível nacional dos EUA",
    "state": "no nível estadual",
    "agricultural_district": "no nível de distrito estatístico agrícola (ASD)",
    "county": "no nível de condado",
}


def grain_of(table):
    for g in ("agricultural_district", "national", "state", "county"):
        if table.endswith("_" + g):
            return g
    raise ValueError(table)


def source_pt(table):
    return (
        "Censo Agropecuário quinquenal do USDA NASS QuickStats"
        if table.startswith("census_of_agriculture")
        else "Estatísticas de pesquisa (survey) do USDA NASS QuickStats"
    )


def read_arch(table):
    with open(ARCH / f"{table}.csv", encoding="utf-8") as f:
        return [(r["name"], r["description"]) for r in csv.DictReader(f)]


def block_scalar(text, indent):
    pad = " " * indent
    return f">\n{pad}" + text


def build_fact(table):
    cols = read_arch(table)
    names = [n for n, _ in cols]
    key = [n for n in names if n in KEY_SET]
    grain = grain_of(table)
    desc = (
        f"{source_pt(table)} {GRAIN_PT[grain]}, em formato longo: uma linha por "
        f"ano, geografia, commodity, categoria estatística, domínio e unidade. "
        f"A unidade de medida varia por linha e está na coluna unit."
    )
    lines = [f"  - name: us_usda_nass__{table}"]
    lines.append(f"    description: {block_scalar(desc, 6)}")
    lines.append("    tests:")
    lines.append("      - dbt_utils.unique_combination_of_columns:")
    lines.append("          combination_of_columns:")
    lines += [f"            - {c}" for c in key]
    lines.append("      - not_null_proportion_multiple_columns:")
    lines.append("          at_least: 0.05")
    lines.append("          ignore_values:")
    lines += [f"            - {c}" for c in SPARSE_IGNORE]
    lines.append("      - custom_dictionary_coverage:")
    lines.append("          dictionary_model: ref('us_usda_nass__dicionario')")
    lines.append("          columns_covered_by_dictionary:")
    lines.append("            - value_suppression_flag")
    lines.append("    columns:")
    for n, d in cols:
        dq = d.replace("\\", "\\\\").replace('"', '\\"')
        lines.append(f"      - name: {n}")
        lines.append(f'        description: "{dq}"')
        if n in NOT_NULL:
            lines.append("        tests: [not_null]")
    return "\n".join(lines)


DICIONARIO = """  - name: us_usda_nass__dicionario
    description: >
      Dicionário de valores codificados do conjunto us_usda_nass e seus rótulos
      legíveis. Cobre a coluna value_suppression_flag das tabelas de pesquisa e
      censo (códigos de supressão do NASS, como (D) e (Z)).
    columns:
      - name: id_tabela
        description: Nome da tabela à qual a coluna pertence
      - name: nome_coluna
        description: Nome da coluna codificada
      - name: chave
        description: Valor codificado
      - name: cobertura_temporal
        description: Cobertura temporal do mapeamento
      - name: valor
        description: Rótulo legível correspondente à chave"""


def main():
    out = ["---", "version: 2", "models:"]
    for t in FACT_TABLES:
        out.append(build_fact(t))
    out.append(DICIONARIO)
    (ROOT / "schema.yml").write_text("\n".join(out) + "\n", encoding="utf-8")
    print(f"  wrote schema.yml ({len(FACT_TABLES)} fact tables + dicionario)")


if __name__ == "__main__":
    main()
