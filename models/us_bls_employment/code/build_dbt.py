"""Generate the us_bls_employment dbt models and schema.yml from the architecture.

The architecture CSVs are the single source of truth for column order, types and
descriptions (see .claude/rules/onboarding-workflow.md), so the models are
generated from them rather than hand-maintained alongside them.

Run: uv run python models/us_bls_employment/code/build_dbt.py

The committed models are the pre-commit-formatted versions (sqlfmt
normalises quoting, yamlfix restyles schema.yml), so a regen shows a
formatting-only diff until those hooks run over it again.
"""

from pathlib import Path

# PyYAML ships no type stubs and types-PyYAML is not a dependency of this
# repo. Other datasets avoid the diagnostic by having their whole code/
# directory in pyrefly's project-excludes; suppressing the one import keeps
# the rest of this file type-checked.
# pyrefly: ignore [untyped-import]
import yaml

from pipelines.datasets.us_bls_employment.constants import constants
from pipelines.datasets.us_bls_employment.utils import read_arch

DATASET = constants.DATASET_ID.value
MODELS = Path(__file__).resolve().parents[1]

# Earliest year in each program, from the cleaning run; end is the latest year
# plus five, per the partitioning convention.
PARTITION = {
    "ces_national": {"start": 1939, "end": 2031, "interval": 1},
    "ces_state_metro": {"start": 1939, "end": 2031, "interval": 1},
    "laus": {"start": 1976, "end": 2031, "interval": 1},
    "jolts": {"start": 2000, "end": 2031, "interval": 1},
}

CLUSTER = {
    "ces_national": ["seasonal_adjustment", "industry_id", "data_type_id"],
    "ces_state_metro": ["seasonal_adjustment", "state_id", "industry_id"],
    "laus": ["seasonal_adjustment", "state_id", "measure_id"],
    "jolts": ["seasonal_adjustment", "industry_id", "dataelement_id"],
}

# Foreign keys with a documented, closed set of values that cannot resolve. They
# use custom_relationships, which takes an exclusion list, so that any *new*
# unmatched code still fails the build instead of being absorbed silently.
IGNORE_VALUES = {
    # Alaska census areas retired before the current county delineation.
    ("laus", "county_id"): ["02201", "02232", "02261", "02280"],
}

DESCRIPTIONS = {
    "ces_national": (
        "Estatísticas nacionais de emprego, horas e salários do Current "
        "Employment Statistics (CES), a pesquisa mensal de estabelecimentos do "
        "BLS. Uma linha por série, ano e período, cobrindo emprego em folha de "
        "pagamento, horas semanais médias, salários médios por hora e por "
        "semana, horas e folhas agregadas, índices de difusão e taxas de "
        "resposta, por setor de atividade e ajuste sazonal."
    ),
    "ces_state_metro": (
        "Estatísticas estaduais e metropolitanas de emprego, horas e salários "
        "do State and Metro Area Employment, Hours, and Earnings (SAE), a "
        "desagregação geográfica da pesquisa de estabelecimentos do BLS. Uma "
        "linha por série, ano e período, para cada estado, território e área "
        "metropolitana ou micropolitana, por setor de atividade e ajuste "
        "sazonal."
    ),
    "laus": (
        "Estimativas mensais de força de trabalho, emprego, desemprego e taxa "
        "de desemprego do Local Area Unemployment Statistics (LAUS), para "
        "estados, condados, áreas metropolitanas e micropolitanas, cidades e "
        "demais áreas locais dos Estados Unidos. Uma linha por série, ano e "
        "período, com ajuste sazonal identificado como dimensão. Quatro áreas "
        "censitárias do Alasca extintas antes da delimitação atual de condados "
        "(02201, 02232, 02261, 02280) aparecem entre 1990 e 2019 e não resolvem "
        "contra o diretório de condados; o teste de relacionamento de county_id "
        "as trata como exceção declarada."
    ),
    "jolts": (
        "Vagas abertas, contratações e desligamentos do Job Openings and Labor "
        "Turnover Survey (JOLTS). Uma linha por série, ano e período, cobrindo "
        "vagas abertas, contratações, desligamentos totais, pedidos de "
        "demissão, demissões e dispensas, e a razão entre desempregados e vagas "
        "abertas, em nível e em taxa, por setor de atividade, região, classe de "
        "tamanho do estabelecimento e ajuste sazonal."
    ),
    "dicionario": (
        "Dicionário de códigos das tabelas do conjunto us_bls_employment, com o "
        "rótulo correspondente a cada código armazenado nas colunas codificadas."
    ),
}

# Directory foreign keys, as declared in the architecture. The time directory
# stores its key inside a STRUCT, so the relationships test binds to `ano.ano`
# and `mes.mes` rather than the bare column.
REL = {
    "br_bd_diretorios_data_tempo.ano:ano": (
        "br_bd_diretorios_data_tempo__ano",
        "ano.ano",
    ),
    "br_bd_diretorios_data_tempo.mes:mes": (
        "br_bd_diretorios_data_tempo__mes",
        "mes.mes",
    ),
    "br_bd_diretorios_us.state:id_state": (
        "br_bd_diretorios_us__state",
        "id_state",
    ),
    "br_bd_diretorios_us.county:id_county": (
        "br_bd_diretorios_us__county",
        "id_county",
    ),
    "br_bd_diretorios_us.cbsa_2023:id_cbsa": (
        "br_bd_diretorios_us__cbsa_2023",
        "id_cbsa",
    ),
    "br_bd_diretorios_us.naics_2022:id_naics": (
        "br_bd_diretorios_us__naics_2022",
        "id_naics",
    ),
}

# The natural key of each fact table: what makes an observation unique.
UNIQUE_KEY = {
    "ces_national": ["year", "period_id", "series_id"],
    "ces_state_metro": ["year", "period_id", "series_id"],
    "laus": ["year", "period_id", "series_id"],
    "jolts": ["year", "period_id", "series_id"],
    "dicionario": ["id_tabela", "nome_coluna", "chave"],
}

# not_null is asserted only where the column is genuinely always present.
# `month` is NULL on the annual-average rows, `value` wherever BLS printed "-",
# and every foreign key is NULL on the aggregate rows it does not describe.
NOT_NULL = {
    "year",
    "period_id",
    "series_id",
    "seasonal_adjustment",
    "id_tabela",
    "nome_coluna",
    "chave",
    "valor",
}


def sql(table: str) -> str:
    """Render one dbt model."""
    arch = read_arch(table)
    casts = ",\n".join(
        f"    safe_cast({a['name']} as {a['bigquery_type'].lower()}) {a['name']}"
        for a in arch
    )
    if table == "dicionario":
        cfg = (
            f'        schema="{DATASET}",\n'
            f'        alias="{table}",\n'
            '        materialized="table",\n'
        )
    else:
        p = PARTITION[table]
        cfg = (
            f'        schema="{DATASET}",\n'
            f'        alias="{table}",\n'
            '        materialized="table",\n'
            "        partition_by={\n"
            '            "field": "year",\n'
            '            "data_type": "int64",\n'
            f'            "range": {{"start": {p["start"]}, '
            f'"end": {p["end"]}, "interval": {p["interval"]}}},\n'
            "        },\n"
            f"        cluster_by={CLUSTER[table]!r},\n"
        )
    return (
        "{{\n    config(\n"
        + cfg
        + "    )\n}}\n\n\nselect\n"
        + casts
        + f'\nfrom {{{{ set_datalake_project("{DATASET}_staging.{table}") }}}} as t\n'
    )


def model_yaml(table: str) -> dict:
    """Render one schema.yml model entry."""
    arch = read_arch(table)
    cols = []
    for a in arch:
        entry: dict = {"name": a["name"], "description": a["description"]}
        tests: list = []
        if a["name"] in NOT_NULL:
            tests.append("not_null")
        if a["directory_column"] in REL:
            ref, field = REL[a["directory_column"]]
            ignore = IGNORE_VALUES.get((table, a["name"]))
            if ignore:
                tests.append(
                    {
                        "custom_relationships": {
                            "to": f"ref('{ref}')",
                            "field": field,
                            "ignore_values": ignore,
                            "proportion_allowed_failures": 0,
                        }
                    }
                )
            else:
                tests.append(
                    {"relationships": {"to": f"ref('{ref}')", "field": field}}
                )
        if tests:
            entry["tests"] = tests
        cols.append(entry)
    # not_null_proportion ignores the columns that are legitimately sparse:
    # foreign keys that only describe some rows, and footnotes.
    sparse = [
        a["name"]
        for a in arch
        if a["name"]
        in {
            "month",
            "naics_id",
            "state_id",
            "county_id",
            "cbsa_id",
            "region_id",
            "footnote_id",
            "cobertura_temporal",
        }
    ]
    proportion: dict = {"at_least": 0.05}
    if sparse:
        proportion["ignore_values"] = sparse
    return {
        "name": f"{DATASET}__{table}",
        "description": DESCRIPTIONS[table],
        "tests": [
            {
                "dbt_utils.unique_combination_of_columns": {
                    "combination_of_columns": UNIQUE_KEY[table]
                }
            },
            {"not_null_proportion_multiple_columns": proportion},
        ],
        "columns": cols,
    }


def main() -> None:
    """Write every model and the dataset's schema.yml."""
    for table in constants.ALL_TABLES.value:
        (MODELS / f"{DATASET}__{table}.sql").write_text(sql(table))
    schema = {
        "version": 2,
        "models": [model_yaml(t) for t in constants.ALL_TABLES.value],
    }
    text = yaml.dump(schema, sort_keys=False, allow_unicode=True, width=88)
    (MODELS / "schema.yml").write_text("---\n" + text)
    print(f"wrote {len(constants.ALL_TABLES.value)} models + schema.yml")


if __name__ == "__main__":
    main()
