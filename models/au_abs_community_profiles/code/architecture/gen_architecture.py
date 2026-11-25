"""Generate architecture CSVs for au_abs_community_profiles (14 tables).

Long store: one table per geography level (national + 11 levelled) carrying
census_year / id_<level> / profile / table_code / cell_code / value, plus an
auxiliary_info cell catalogue and a dicionario (profile codes).

Descriptions follow the Data Basis style manual: capitalized first letter, NO
trailing period on column descriptions. Column NAMES are English (English-language
dataset); descriptions trilingual PT/EN/ES.
"""

import csv
import os

HERE = os.path.dirname(os.path.abspath(__file__))
DIR_DS = "br_bd_diretorios_au"  # geography directory (gcp_dataset_id)
TIME_DS = "br_bd_diretorios_data_tempo"

HEADER = [
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


def col(
    name, btype, desc, unit="", dic="no", directory="", obs="", original=""
):
    return {
        "name": name,
        "bigquery_type": btype,
        "description": desc,
        "temporal_coverage": "",
        "covered_by_dictionary": dic,
        "directory_column": directory,
        "measurement_unit": unit,
        "has_sensitive_data": "no",
        "observations": obs,
        "original_name": original,
    }


# geo level -> (bd table, id column, directory table @2021, name pt/en/es)
LEVELS = {
    "state": (
        "id_state",
        "state",
        "estados e territórios",
        "states and territories",
        "estados y territorios",
    ),
    "sa1": (
        "id_sa1",
        "sa1_2021",
        "Statistical Areas Level 1 (SA1)",
        "Statistical Areas Level 1 (SA1)",
        "Statistical Areas Level 1 (SA1)",
    ),
    "sa2": (
        "id_sa2",
        "sa2_2021",
        "Statistical Areas Level 2 (SA2)",
        "Statistical Areas Level 2 (SA2)",
        "Statistical Areas Level 2 (SA2)",
    ),
    "sa3": (
        "id_sa3",
        "sa3_2021",
        "Statistical Areas Level 3 (SA3)",
        "Statistical Areas Level 3 (SA3)",
        "Statistical Areas Level 3 (SA3)",
    ),
    "sa4": (
        "id_sa4",
        "sa4_2021",
        "Statistical Areas Level 4 (SA4)",
        "Statistical Areas Level 4 (SA4)",
        "Statistical Areas Level 4 (SA4)",
    ),
    "gccsa": (
        "id_gccsa",
        "gccsa_2021",
        "Greater Capital City Statistical Areas (GCCSA)",
        "Greater Capital City Statistical Areas (GCCSA)",
        "Greater Capital City Statistical Areas (GCCSA)",
    ),
    "lga": (
        "id_lga",
        "lga_2021",
        "áreas de governo local (LGA)",
        "local government areas (LGA)",
        "áreas de gobierno local (LGA)",
    ),
    "suburb": (
        "id_suburb",
        "suburb_2021",
        "subúrbios e localidades",
        "suburbs and localities",
        "suburbios y localidades",
    ),
    "postal_area": (
        "id_postal_area",
        "postal_area_2021",
        "áreas postais (POA)",
        "postal areas (POA)",
        "áreas postales (POA)",
    ),
    "commonwealth_electoral_division": (
        "id_commonwealth_electoral_division",
        "commonwealth_electoral_division_2021",
        "divisões eleitorais federais (CED)",
        "Commonwealth electoral divisions (CED)",
        "divisiones electorales federales (CED)",
    ),
    "state_electoral_division": (
        "id_state_electoral_division",
        "state_electoral_division_2021",
        "divisões eleitorais estaduais (SED)",
        "state electoral divisions (SED)",
        "divisiones electorales estatales (SED)",
    ),
}

CENSUS_YEAR = col(
    "census_year",
    "INT64",
    "Ano do Censo (2011, 2016 ou 2021)",
    unit="year",
    directory=f"{TIME_DS}.ano:ano",
    obs="Coluna de particionamento. EN: Census year (2011, 2016 or 2021). ES: Año del Censo (2011, 2016 o 2021)",
    original="census_year",
)
PROFILE = col(
    "profile",
    "STRING",
    "Perfil do Censo de origem (GCP = General Community Profile; TSP = Time Series Profile)",
    obs="EN: Source Census profile (GCP/TSP). ES: Perfil del Censo de origen (GCP/TSP)",
    original="profile",
)
TABLE_CODE = col(
    "table_code",
    "STRING",
    "Código da tabela do perfil (por exemplo G01, B01, T01); nome e população em auxiliary_info",
    obs="EN: Profile table code (e.g. G01/B01/T01). ES: Código de la tabla del perfil",
    original="table_code",
)
CELL_CODE = col(
    "cell_code",
    "STRING",
    "Código da célula (variável) do perfil; descrição, estatística e unidade em auxiliary_info",
    obs="EN: Profile cell (variable) code; see auxiliary_info. ES: Código de la celda del perfil",
    original="cell_code",
)
VALUE = col(
    "value",
    "FLOAT64",
    "Valor da célula; a unidade de medida varia por célula (contagem, mediana ou média — ver auxiliary_info)",
    obs="Unidade por célula em auxiliary_info (measurement_unit); mistura contagens e medianas/médias. EN/ES idem",
    original="value",
)


def geo_id_col(level):
    id_col, dir_tbl, _pt, en, es = LEVELS[level]
    label = en.split(" (")[0]
    return col(
        id_col,
        "STRING",
        f"Código identificador da unidade geográfica ({label})",
        directory=f"{DIR_DS}.{dir_tbl}:{id_col}",
        obs=(
            "FK à edição 2021 do diretório; para census_year 2011/2016 refere-se "
            f"à edição daquele ano (teste dbt por ano). EN: {label} code. "
            f"ES: código de {es}"
        ),
        original=id_col,
    )


def geo_table(level):
    return [
        CENSUS_YEAR,
        geo_id_col(level),
        PROFILE,
        TABLE_CODE,
        CELL_CODE,
        VALUE,
    ]


def national_table():
    return [CENSUS_YEAR, PROFILE, TABLE_CODE, CELL_CODE, VALUE]


def auxiliary_info():
    def c(name, desc, obs=""):
        return col(name, "STRING", desc, obs=obs, original=name)

    return [
        col(
            "profile",
            "STRING",
            "Perfil do Censo (GCP ou TSP)",
            original="profile",
        ),
        col(
            "census_year",
            "INT64",
            "Ano do Censo (2011, 2016 ou 2021)",
            unit="year",
            directory=f"{TIME_DS}.ano:ano",
            original="census_year",
        ),
        c(
            "table_code",
            "Código da tabela do perfil (por exemplo G01, B01, T01)",
        ),
        c("table_name", "Nome da tabela do perfil"),
        c(
            "table_population",
            "População da tabela (Persons, Families, Dwellings)",
        ),
        c(
            "cell_code",
            "Código da célula (variável); chave de junção com as tabelas de dados",
        ),
        c(
            "long_description",
            "Descrição longa (codificada) da célula fornecida pela ABS",
        ),
        c("heading", "Rótulo da coluna da célula no perfil"),
        c(
            "datapack_part",
            "Arquivo CSV do DataPack em que a célula aparece (inclui partes A/B/C)",
        ),
        c(
            "statistic_type",
            "Tipo de estatística da célula (count, median ou average)",
        ),
        col(
            "measurement_unit",
            "STRING",
            "Unidade de medida da célula (por exemplo persons, dwellings, $/weekly, years)",
            original="measurement_unit",
        ),
    ]


def dicionario():
    def c(name, desc):
        return col(name, "STRING", desc, original=name)

    return [
        c("id_tabela", "Nome da tabela à qual a chave se aplica"),
        c("nome_coluna", "Nome da coluna à qual a chave se aplica"),
        c("chave", "Valor codificado armazenado na coluna"),
        c("cobertura_temporal", "Cobertura temporal da chave"),
        c("valor", "Rótulo correspondente à chave"),
    ]


def all_tables():
    t = {"national": national_table()}
    for level in LEVELS:
        t[level] = geo_table(level)
    t["auxiliary_info"] = auxiliary_info()
    return t


def main():
    tables = all_tables()
    for name, cols in tables.items():
        with open(os.path.join(HERE, f"{name}.csv"), "w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=HEADER, lineterminator="\n")
            w.writeheader()
            for cc in cols:
                w.writerow(cc)
    print(f"wrote {len(tables)} architecture CSVs to {HERE}")
    for name, cols in tables.items():
        print(f"  {name:36} {len(cols):2} cols: {[c['name'] for c in cols]}")


if __name__ == "__main__":
    main()
