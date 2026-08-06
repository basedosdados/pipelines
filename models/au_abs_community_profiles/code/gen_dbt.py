"""Generate dbt models (.sql) + schema.yml for au_abs_community_profiles from the
architecture CSVs. Long geo tables partition by census_year; the geo id column
gets year- AND profile-scoped `relationships` tests to the matching-vintage
br_bd_diretorios_au directory table (GCP -> native vintage; TSP -> 2021 always).

Run:  python gen_dbt.py
"""

import csv
import os

HERE = os.path.dirname(os.path.abspath(__file__))
ARCH = os.path.join(HERE, "architecture")
MODELS = os.path.dirname(HERE)
DATASET = "au_abs_community_profiles"
DIR_DS = "br_bd_diretorios_au"

INT_COLS = {"census_year"}
FLOAT_COLS = {"value"}

# geo table -> (id col, directory table stem). state is unstamped (no vintage).
GEO = {
    "state": ("id_state", "state"),
    "sa1": ("id_sa1", "sa1"),
    "sa2": ("id_sa2", "sa2"),
    "sa3": ("id_sa3", "sa3"),
    "sa4": ("id_sa4", "sa4"),
    "gccsa": ("id_gccsa", "gccsa"),
    "lga": ("id_lga", "lga"),
    "suburb": ("id_suburb", "suburb"),
    "postal_area": ("id_postal_area", "postal_area"),
    "commonwealth_electoral_division": (
        "id_commonwealth_electoral_division",
        "commonwealth_electoral_division",
    ),
    "state_electoral_division": (
        "id_state_electoral_division",
        "state_electoral_division",
    ),
}
TABLES = ["national", *GEO.keys(), "auxiliary_info"]
DATA_TABLES = ["national", *GEO.keys()]  # partitioned, census-based

DESC = {
    "national": "Perfis da comunidade do Censo australiano no nível nacional (Austrália), em formato longo.",
    "auxiliary_info": "Catálogo das células (variáveis) dos perfis: descrição, tabela, tipo de estatística e unidade de medida.",
    "dicionario": "Dicionário de valores codificados do conjunto (por exemplo o perfil GCP/TSP).",
}
GEO_DESC = {
    "state": "estados e territórios",
    "sa1": "Statistical Areas Level 1 (SA1)",
    "sa2": "Statistical Areas Level 2 (SA2)",
    "sa3": "Statistical Areas Level 3 (SA3)",
    "sa4": "Statistical Areas Level 4 (SA4)",
    "gccsa": "Greater Capital City Statistical Areas (GCCSA)",
    "lga": "áreas de governo local (LGA)",
    "suburb": "subúrbios e localidades",
    "postal_area": "áreas postais (POA)",
    "commonwealth_electoral_division": "divisões eleitorais federais (CED)",
    "state_electoral_division": "divisões eleitorais estaduais (SED)",
}


def read_arch(table):
    with open(os.path.join(ARCH, f"{table}.csv")) as f:
        return list(csv.DictReader(f))


def cast(c):
    t = (
        "int64"
        if c in INT_COLS
        else "float64"
        if c in FLOAT_COLS
        else "string"
    )
    return f"    safe_cast({c} as {t}) {c}"


def gen_sql(table):
    rows = read_arch(table)
    cfg = [
        "{{",
        "    config(",
        f'        alias="{table}",',
        f'        schema="{DATASET}",',
        '        materialized="table",',
    ]
    if table in DATA_TABLES:
        cfg += [
            "        partition_by={",
            '            "field": "census_year",',
            '            "data_type": "int64",',
            '            "range": {"start": 2011, "end": 2027, "interval": 1},',
            "        },",
        ]
    cfg += ["    )", "}}", "select"]
    casts = ",\n".join(cast(r["name"]) for r in rows)
    frm = f'from {{{{ set_datalake_project("{DATASET}_staging.{table}") }}}} as t'
    return "\n".join(cfg) + "\n" + casts + "\n" + frm + "\n"


def rel_block(to_model, field, where, name):
    # explicit name: two tests can target the same directory table (GCP-2021 and
    # TSP), and dbt names relationship tests by target only -> collide otherwise.
    # severity=warn: the census and the ASGS directory are independently versioned,
    # so redistricted units (esp. electoral divisions, e.g. 24 WA SEDs in 2016)
    # can drift between vintages. The FK is an advisory lineage link, not a hard
    # constraint; grain uniqueness/not-null stay error.
    return [
        "          - relationships:",
        f"              name: {name}",
        f"              to: ref('{to_model}')",
        f"              field: {field}",
        "              config:",
        f'                where: "{where}"',
        "                severity: warn",
    ]


def geo_id_tests(level, id_col):
    """Year- and profile-scoped relationships tests to the directory."""
    dir_stem = GEO[level][1]
    if level == "state":  # unstamped directory table, all rows
        return [
            "        tests:",
            "          - not_null",
            "          - relationships:",
            f"              to: ref('{DIR_DS}__state')",
            "              field: id_state",
            "              config:",
            "                severity: warn",
        ]
    out = ["        tests:", "          - not_null"]
    for yr in (2011, 2016, 2021):  # GCP native vintage
        out += rel_block(
            f"{DIR_DS}__{dir_stem}_{yr}",
            id_col,
            f"census_year = {yr} and profile = 'GCP'",
            f"rel_au_cp_{level}_gcp_{yr}",
        )
    # TSP always on 2021 boundaries
    out += rel_block(
        f"{DIR_DS}__{dir_stem}_2021",
        id_col,
        "profile = 'TSP'",
        f"rel_au_cp_{level}_tsp",
    )
    return out


def gen_schema_model(table):
    rows = read_arch(table)
    if table in GEO:
        id_col = GEO[table][0]
        grain = ["census_year", id_col, "profile", "table_code", "cell_code"]
        desc = f"Perfis da comunidade do Censo australiano no nível {GEO_DESC[table]}, em formato longo."
    elif table == "national":
        grain = ["census_year", "profile", "table_code", "cell_code"]
        desc = DESC["national"]
    elif table == "auxiliary_info":
        grain = ["profile", "census_year", "table_code", "cell_code"]
        desc = DESC["auxiliary_info"]
    else:  # dicionario
        grain = ["id_tabela", "nome_coluna", "chave"]
        desc = DESC["dicionario"]

    out = [
        f"  - name: {DATASET}__{table}",
        "    description: >",
        f"      {desc}",
        "    tests:",
        "      - dbt_utils.unique_combination_of_columns:",
        f"          combination_of_columns: {grain}",
    ]
    if table in DATA_TABLES:
        out += [
            "      - not_null_proportion_multiple_columns:",
            "          at_least: 0.05",
        ]
    out.append("    columns:")
    for r in rows:
        c = r["name"]
        out.append(f"      - name: {c}")
        out.append(f"        description: {r['description']}")
        if table in GEO and c == GEO[table][0]:
            out += geo_id_tests(table, c)
        elif c == "census_year" and table in DATA_TABLES:
            out.append("        tests: [not_null]")
    return "\n".join(out)


def main():
    for t in TABLES:
        with open(os.path.join(MODELS, f"{DATASET}__{t}.sql"), "w") as f:
            f.write(gen_sql(t))
    parts = ["version: 2", "", "models:"]
    for t in TABLES:
        parts.append(gen_schema_model(t))
    with open(os.path.join(MODELS, "schema.yml"), "w") as f:
        f.write("---\n" + "\n".join(parts) + "\n")
    print(f"wrote {len(TABLES)} models + schema.yml to {MODELS}")


if __name__ == "__main__":
    main()
