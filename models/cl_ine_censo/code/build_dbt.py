"""Generate the dbt models and schema.yml from the architecture CSVs.

The architecture table is the source of truth; regenerate rather than hand-editing
the SQL, so the two never drift.

Everything numeric below was MEASURED against the cleaned output, not guessed:
the primary keys, and the non-null proportions that decide `ignore_values`.
"""

from __future__ import annotations

import csv

from constants import ARCHITECTURE_DIR, CENSUS_YEAR, DATASET_ID

MODEL_DIR = ARCHITECTURE_DIR.parent.parent

# Measured 2026-09-23 against the cleaned parquet. Each is unique on the full
# table, with the distinct count equal to the row count.
PRIMARY_KEYS = {
    "persona": ["id_vivienda", "id_hogar", "id_persona"],
    "hogar": ["id_vivienda", "id_hogar"],
    "vivienda": ["id_vivienda"],
    "manzana_entidad": ["manzent"],
    # id_zona is null on the 9,736 localidad rows; the pair is still unique.
    "zona_localidad": ["id_zona", "id_localidad"],
    "dicionario": ["id_tabela", "nome_coluna", "chave"],
}

# Columns excluded from not_null_proportion_multiple_columns because they are
# sparse BY CONSTRUCTION, so the 5% floor tells us nothing about data quality.
# Measured non-null proportions are given.
IGNORE_NULL_PROPORTION = {
    "persona": [
        # Each asistencia_* flag is populated only for people attending that
        # specific level of education, so a low rate is the correct answer, not
        # a defect. Measured: media 0.054, parv 0.058, superior 0.095,
        # basica 0.105 - three of the four sit just above the 0.05 floor.
        "asistencia_parv",
        "asistencia_basica",
        "asistencia_media",
        "asistencia_superior",
        # Asked only of people who arrived in Chile from abroad. Measured 0.087.
        "p26_llegada_periodo",
        # Asked only of people who self-identified as indigenous. Measured 0.114.
        "p28_pueblo_pert",
    ],
    # Populated only for dwellings holding more than one household. Measured 0.020.
    "vivienda": ["p11c_num_hogar"],
    "hogar": [],
    # geometria is never null, but it is the heaviest column in the dataset
    # (~440 MB of WKT); scanning it to confirm 0% null buys nothing.
    "manzana_entidad": ["geometria"],
    "zona_localidad": ["geometria"],
}

DIRECTORY_TESTS = {
    "id_region": ("br_bd_diretorios_cl__region", "id_region"),
    "id_provincia": ("br_bd_diretorios_cl__provincia", "id_provincia"),
    "id_comuna": ("br_bd_diretorios_cl__comuna", "id_comuna"),
}

TABLE_DESCRIPTIONS = {
    "persona": (
        "Microdatos de personas del Censo de Poblacion y Vivienda 2024 de Chile, "
        "con una fila por persona censada. El maximo nivel de desagregacion "
        "geografica es la comuna: el INE no publica la manzana ni la zona censal "
        "de cada persona por control de divulgacion estadistica, de modo que "
        "esta tabla NO se puede cruzar con manzana_entidad ni zona_localidad."
    ),
    "hogar": (
        "Microdatos de hogares del Censo de Poblacion y Vivienda 2024 de Chile, "
        "con una fila por hogar censado. Se vincula con vivienda por id_vivienda "
        "y con persona por id_vivienda e id_hogar."
    ),
    "vivienda": (
        "Microdatos de viviendas del Censo de Poblacion y Vivienda 2024 de Chile, "
        "con una fila por vivienda censada. El total publicado por el INE "
        "(7.642.716) es menor que el numero de filas porque excluye los "
        "operativos de vivienda colectiva, que aqui aparecen como registros con "
        "las preguntas de vivienda en nulo."
    ),
    "manzana_entidad": (
        "Base manzana-entidad del Censo 2024 de Chile: 189 variables agregadas de "
        "poblacion, hogares y viviendas por manzana urbana o entidad rural, con "
        "la geometria del poligono. Union de las capas cartograficas Manzanas y "
        "Entidades publicadas por el INE; la columna nivel_geografico indica el "
        "origen de cada fila."
    ),
    "zona_localidad": (
        "Base zona-localidad del Censo 2024 de Chile: 189 variables agregadas de "
        "poblacion, hogares y viviendas por zona censal urbana o localidad rural, "
        "con la geometria del poligono. Union de las capas cartograficas Zonal y "
        "Localidades publicadas por el INE; la columna nivel_geografico indica el "
        "origen de cada fila."
    ),
    "dicionario": (
        "Diccionario de las columnas codificadas del Censo 2024 de Chile, con una "
        "fila por combinacion de tabla, columna y codigo. Incluye los codigos "
        "centinela -99 (no respuesta) y -66 (valor suprimido por anonimizacion). "
        "Las etiquetas provienen del diccionario Redatam oficial CPV2024.dicX."
    ),
}


def read_architecture(table: str) -> list[dict[str, str]]:
    with (ARCHITECTURE_DIR / f"{table}.csv").open(encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def cast_expression(column: dict[str, str]) -> str:
    name = column["name"]
    kind = column["bigquery_type"]

    if name == "geometria":
        # safe. prefix so an unparseable polygon yields NULL instead of failing
        # the whole model; make_valid repairs self-intersections, which INE's
        # block polygons occasionally carry.
        return f"safe.st_geogfromtext({name}, make_valid => true) {name}"

    if name in ("ano", "id_region"):
        # Both are hive partition keys, so their type is whatever BigQuery infers
        # from the directory names. ano is cast outright; id_region is re-padded
        # because an INTEGER inference would silently drop the leading zero and
        # break the join to br_bd_diretorios_cl.
        if name == "ano":
            return f"safe_cast({name} as int64) {name}"
        return f"lpad(safe_cast({name} as string), 2, '0') {name}"

    return f"safe_cast({name} as {kind.lower()}) {name}"


def build_sql(table: str) -> str:
    columns = read_architecture(table)
    selects = ",\n    ".join(cast_expression(c) for c in columns)

    config_lines = [
        f'        schema="{DATASET_ID}",',
        f'        alias="{table}",',
        '        materialized="table",',
    ]
    if table != "dicionario":
        config_lines += [
            "        partition_by={",
            '            "field": "ano",',
            '            "data_type": "int64",',
            f'            "range": {{"start": {CENSUS_YEAR}, '
            f'"end": {CENSUS_YEAR + 5}, "interval": 1}},',
            "        },",
            '        cluster_by=["id_comuna"],',
        ]
    config = "\n".join(config_lines)

    return (
        "{{\n    config(\n"
        f"{config}\n"
        "    )\n}}\n\n\n"
        f"select\n    {selects}\n"
        f'from {{{{ set_datalake_project("{DATASET_ID}_staging.{table}") }}}} as t\n'
    )


def yaml_block(text: str, indent: int) -> str:
    """Render a description as a folded block scalar.

    Always `>` per house rule: a bare scalar breaks on any description
    containing a colon, and several of these do.
    """
    pad = " " * indent
    words, lines, current = text.split(), [], ""
    for word in words:
        if len(current) + len(word) + 1 > 72:
            lines.append(current)
            current = word
        else:
            current = f"{current} {word}".strip()
    if current:
        lines.append(current)
    body = "\n".join(f"{pad}{line}" for line in lines)
    return f">\n{body}"


def build_schema() -> str:
    out = ["---", "version: 2", "models:"]

    for table in (
        "persona",
        "hogar",
        "vivienda",
        "manzana_entidad",
        "zona_localidad",
        "dicionario",
    ):
        columns = read_architecture(table)
        names = [c["name"] for c in columns]
        coded = [
            c["name"] for c in columns if c["covered_by_dictionary"] == "yes"
        ]

        out.append(f"  - name: {DATASET_ID}__{table}")
        out.append(
            f"    description: {yaml_block(TABLE_DESCRIPTIONS[table], 6)}"
        )
        out.append("    tests:")
        out.append("      - dbt_utils.unique_combination_of_columns:")
        out.append(
            "          combination_of_columns: "
            f"[{', '.join(PRIMARY_KEYS[table])}]"
        )
        out.append("      - not_null_proportion_multiple_columns:")
        out.append("          at_least: 0.05")
        ignored = IGNORE_NULL_PROPORTION.get(table, [])
        if ignored:
            out.append("          ignore_values:")
            out += [f"            - {c}" for c in ignored]
        if coded:
            out.append("      - custom_dictionary_coverage:")
            out.append(
                f"          dictionary_model: ref('{DATASET_ID}__dicionario')"
            )
            out.append("          columns_covered_by_dictionary:")
            out += [f"            - {c}" for c in coded]

        out.append("    columns:")
        for column in columns:
            name = column["name"]
            out.append(f"      - name: {name}")
            out.append(
                f"        description: {yaml_block(column['description'], 10)}"
            )
            tests = []
            if name in PRIMARY_KEYS[table] or name in ("ano", "id_comuna"):
                tests.append("not_null")
            if name in DIRECTORY_TESTS and table != "dicionario":
                model, field = DIRECTORY_TESTS[name]
                out.append("        tests:")
                if tests:
                    out.append(f"          - {tests[0]}")
                out.append("          - relationships:")
                out.append(f"              to: ref('{model}')")
                out.append(f"              field: {field}")
                continue
            if tests:
                out.append(f"        tests: [{', '.join(tests)}]")

        # A column listed in both is a bug; catch it here rather than in CI.
        assert len(names) == len(set(names)), f"duplicate column in {table}"

    return "\n".join(out) + "\n"


def main() -> None:
    for table in (
        "persona",
        "hogar",
        "vivienda",
        "manzana_entidad",
        "zona_localidad",
        "dicionario",
    ):
        path = MODEL_DIR / f"{DATASET_ID}__{table}.sql"
        path.write_text(build_sql(table), encoding="utf-8")
        print(f"  {path.name}")

    schema = MODEL_DIR / "schema.yml"
    schema.write_text(build_schema(), encoding="utf-8")
    print(f"  {schema.name}")


if __name__ == "__main__":
    main()
