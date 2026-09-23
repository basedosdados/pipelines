"""Generate the architecture CSVs from INE's own dictionaries.

One CSV per table in ``architecture/``. The architecture table is the source of
truth for column names, types and descriptions; the dbt models and the backend
metadata are both generated from it.

Typing follows the house rule "type by arithmetic meaning": INT64/FLOAT64 only
where summing or averaging the values means something and the result has a
nameable unit. Everything else - codes, flags, identifiers, territorial codes -
is STRING, with ``covered_by_dictionary = yes`` where a label set exists.

Run after clean.py, since the column lists are read from the cleaned output.
"""

from __future__ import annotations

import csv

import openpyxl  # type: ignore[import-untyped]
import pyarrow.dataset as pads
from constants import (
    AGGREGATE_PREFIXES,
    ARCHITECTURE_DIR,
    INPUT_DIR,
    OUTPUT_DIR,
)
from dictionary import _clean, load_redatam_dictionary

FIELDS = [
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

MICRODATA_TABLES = ("persona", "hogar", "vivienda")
CARTOGRAPHY_TABLES = ("manzana_entidad", "zona_localidad")

DIRECTORY = {
    "id_region": "br_bd_diretorios_cl.region:id_region",
    "id_provincia": "br_bd_diretorios_cl.provincia:id_provincia",
    "id_comuna": "br_bd_diretorios_cl.comuna:id_comuna",
}

SENTINEL_NOTE = (
    "Los codigos centinela -99 (no respuesta) y -66 (valor suprimido por "
    "anonimizacion) fueron convertidos a NULL, porque en una columna numerica "
    "contaminarian cualquier promedio o suma"
)

# Genuine quantities, with the unit each one is measured in. Everything absent
# from this map is a code, a flag or an identifier and is therefore STRING.
QUANTITY_UNITS = {
    "edad": "year",
    "escolaridad": "year",
    "p46a_tot_hijs_nac": "person",
    "p46b_hijas_nac": "person",
    "p46c_hijos_nac": "person",
    "p47a_tot_hijs_sobrev": "person",
    "p47b_hijas_sobrev": "person",
    "p47c_hijos_sobrev": "person",
    "p48_anio_nac_uh": "year",
    "p48_mes_nac_uh": "month",
    "cant_hog": "household",
    "cant_per": "person",
    "p5_num_dormitorios": "room",
    "p11a_num_personas": "person",
    "p11c_num_hogar": "household",
}

# Quantities whose top category is an open band, so a mean is slightly biased
# downward. Documented rather than silently shipped.
TOP_CODED = {
    "edad": "Topecodificada: el valor 85 significa '85 anos o mas'",
    "p5_num_dormitorios": "Topecodificada: el valor 6 significa '6 o mas'",
}

FIXED_DESCRIPTIONS = {
    "ano": "Ano del levantamiento censal",
    "id_region": "Codigo unico territorial (CUT) de la region, de dos digitos",
    "id_provincia": "Codigo unico territorial (CUT) de la provincia, de tres digitos",
    "id_comuna": "Codigo unico territorial (CUT) de la comuna, de cinco digitos",
    "id_vivienda": "Identificador de la vivienda, unico en todo el pais",
    "id_hogar": "Identificador del hogar dentro de la vivienda",
    "id_persona": "Identificador de la persona dentro del hogar",
    "nivel_geografico": (
        "Nivel geografico del registro, que indica de cual de las dos capas "
        "cartograficas de INE proviene la fila"
    ),
    "geometria": "Poligono del area censal",
    "nombre_region": "Nombre de la region",
    "nombre_provincia": "Nombre de la provincia",
    "nombre_comuna": "Nombre de la comuna",
    "nombre_distrito": "Nombre del distrito censal",
    "nombre_localidad": "Nombre de la localidad",
    "nombre_entidad": "Nombre de la entidad poblada",
}

# INE's own variable labels are the questionnaire wording, which is precise and
# worth keeping for most columns. These few are too terse to stand alone once
# separated from the questionnaire ("Area", "Tablet", "A cual?"), so they get a
# description that says what the column actually holds. Every one was written
# against the observed value set, not guessed.
TERSE_OVERRIDES = {
    "area": "Estrato censal urbano o rural en que se ubica el registro",
    "categoria": (
        "Categoria de la entidad poblada segun INE: Ciudad, Pueblo, Aldea, "
        "Caserio, Parcela-Hijuela, Comunidad Indigena, entre otras"
    ),
    "mz_base_censo": (
        "Indica si el poligono forma parte de la cartografia base del censo "
        "(1) o fue incorporado como area complementaria (0)"
    ),
    "tipo_mz": (
        "Tipo de manzana censal: URBANO para manzanas urbanas y ALDEA para "
        "manzanas de aldea. Nulo en los registros de entidad rural"
    ),
    "area_c": "Estrato censal urbano o rural del area cartografica",
    "tipologia_hogar": (
        "Tipologia del hogar segun su composicion: unipersonal, nuclear, "
        "extenso, compuesto o sin nucleo"
    ),
    "discapacidad": (
        "Condicion de discapacidad, derivada del conjunto de preguntas de "
        "dificultad funcional p32a a p32f"
    ),
    "p40_cise_rec": (
        "Categoria ocupacional en el empleo (CISE recodificada): independiente, "
        "dependiente o trabajador no remunerado"
    ),
    "p25_lug_nacimiento_rec": "Indica si la persona nacio en Chile o en el extranjero",
    "p28_pueblo_pert": (
        "Pueblo originario al que declara pertenecer la persona que se "
        "autoidentifico como indigena en p28_autoid_pueblo"
    ),
    "p15a_serv_tel_movil": "Dispone de telefono movil en el hogar",
    "p15b_serv_compu": "Dispone de computador en el hogar",
    "p15c_serv_tablet": "Dispone de tablet en el hogar",
    "p15d_serv_internet_fija": "Dispone de conexion a internet fija en el hogar",
    "p15e_serv_internet_movil": "Dispone de conexion a internet movil en el hogar",
    "p15f_serv_internet_satelital": "Dispone de conexion a internet satelital en el hogar",
    "n_per": "Personas censadas en el area",
    "n_hombres": "Hombres censados en el area",
    "n_mujeres": "Mujeres censadas en el area",
    "n_hog": "Hogares censados en el area",
}

FIXED_OBSERVATIONS = {
    "ano": (
        "Columna de particion. El Censo de Poblacion y Vivienda es decenal, por "
        "lo que la tabla contiene un unico ano"
    ),
    "id_region": "Columna de particion. Derivable de los dos primeros digitos de id_comuna",
    "id_provincia": "Derivable de los tres primeros digitos de id_comuna",
    "id_vivienda": (
        "Llave primaria de la tabla vivienda. Verificado unico en todo el pais: "
        "no requiere id_comuna para desambiguar"
    ),
    "geometria": (
        "Sistema de referencia de origen SIRGAS 2000 (EPSG:4674). No se "
        "reproyecto a WGS 84 (EPSG:4326) porque la diferencia en Chile es "
        "inferior a un metro. Use ST_AREA para obtener superficie en metros "
        "cuadrados"
    ),
}


def load_aggregate_descriptions() -> dict[str, str]:
    """Descriptions of the 189 aggregate variables, from INE's own dictionary."""
    workbook = openpyxl.load_workbook(
        INPUT_DIR / "diccionario.xlsx", read_only=True, data_only=True
    )
    sheet = workbook["Dicionario"]
    descriptions: dict[str, str] = {}
    for index, row in enumerate(sheet.iter_rows(values_only=True)):
        if index == 0 or row[1] is None:
            continue
        name = _clean(str(row[1])).lower()
        description = _clean(str(row[3])) if row[3] is not None else ""
        universe = (
            _clean(str(row[4])) if len(row) > 4 and row[4] is not None else ""
        )
        descriptions[name] = (description, universe)
    return descriptions


def table_columns(table: str) -> list[str]:
    dataset = pads.dataset(
        OUTPUT_DIR / table, format="parquet", partitioning="hive"
    )
    names = list(dataset.schema.names)
    # Partition columns come back last; restore the logical lead order.
    lead = [c for c in ("ano", "id_region") if c in names]
    return lead + [c for c in names if c not in lead]


def row(**kwargs) -> dict[str, str]:
    base = {field: "" for field in FIELDS}
    base["covered_by_dictionary"] = "no"
    base["has_sensitive_data"] = "no"
    base.update(kwargs)
    return base


def microdata_rows(table: str, variables) -> list[dict[str, str]]:
    rows = []
    for column in table_columns(table):
        variable = variables.get(column)
        original = "" if column in ("ano",) else column
        if column in ("id_region", "id_provincia", "id_comuna"):
            original = {
                "id_region": "region",
                "id_provincia": "provincia",
                "id_comuna": "comuna",
            }[column]

        if column in QUANTITY_UNITS:
            observations = SENTINEL_NOTE
            if column in TOP_CODED:
                observations = f"{TOP_CODED[column]}. {observations}"
            rows.append(
                row(
                    name=column,
                    bigquery_type="INT64",
                    description=_describe(column, variable),
                    measurement_unit=QUANTITY_UNITS[column],
                    observations=observations,
                    original_name=original,
                )
            )
            continue

        coded = variable is not None and variable.is_coded and column != "ano"
        rows.append(
            row(
                name=column,
                bigquery_type="INT64" if column == "ano" else "STRING",
                description=_describe(column, variable),
                covered_by_dictionary="yes" if coded else "no",
                directory_column=DIRECTORY.get(column, ""),
                measurement_unit="year" if column == "ano" else "",
                observations=FIXED_OBSERVATIONS.get(column, ""),
                original_name=original,
            )
        )
    return rows


def tidy(text: str) -> str:
    """Capitalise the first letter and strip the trailing full stop.

    House rule: column descriptions start with a capital and do NOT end with a
    period. INE's labels violate both here and there.
    """
    text = text.strip().rstrip(".").strip()
    if text and text[0].isalpha():
        text = text[0].upper() + text[1:]
    return text


def _describe(column: str, variable) -> str:
    if column in TERSE_OVERRIDES:
        return tidy(TERSE_OVERRIDES[column])
    if column in FIXED_DESCRIPTIONS:
        return tidy(FIXED_DESCRIPTIONS[column])
    if variable is not None and variable.description:
        return tidy(variable.description)
    return ""


def cartography_rows(table: str, aggregates: dict) -> list[dict[str, str]]:
    rows = []
    for column in table_columns(table):
        description, universe = aggregates.get(column, ("", ""))
        description = tidy(
            TERSE_OVERRIDES.get(column)
            or FIXED_DESCRIPTIONS.get(column)
            or description
            or column
        )

        if column.startswith(AGGREGATE_PREFIXES):
            rows.append(
                row(
                    name=column,
                    bigquery_type="FLOAT64"
                    if column.startswith("prom_")
                    else "INT64",
                    description=description,
                    measurement_unit=_aggregate_unit(column),
                    observations=f"Universo: {tidy(universe)}"
                    if universe
                    else "",
                    original_name=column,
                )
            )
            continue

        rows.append(
            row(
                name=column,
                bigquery_type="INT64" if column == "ano" else "STRING",
                description=description,
                directory_column=DIRECTORY.get(column, ""),
                measurement_unit="year" if column == "ano" else "",
                observations=FIXED_OBSERVATIONS.get(column, ""),
                original_name=""
                if column in ("ano", "nivel_geografico")
                else column,
            )
        )
    return rows


def _aggregate_unit(column: str) -> str:
    if column == "prom_edad":
        return "year"
    if column == "prom_escolaridad18":
        return "year"
    if column == "prom_per_hog":
        return "person"
    if column.startswith("n_hog"):
        return "household"
    if column.startswith(
        ("n_vp", "n_viv", "n_tipo_viv", "n_mat_", "n_dormitorios")
    ):
        return "dwelling"
    return "person"


def dictionary_table_rows() -> list[dict[str, str]]:
    return [
        row(
            name="id_tabela",
            bigquery_type="STRING",
            description="Nombre de la tabla a la que pertenece la columna codificada",
        ),
        row(
            name="nome_coluna",
            bigquery_type="STRING",
            description="Nombre de la columna codificada",
        ),
        row(
            name="chave",
            bigquery_type="STRING",
            description="Codigo almacenado en la columna",
        ),
        row(
            name="cobertura_temporal",
            bigquery_type="STRING",
            description="Cobertura temporal de la correspondencia entre codigo y etiqueta",
        ),
        row(
            name="valor",
            bigquery_type="STRING",
            description="Etiqueta correspondiente al codigo",
        ),
    ]


def write_csv(table: str, rows: list[dict[str, str]]) -> None:
    ARCHITECTURE_DIR.mkdir(parents=True, exist_ok=True)
    path = ARCHITECTURE_DIR / f"{table}.csv"
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=FIELDS)
        writer.writeheader()
        writer.writerows(rows)
    print(f"  {table:18s} {len(rows):>4d} columns -> {path.name}")


def main() -> None:
    variables = load_redatam_dictionary()
    aggregates = load_aggregate_descriptions()

    print("architecture:")
    for table in MICRODATA_TABLES:
        write_csv(table, microdata_rows(table, variables))
    for table in CARTOGRAPHY_TABLES:
        write_csv(table, cartography_rows(table, aggregates))
    write_csv("dicionario", dictionary_table_rows())


if __name__ == "__main__":
    main()
