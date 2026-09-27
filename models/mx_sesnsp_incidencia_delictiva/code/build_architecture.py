#!/usr/bin/env python3
"""Generate architecture CSVs for both datasets in this onboarding:

  - br_bd_diretorios_mx        (2 tables: estado, municipio) — INEGI AGEEML
  - mx_sesnsp_incidencia_delictiva (7 tables) — SESNSP incidencia delictiva

The architecture CSV is the single source of truth for column order, types,
descriptions, directory FKs, and metadata. Cleaning code and metadata registration
both read these. Spanish column names + Spanish descriptions (data language);
EN/PT translations are produced at metadata registration.

Columns (data-basis-style.md order):
  name,bigquery_type,description,temporal_coverage,covered_by_dictionary,
  directory_column,measurement_unit,has_sensitive_data,observations,original_name

Usage:
    uv run python models/mx_sesnsp_incidencia_delictiva/code/build_architecture.py
"""

import csv
from pathlib import Path

REPO = Path(__file__).resolve().parents[3]
DIR_ARCH = REPO / "models" / "br_bd_diretorios_mx" / "code" / "architecture"
SES_ARCH = (
    REPO
    / "models"
    / "mx_sesnsp_incidencia_delictiva"
    / "code"
    / "architecture"
)

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


# convenience: build a row dict with sensible defaults
def col(
    name,
    bqtype,
    description,
    original_name="",
    directory_column="",
    covered_by_dictionary="no",
    measurement_unit="",
    temporal_coverage="",
    has_sensitive_data="no",
    observations="",
):
    return {
        "name": name,
        "bigquery_type": bqtype,
        "description": description,
        "temporal_coverage": temporal_coverage,
        "covered_by_dictionary": covered_by_dictionary,
        "directory_column": directory_column,
        "measurement_unit": measurement_unit,
        "has_sensitive_data": has_sensitive_data,
        "observations": observations,
        "original_name": original_name,
    }


def write(path, rows):
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=HEADER)
        w.writeheader()
        for r in rows:
            w.writerow(r)
    print(f"wrote {path.relative_to(REPO)}  ({len(rows)} cols)")


# ---------------------------------------------------------------- directory
def build_directory():
    estado = [
        col(
            "id_estado",
            "STRING",
            "Clave geoestadística de la entidad federativa según INEGI, de dos dígitos",
            original_name="cve_ent",
            observations="Llave primaria. Clave AGEE del Marco Geoestadístico del INEGI",
        ),
        col(
            "nombre",
            "STRING",
            "Nombre de la entidad federativa",
            original_name="nomgeo",
        ),
        col(
            "abreviatura",
            "STRING",
            "Abreviatura oficial de la entidad federativa",
            original_name="nom_abrev",
        ),
    ]
    municipio = [
        col(
            "id_municipio",
            "STRING",
            "Clave geoestadística del municipio según INEGI, de cinco dígitos",
            original_name="cvegeo",
            observations="Llave primaria. Concatena la clave de entidad (2) y la clave de "
            "municipio (3). Catálogo íntegro del INEGI; los códigos agregados no "
            "georreferenciados del SESNSP (municipio 998/999) se excluyen de las "
            "pruebas de integridad referencial, no se agregan aquí",
        ),
        col(
            "id_estado",
            "STRING",
            "Clave de la entidad federativa a la que pertenece el municipio",
            original_name="cve_ent",
            directory_column="br_bd_diretorios_mx.estado:id_estado",
        ),
        col(
            "nombre", "STRING", "Nombre del municipio", original_name="nomgeo"
        ),
    ]
    write(DIR_ARCH / "estado.csv", estado)
    write(DIR_ARCH / "municipio.csv", municipio)


# ---------------------------------------------------------------- sesnsp
def sesnsp_cols(muni, victimas, coverage):
    """Assemble the long-format column list for a SESNSP crime table."""
    rows = []
    # temporal partitions
    rows.append(
        col(
            "ano",
            "INT64",
            "Año de referencia del registro",
            temporal_coverage=coverage,
            directory_column="br_bd_diretorios_data_tempo.ano:ano",
            original_name="Año",
            observations="Columna de partición",
        )
    )
    rows.append(
        col(
            "mes",
            "INT64",
            "Mes de referencia del registro, de 1 a 12",
            directory_column="br_bd_diretorios_data_tempo.mes:mes",
            original_name="Enero..Diciembre",
            observations="Derivada de las columnas mensuales de la base ancha",
        )
    )
    # geography
    rows.append(
        col(
            "id_entidad",
            "STRING",
            "Clave de la entidad federativa según INEGI, de dos dígitos",
            directory_column="br_bd_diretorios_mx.estado:id_estado",
            original_name="Clave_Ent",
        )
    )
    if muni:
        rows.append(
            col(
                "id_municipio",
                "STRING",
                "Clave del municipio según INEGI, de cinco dígitos",
                directory_column="br_bd_diretorios_mx.municipio:id_municipio",
                original_name="Cve. Municipio",
                observations="Derivada como Cve. Municipio rellenada a cinco dígitos "
                "con ceros a la izquierda",
            )
        )
    # crime classification (readable labels, not codes → covered_by_dictionary=no)
    rows.append(
        col(
            "bien_juridico_afectado",
            "STRING",
            "Bien jurídico afectado por el delito según la clasificación del SESNSP",
            original_name="Bien jurídico afectado",
        )
    )
    rows.append(
        col(
            "tipo_delito",
            "STRING",
            "Tipo de delito según la clasificación del SESNSP",
            original_name="Tipo de delito",
        )
    )
    rows.append(
        col(
            "subtipo_delito",
            "STRING",
            "Subtipo de delito según la clasificación del SESNSP",
            original_name="Subtipo de delito",
        )
    )
    rows.append(
        col(
            "modalidad",
            "STRING",
            "Modalidad del delito según la clasificación del SESNSP",
            original_name="Modalidad",
        )
    )
    if victimas:
        rows.append(
            col("sexo", "STRING", "Sexo de la víctima", original_name="Sexo")
        )
        rows.append(
            col(
                "rango_edad",
                "STRING",
                "Rango de edad de la víctima",
                original_name="Rango de edad",
            )
        )
    # measure
    unit = "víctima" if victimas else "delito"
    what = "víctimas registradas" if victimas else "delitos registrados"
    rows.append(
        col(
            "cantidad",
            "INT64",
            f"Cantidad de {what} en el mes para la combinación de "
            "clasificación y geografía",
            measurement_unit=unit,
            original_name="(valor de la columna mensual)",
            observations="Se conservan los ceros reportados; se descartan los meses "
            "aún sin publicar (celdas vacías)",
        )
    )
    return rows


SESNSP_TABLES = {
    # table_slug: (muni, victimas, coverage)
    "municipio_delitos_2015_2025": (True, False, "2015(1)2025"),
    "municipio_delitos": (True, False, "2026(1)"),
    "municipio_victimas": (True, True, "2026(1)"),
    "estatal_delitos_2015_2025": (False, False, "2015(1)2025"),
    "estatal_delitos": (False, False, "2026(1)"),
    "estatal_victimas_2015_2025": (False, True, "2015(1)2025"),
    "estatal_victimas": (False, True, "2026(1)"),
}


def build_sesnsp():
    for slug, (muni, victimas, coverage) in SESNSP_TABLES.items():
        write(SES_ARCH / f"{slug}.csv", sesnsp_cols(muni, victimas, coverage))


if __name__ == "__main__":
    build_directory()
    build_sesnsp()
    print("done")
