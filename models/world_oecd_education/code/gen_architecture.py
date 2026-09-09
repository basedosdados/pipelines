"""Write the architecture CSVs for world_oecd_education from the cached DSDs.

The architecture CSVs under ``architecture/`` are the single source of truth for
column names, order, types and the SDMX -> clean name mapping. ``clean.py`` and
``gen_dbt.py`` read them; nothing else declares a schema.

Each table's columns are, in order:

1. ``year`` — the partition column. Taken from ``TIME_PERIOD`` where the cube has
   one, and otherwise parsed out of the ``REF_PERIOD`` attribute, so that every
   table partitions the same way even though six cubes carry no time dimension.
2. ``country_iso3_code`` — derived. ``REF_AREA`` mixes countries, subnational
   entities and aggregates like ``OECD``, so it cannot itself be a foreign key;
   this column is filled only where the area is a real ISO-3166 alpha-3 country
   and is what links to ``br_bd_diretorios_mundo.pais``.
3. the DSD's dimensions, in SDMX order, all STRING and dictionary-covered.
4. ``obs_value``.
5. the DSD's attributes.
6. ``source_flow`` / ``source_flow_version`` — derived provenance, which matter
   because some tables union more than one flow version.

Run: ``python gen_architecture.py``
"""

import csv
import xml.etree.ElementTree as ET

from common import ARCH_DIR, STRUCTURE
from concepts import CONCEPTS
from tables import TABLES

S = "{http://www.sdmx.org/resources/sdmxml/schemas/v2_1/structure}"

HEADER = [
    "name",
    "bigquery_type",
    "description_pt",
    "description_en",
    "description_es",
    "temporal_coverage",
    "covered_by_dictionary",
    "directory_column",
    "measurement_unit",
    "has_sensitive_data",
    "observations_pt",
    "observations_en",
    "observations_es",
    "original_name",
]

# Columns Data Basis derives rather than reads straight from the source.
DERIVED = {
    "year": dict(
        bigquery_type="INT64",
        directory_column="br_bd_diretorios_data_tempo.ano:ano",
        measurement_unit="year",
        description_pt="Ano de referência da observação",
        description_en="Reference year of the observation",
        description_es="Año de referencia de la observación",
        observations_pt=(
            "Coluna de particionamento. Nos cubos com dimensão temporal vem de TIME_PERIOD; "
            "nos cubos sem dimensão temporal é extraída do atributo REF_PERIOD"
        ),
        observations_en=(
            "Partition column. In cubes with a time dimension it comes from TIME_PERIOD; in "
            "cubes without one it is parsed from the REF_PERIOD attribute"
        ),
        observations_es=(
            "Columna de particionamiento. En los cubos con dimensión temporal proviene de "
            "TIME_PERIOD; en los cubos sin ella se extrae del atributo REF_PERIOD"
        ),
    ),
    "country_iso3_code": dict(
        bigquery_type="STRING",
        directory_column="br_bd_diretorios_mundo.pais:id_pais",
        description_pt="Código ISO 3166-1 alfa-3 do país da observação",
        description_en="ISO 3166-1 alpha-3 code of the observation's country",
        description_es="Código ISO 3166-1 alfa-3 del país de la observación",
        observations_pt=(
            "Derivada de reference_area. Vazia quando a área não é um país, como nas entidades "
            "subnacionais e nos agregados do tipo OECD ou EU25"
        ),
        observations_en=(
            "Derived from reference_area. Empty when the area is not a country, as for "
            "subnational entities and aggregates such as OECD or EU25"
        ),
        observations_es=(
            "Derivada de reference_area. Vacía cuando el área no es un país, como en las "
            "entidades subnacionales y los agregados del tipo OECD o EU25"
        ),
    ),
    "source_flow": dict(
        bigquery_type="STRING",
        description_pt="Dataflow SDMX de onde a linha foi extraída",
        description_en="SDMX dataflow the row was extracted from",
        description_es="Dataflow SDMX del que se extrajo la fila",
        observations_pt=(
            "Registrada porque algumas tabelas unem mais de uma versão do dataflow, e a versão "
            "identifica a edição do Education at a Glance"
        ),
        observations_en=(
            "Recorded because some tables union more than one dataflow version, and the version "
            "identifies the Education at a Glance edition"
        ),
        observations_es=(
            "Registrada porque algunas tablas unen más de una versión del dataflow, y la versión "
            "identifica la edición de Education at a Glance"
        ),
    ),
    "source_flow_version": dict(
        bigquery_type="STRING",
        description_pt="Versão do dataflow SDMX de onde a linha foi extraída",
        description_en="Version of the SDMX dataflow the row was extracted from",
        description_es="Versión del dataflow SDMX del que se extrajo la fila",
        observations_pt=(
            "A OCDE mantém versões antigas publicadas. Nos cubos sem dimensão temporal, versões "
            "sucessivas cobrem anos de referência distintos"
        ),
        observations_en=(
            "The OECD keeps older versions published. In cubes with no time dimension, successive "
            "versions cover different reference years"
        ),
        observations_es=(
            "La OCDE mantiene publicadas las versiones antiguas. En los cubos sin dimensión "
            "temporal, versiones sucesivas cubren años de referencia distintos"
        ),
    ),
}


def components(dsd_id, version):
    """(dimensions, has_time, attributes) of a DSD, in SDMX order, as component ids."""
    path = STRUCTURE / f"dsd_{dsd_id}_{version}.xml"
    root = ET.parse(path).getroot()
    dsd = next(root.iter(f"{S}DataStructure"), None)
    if dsd is None:
        raise ValueError(f"no DataStructure in {path}")
    dim_list = dsd.find(f"{S}DataStructureComponents/{S}DimensionList")
    if dim_list is None:
        raise ValueError(f"no DimensionList in {path}")
    attr_list = dsd.find(f"{S}DataStructureComponents/{S}AttributeList")
    dims = sorted(
        (int(d.get("position") or 0), d.get("id") or "")
        for d in dim_list.iter(f"{S}Dimension")
        if d.get("id")
    )
    attrs = sorted(
        a.get("id") or ""
        for a in (
            attr_list.iter(f"{S}Attribute") if attr_list is not None else ()
        )
        if a.get("id")
    )
    has_time = any(td.get("id") for td in dim_list.iter(f"{S}TimeDimension"))
    return [d for _, d in dims], has_time, attrs


def row(name, spec, original=""):
    """One architecture CSV row from a DERIVED spec."""
    return {
        "name": name,
        "bigquery_type": spec["bigquery_type"],
        "description_pt": spec["description_pt"],
        "description_en": spec["description_en"],
        "description_es": spec["description_es"],
        "temporal_coverage": "",
        "covered_by_dictionary": spec.get("covered_by_dictionary", "no"),
        "directory_column": spec.get("directory_column", ""),
        "measurement_unit": spec.get("measurement_unit", ""),
        "has_sensitive_data": "no",
        "observations_pt": spec.get("observations_pt", ""),
        "observations_en": spec.get("observations_en", ""),
        "observations_es": spec.get("observations_es", ""),
        "original_name": original,
    }


def concept_row(component):
    """One architecture CSV row from an SDMX component id."""
    col, bq_type, in_dict, pt, en, es = CONCEPTS[component]
    obs = ("", "", "")
    if col == "obs_value":
        obs = (
            "A unidade varia por linha e está registrada em unit_measure; o valor deve ser "
            "multiplicado por dez elevado a unit_multiplier",
            "The unit varies by row and is recorded in unit_measure; the value must be "
            "multiplied by ten to the power of unit_multiplier",
            "La unidad varía por fila y se registra en unit_measure; el valor debe multiplicarse "
            "por diez elevado a unit_multiplier",
        )
    return {
        "name": col,
        "bigquery_type": bq_type,
        "description_pt": pt,
        "description_en": en,
        "description_es": es,
        "temporal_coverage": "",
        "covered_by_dictionary": "yes" if in_dict else "no",
        "directory_column": "",
        "measurement_unit": "",
        "has_sensitive_data": "no",
        "observations_pt": obs[0],
        "observations_en": obs[1],
        "observations_es": obs[2],
        "original_name": component,
    }


def build(slug, spec):
    dims, has_time, attrs = components(spec["dsd"], spec["dsd_version"])
    # Six cubes carry no time dimension; for those the year is parsed out of the
    # REF_PERIOD attribute so that every table still partitions on year.
    rows = [
        row(
            "year",
            DERIVED["year"],
            "TIME_PERIOD" if has_time else "REF_PERIOD",
        )
    ]
    rows.append(
        row("country_iso3_code", DERIVED["country_iso3_code"], "REF_AREA")
    )
    # REF_AREA leads the dimensions; the rest follow in SDMX order.
    ordered = ["REF_AREA"] + [d for d in dims if d != "REF_AREA"]
    rows += [concept_row(d) for d in ordered]
    rows.append(concept_row("OBS_VALUE"))
    rows += [concept_row(a) for a in attrs]
    rows.append(row("source_flow", DERIVED["source_flow"]))
    rows.append(row("source_flow_version", DERIVED["source_flow_version"]))
    return rows


def main():
    ARCH_DIR.mkdir(parents=True, exist_ok=True)
    total = 0
    for slug, spec in TABLES.items():
        rows = build(slug, spec)
        path = ARCH_DIR / f"{slug}.csv"
        # lineterminator="\n": csv.writer defaults to CRLF, which trips the
        # repo's mixed-line-ending pre-commit hook and CI.
        with path.open("w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=HEADER, lineterminator="\n")
            w.writeheader()
            w.writerows(rows)
        total += len(rows)
        print(f"  {slug:22s} {len(rows):3d} columns -> {path.name}")
    print(f"{len(TABLES)} architecture files, {total} columns")


if __name__ == "__main__":
    main()
