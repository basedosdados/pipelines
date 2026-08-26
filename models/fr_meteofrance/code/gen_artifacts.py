"""Generate the architecture CSVs, the dbt models and schema.yml from ``schema_map``.

Run after any schema change so the architecture, the SQL and the tests cannot
drift apart:

    uv run python models/fr_meteofrance/code/gen_artifacts.py
"""

import csv
import os
import sys
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from descriptions_i18n import EN_ES
from schema_map import (
    DICIONARIO_COLUMNS,
    NORMALE_COLUMNS,
    OBSERVATIONS,
    STATION_CLIMATOLOGIQUE_COLUMNS,
    STATION_SYNOP_COLUMNS,
    SYNOP_COLUMNS,
    SYNOP_LEADING,
)

HERE = Path(os.path.dirname(os.path.abspath(__file__)))
MODELS = HERE.parent
ARCH = HERE / "architecture"
DATASET = "fr_meteofrance"

ARCH_HEADER = [
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

DIRECTORY = {
    "ano": "br_bd_diretorios_data_tempo.ano:ano",
    "mes": "br_bd_diretorios_data_tempo.mes:mes",
    "id_departement": "br_bd_diretorios_fr.departement:id_departamento",
}

# (target, bigquery type, unit, dictionary?, description PT, original source name)
SYNOP_ARCH = [(n, t, u, d, desc, "") for n, t, u, d, desc in SYNOP_LEADING] + [
    (tgt, t, u, d, desc, src) for src, tgt, t, u, d, desc in SYNOP_COLUMNS
]
SYNOP_ARCH = [
    (
        n,
        t,
        u,
        d,
        desc,
        {
            "ano": "validity_time",
            "mes": "validity_time",
            "data": "validity_time",
            "hora": "validity_time",
            "indicatif_omm": "geo_id_wmo",
            "date_heure_traitement": "reference_time",
            "date_heure_insertion": "insert_time",
        }.get(n, src),
    )
    for n, t, u, d, desc, src in SYNOP_ARCH
]

STATION_SYNOP_ARCH = [
    (
        n,
        t,
        u,
        d,
        desc,
        {
            "indicatif_omm": "geo_id_wmo",
            "indicatif_wigos": "geo_id_wigos",
            "nom_station": "name",
            "latitude": "lat",
            "longitude": "lon",
            "altitude": "Altitude",
            "date_ouverture": "Date_ouverture",
        }.get(n, ""),
    )
    for n, t, u, d, desc in STATION_SYNOP_COLUMNS
]
STATION_CLIM_ARCH = [
    (n, t, u, d, desc, "")
    for n, t, u, d, desc in STATION_CLIMATOLOGIQUE_COLUMNS
]
NORMALE_ARCH = [(n, t, u, d, desc, "") for n, t, u, d, desc in NORMALE_COLUMNS]
DICIONARIO_ARCH = [
    (n, t, "", False, desc, "") for n, t, _u, _d, desc in DICIONARIO_COLUMNS
]

TABLES = {
    "synop": SYNOP_ARCH,
    "station_synop": STATION_SYNOP_ARCH,
    "station_climatologique": STATION_CLIM_ARCH,
    "normale_climatologique": NORMALE_ARCH,
    "dicionario": DICIONARIO_ARCH,
}

COVERAGE = {
    "synop": "1996(1)2026",
    "station_synop": "1996(1)2026",
    "station_climatologique": "1991(1)2020",
    "normale_climatologique": "1991(1)2020",
    "dicionario": "",
}

PARTITION = {
    "synop": {
        "field": "ano",
        "data_type": "int64",
        "range": {"start": 1996, "end": 2031, "interval": 1},
    },
}
CLUSTER = {"synop": ["indicatif_omm"]}


def write_architecture():
    ARCH.mkdir(parents=True, exist_ok=True)
    for table, cols in TABLES.items():
        path = ARCH / f"{table}.csv"
        with path.open("w", newline="", encoding="utf-8") as fh:
            writer = csv.writer(fh)
            writer.writerow(ARCH_HEADER)
            for name, btype, unit, is_dict, desc, original in cols:
                writer.writerow(
                    [
                        name,
                        btype,
                        desc,
                        "",
                        "yes" if is_dict else "no",
                        DIRECTORY.get(name, ""),
                        unit,
                        "no",
                        OBSERVATIONS.get(name, ""),
                        original,
                    ]
                )
        print(f"  architecture/{table}.csv  ({len(cols)} columns)")


def columns_json():
    """Payload for `bulk_upsert_columns`, one entry per table."""
    payload = {}
    for table, cols in TABLES.items():
        entries = []
        for name, btype, unit, is_dict, desc, _original in cols:
            en, es = EN_ES.get(name, ("", ""))
            entry = {
                "name": name,
                "bigquery_type": btype,
                "description_pt": desc,
                "description_en": en,
                "description_es": es,
                "covered_by_dictionary": is_dict,
                "is_partition": name == "ano" and table == "synop",
            }
            if unit:
                entry["measurement_unit"] = unit
            if OBSERVATIONS.get(name):
                entry["observations"] = OBSERVATIONS[name]
            if DIRECTORY.get(name):
                entry["directory_column"] = DIRECTORY[name]
            entries.append(entry)
        payload[table] = entries
    return payload


def sql_cast(name, btype):
    if btype == "GEOGRAPHY":
        return f"    st_geogfromtext(safe_cast({name} as string), make_valid => true) {name},"
    return f"    safe_cast({name} as {btype.lower()}) {name},"


def write_models():
    for table, cols in TABLES.items():
        config = [
            '        schema="fr_meteofrance",',
            f'        alias="{table}",',
            '        materialized="table",',
        ]
        if table in PARTITION:
            p = PARTITION[table]
            config.append(
                "        partition_by={\n"
                f'            "field": "{p["field"]}",\n'
                f'            "data_type": "{p["data_type"]}",\n'
                f'            "range": {{"start": {p["range"]["start"]}, '
                f'"end": {p["range"]["end"]}, "interval": {p["range"]["interval"]}}},\n'
                "        },"
            )
        if table in CLUSTER:
            config.append(f"        cluster_by={CLUSTER[table]!r},")

        casts = [sql_cast(name, btype) for name, btype, *_ in cols]
        casts[-1] = casts[-1].rstrip(",")
        body = "\n".join(
            [
                "{{",
                "    config(",
                *config,
                "    )",
                "}}",
                "",
                "",
                "select",
                *casts,
                "from",
                f'    {{{{ set_datalake_project("fr_meteofrance_staging.{table}") }}}}',
                "    as t",
                "",
            ]
        )
        path = MODELS / f"{DATASET}__{table}.sql"
        path.write_text(body, encoding="utf-8")
        print(f"  {path.name}")


def main():
    write_architecture()
    write_models()
    import json

    (HERE / "columns.json").write_text(
        json.dumps(columns_json(), ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    print("  columns.json")


if __name__ == "__main__":
    main()
