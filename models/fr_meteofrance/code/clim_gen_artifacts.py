"""Generate architecture CSVs, dbt models and columns JSON for the climatologiques tables.

    uv run python models/fr_meteofrance/code/clim_gen_artifacts.py

Run after any change to ``clim_schema.py``, then run pre-commit — the emitted
SQL is unformatted and ``sqlfmt`` rewrites it.
"""

import csv
import gzip
import json
import os
import sys
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import clim_schema as cs

HERE = Path(os.path.dirname(os.path.abspath(__file__)))
MODELS = HERE.parent
ARCH = HERE / "architecture"
INPUT = Path(
    os.path.expanduser(
        os.environ.get("MFC_INPUT", "~/Downloads/fr_meteofrance_clim/input")
    )
)

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

# The daily archive reaches back to 1688; the range must cover every row or
# BigQuery drops the rest into __UNPARTITIONED__.
PARTITION = {
    "field": "annee",
    "data_type": "int64",
    "range": {"start": 1688, "end": 2031, "interval": 1},
}
CLUSTER = ["numero_poste"]

DIRECTORY = {
    "annee": "br_bd_diretorios_data_tempo.ano:ano",
    "mois": "br_bd_diretorios_data_tempo.mes:mes",
    "id_departement": "br_bd_diretorios_fr.departement:id_departamento",
}

LEADING_DAILY = [
    (
        "annee",
        "INT64",
        "year",
        False,
        "Ano da observação",
        "Year of the observation",
        "Año de la observación",
        "AAAAMMJJ",
    ),
    (
        "mois",
        "INT64",
        "month",
        False,
        "Mês da observação",
        "Month of the observation",
        "Mes de la observación",
        "AAAAMMJJ",
    ),
    (
        "date",
        "DATE",
        "",
        False,
        "Data da observação",
        "Date of the observation",
        "Fecha de la observación",
        "AAAAMMJJ",
    ),
    (
        "numero_poste",
        "STRING",
        "",
        False,
        "Número Météo-France do posto, de oito dígitos",
        "Eight-digit Météo-France station number",
        "Número Météo-France del puesto, de ocho dígitos",
        "NUM_POSTE",
    ),
]
LEADING_MONTHLY = [
    (
        "annee",
        "INT64",
        "year",
        False,
        "Ano da observação",
        "Year of the observation",
        "Año de la observación",
        "AAAAMM",
    ),
    (
        "mois",
        "INT64",
        "month",
        False,
        "Mês da observação",
        "Month of the observation",
        "Mes de la observación",
        "AAAAMM",
    ),
    (
        "numero_poste",
        "STRING",
        "",
        False,
        "Número Météo-France do posto, de oito dígitos",
        "Eight-digit Météo-France station number",
        "Número Météo-France del puesto, de ocho dígitos",
        "NUM_POSTE",
    ),
]
POSTE = [
    (
        "numero_poste",
        "STRING",
        "",
        False,
        "Número Météo-France do posto, de oito dígitos",
        "Eight-digit Météo-France station number",
        "Número Météo-France del puesto, de ocho dígitos",
        "NUM_POSTE",
    ),
    (
        "nom_poste",
        "STRING",
        "",
        False,
        "Nome usual do posto",
        "Common name of the station",
        "Nombre usual del puesto",
        "NOM_USUEL",
    ),
    (
        "id_departement",
        "STRING",
        "",
        False,
        "Código do departamento ou da coletividade ultramarina do posto",
        "Code of the department or overseas collectivity of the station",
        "Código del departamento o de la colectividad de ultramar del puesto",
        "",
    ),
    (
        "latitude",
        "FLOAT64",
        "degree",
        False,
        "Latitude do posto, negativa ao sul do equador",
        "Latitude of the station, negative south of the equator",
        "Latitud del puesto, negativa al sur del ecuador",
        "LAT",
    ),
    (
        "longitude",
        "FLOAT64",
        "degree",
        False,
        "Longitude do posto, negativa a oeste de Greenwich",
        "Longitude of the station, negative west of Greenwich",
        "Longitud del puesto, negativa al oeste de Greenwich",
        "LON",
    ),
    (
        "altitude",
        "FLOAT64",
        "meter",
        False,
        "Altitude do pé do abrigo, ou do pluviômetro quando não há abrigo",
        "Altitude of the station, measured at the foot of the shelter or rain gauge",
        "Altitud del puesto, medida al pie del abrigo o del pluviómetro",
        "ALTI",
    ),
    (
        "geolocalisation",
        "GEOGRAPHY",
        "",
        False,
        "Ponto geográfico do posto, em WGS 84",
        "Geographic point of the station, in WGS 84",
        "Punto geográfico del puesto, en WGS 84",
        "",
    ),
]

OBSERVATIONS = {
    "id_departement": (
        "Segue a codificação da Météo-France, que difere do Code officiel géographique do "
        "INSEE: a Córsega recebe 20, e as coletividades de além-mar 984, 986, 987 e 988 não "
        "constam do diretório francês."
    ),
    "annee": (
        "O arquivo diário remonta a 1688, muito antes da rede moderna: os anos anteriores a "
        "1900 têm pouquíssimos postos."
    ),
}


def header(path):
    with gzip.open(path, "rt", encoding="utf-8") as fh:
        return fh.readline().strip().split(";")


def tables():
    d = cs.descriptors()
    quot_a = header(next((INPUT / "quot").glob("*_RR-T-Vent.csv.gz")))
    quot_b = header(next((INPUT / "quot").glob("*_autres-parametres.csv.gz")))
    mens = header(next((INPUT / "mens").glob("*.csv.gz")))

    q = cs.expand(
        quot_a + [c for c in quot_b if c not in quot_a],
        cs.QUOT_PARAMS,
        cs.QUOT_FLAGS,
        d,
    )
    m = cs.expand(mens, cs.MENS_PARAMS, {}, d)
    return {
        "quotidienne": LEADING_DAILY
        + [(r[0], r[1], r[2], r[3], r[4], r[5], r[6], r[7]) for r in q],
        "mensuelle": LEADING_MONTHLY
        + [(r[0], r[1], r[2], r[3], r[4], r[5], r[6], r[7]) for r in m],
        "poste": POSTE,
    }


def write_architecture(spec):
    ARCH.mkdir(parents=True, exist_ok=True)
    for table, rows in spec.items():
        with (ARCH / f"{table}.csv").open(
            "w", newline="", encoding="utf-8"
        ) as fh:
            w = csv.writer(fh)
            w.writerow(ARCH_HEADER)
            for name, btype, unit, is_dict, pt, _en, _es, original in rows:
                w.writerow(
                    [
                        name,
                        btype,
                        pt,
                        "",
                        "yes" if is_dict else "no",
                        DIRECTORY.get(name, ""),
                        unit,
                        "no",
                        OBSERVATIONS.get(name, ""),
                        original,
                    ]
                )
        print(f"  architecture/{table}.csv  ({len(rows)} columns)")


def write_models(spec):
    for table, rows in spec.items():
        config = [
            '        schema="fr_meteofrance",',
            f'        alias="{table}",',
            '        materialized="table",',
        ]
        if table != "poste":
            p = PARTITION
            config.append(
                "        partition_by={\n"
                f'            "field": "{p["field"]}",\n'
                f'            "data_type": "{p["data_type"]}",\n'
                f'            "range": {{"start": {p["range"]["start"]}, '
                f'"end": {p["range"]["end"]}, "interval": {p["range"]["interval"]}}},\n'
                "        },"
            )
            config.append(f"        cluster_by={CLUSTER!r},")
        casts = []
        for name, btype, *_ in rows:
            if btype == "GEOGRAPHY":
                casts.append(
                    f"    st_geogfromtext(safe_cast({name} as string), make_valid => true) {name},"
                )
            else:
                casts.append(
                    f"    safe_cast({name} as {btype.lower()}) {name},"
                )
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
        (MODELS / f"fr_meteofrance__{table}.sql").write_text(
            body, encoding="utf-8"
        )
        print(f"  fr_meteofrance__{table}.sql")


def write_columns_json(spec):
    payload = {}
    for table, rows in spec.items():
        entries = []
        for name, btype, unit, is_dict, pt, en, es, _original in rows:
            e = {
                "name": name,
                "bigquery_type": btype,
                "description_pt": pt,
                "description_en": en,
                "description_es": es,
                "covered_by_dictionary": is_dict,
                "is_partition": name == "annee" and table != "poste",
            }
            if unit:
                e["measurement_unit"] = unit
            if OBSERVATIONS.get(name):
                e["observations"] = OBSERVATIONS[name]
            if DIRECTORY.get(name):
                e["directory_column"] = DIRECTORY[name]
            entries.append(e)
        payload[table] = entries
    (HERE / "clim_columns.json").write_text(
        json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    print("  clim_columns.json")


if __name__ == "__main__":
    spec = tables()
    write_architecture(spec)
    write_models(spec)
    write_columns_json(spec)
