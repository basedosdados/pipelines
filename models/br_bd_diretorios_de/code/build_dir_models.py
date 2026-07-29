#!/usr/bin/env python3
"""Generate dbt SQL + schema.yml + architecture CSVs for br_bd_diretorios_de.

Run: cd models/br_bd_diretorios_de/code && python3 build_dir_models.py
"""

import csv
import os

HERE = os.path.dirname(os.path.abspath(__file__))
MODELS = os.path.join(HERE, "..")
ARCH = os.path.join(HERE, "architecture")
DS = "br_bd_diretorios_de"

# (col, type, description_en, measurement_unit, directory_column)
TABLES = {
    "state": {
        "desc": "Directory of the 16 German federal states (Bundesländer), keyed by the 2-digit Land code",
        "cols": [
            (
                "id_state",
                "STRING",
                "State identifier, 2-digit Land code",
                "",
                "",
            ),
            (
                "state_abbreviation",
                "STRING",
                "Official state abbreviation (e.g. BY, NW)",
                "",
                "",
            ),
            ("name", "STRING", "State name in German", "", ""),
            ("name_en", "STRING", "State name in English", "", ""),
        ],
        "key": "id_state",
    },
    "county": {
        "desc": "Directory of German counties (Kreise and kreisfreie Städte) at 2021 boundaries, keyed by the 5-digit county code",
        "cols": [
            (
                "id_county",
                "STRING",
                "County identifier, 5-digit Kreisschlüssel",
                "",
                "",
            ),
            (
                "id_state",
                "STRING",
                "State identifier, 2-digit Land code",
                "",
                f"{DS}.state:id_state",
            ),
            ("name", "STRING", "County name in German", "", ""),
        ],
        "key": "id_county",
    },
    "municipality": {
        "desc": "Directory of German municipalities (Gemeinden) at 2021 boundaries, keyed by the 8-digit Amtlicher Gemeindeschlüssel",
        "cols": [
            (
                "id_municipality",
                "STRING",
                "Municipality identifier, 8-digit Amtlicher Gemeindeschlüssel (AGS)",
                "",
                "",
            ),
            (
                "id_county",
                "STRING",
                "County identifier, 5-digit Kreisschlüssel",
                "",
                f"{DS}.county:id_county",
            ),
            (
                "id_state",
                "STRING",
                "State identifier, 2-digit Land code",
                "",
                f"{DS}.state:id_state",
            ),
            ("name", "STRING", "Municipality name in German", "", ""),
        ],
        "key": "id_municipality",
    },
    "constituency": {
        "desc": "Directory of German electoral constituencies (Wahlkreise), federal (Bundestag) and state (Landtag). Identity reflects the most recent election; constituencies are periodically redrawn",
        "cols": [
            (
                "id_constituency",
                "STRING",
                "Constituency identifier: federal_<nr> or state_<state>_<nr>",
                "",
                "",
            ),
            (
                "constituency_type",
                "STRING",
                "Level of the constituency: federal or state",
                "",
                "",
            ),
            (
                "id_state",
                "STRING",
                "State identifier, 2-digit Land code",
                "",
                f"{DS}.state:id_state",
            ),
            ("name", "STRING", "Constituency (Wahlkreis) name", "", ""),
        ],
        "key": "id_constituency",
    },
    "party": {
        "desc": "Directory of German parties and lists using GERDA normalized names, enriched with ParlGov attributes where available",
        "cols": [
            (
                "id_party",
                "STRING",
                "Party or list, GERDA normalized name",
                "",
                "",
            ),
            (
                "name",
                "STRING",
                "Party name in English (ParlGov) or a readable form of the GERDA name",
                "",
                "",
            ),
            ("name_short", "STRING", "Short party name (ParlGov)", "", ""),
            ("family", "STRING", "Party family (ParlGov)", "", ""),
            (
                "left_right",
                "FLOAT64",
                "Left-right ideology score from ParlGov (0 left to 10 right)",
                "",
                "",
            ),
            ("parlgov_party_id", "STRING", "ParlGov party identifier", "", ""),
            (
                "is_far_right",
                "STRING",
                "1 if classified far right by GERDA, else 0",
                "",
                "",
            ),
            (
                "is_far_left",
                "STRING",
                "1 if classified far left by GERDA (excluding Die Linke/PDS), else 0",
                "",
                "",
            ),
            (
                "is_cdu_csu",
                "STRING",
                "1 if the party is CDU or CSU, else 0",
                "",
                "",
            ),
            (
                "category",
                "STRING",
                "Row category: party, residual_other, local_voter_groups, or independents",
                "",
                "",
            ),
        ],
        "key": "id_party",
    },
}


def cast(col, t):
    return f"safe_cast({col} as {t.lower()}) {col}"


def build():
    os.makedirs(ARCH, exist_ok=True)
    blocks = []
    for tbl, spec in TABLES.items():
        cols = spec["cols"]
        # SQL
        sql = [
            "{{",
            "    config(",
            f'        schema="{DS}",',
            f'        alias="{tbl}",',
            '        materialized="table",',
            "    )",
            "}}",
            "",
            "select",
        ]
        sql.append(",\n".join(f"    {cast(c, t)}" for c, t, *_ in cols))
        sql.append(
            f'from {{{{ set_datalake_project("{DS}_staging.{tbl}") }}}} as t'
        )
        with open(os.path.join(MODELS, f"{DS}__{tbl}.sql"), "w") as f:
            f.write("\n".join(sql) + "\n")
        # architecture CSV
        with open(os.path.join(ARCH, f"{tbl}.csv"), "w", newline="") as f:
            w = csv.writer(f)
            w.writerow(
                [
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
            )
            for c, t, desc, unit, direc in cols:
                w.writerow([c, t, desc, "", "no", direc, unit, "no", "", c])
        # schema.yml block
        b = [
            f"  - name: {DS}__{tbl}",
            "    description: >",
            f"      {spec['desc']}",
            "    tests:",
            "      - dbt_utils.unique_combination_of_columns:",
            f"          combination_of_columns: ['{spec['key']}']",
            "    columns:",
        ]
        for c, _t, desc, _unit, direc in cols:
            b.append(f"      - name: {c}")
            b.append("        description: >")
            b.append(f"          {desc}")
            tests = []
            if c == spec["key"]:
                tests.append("          - not_null")
                tests.append("          - unique")
            if direc:
                tgt = direc.split(":")[0].replace(".", "__")
                fld = direc.split(":")[1]
                tests += [
                    "          - relationships:",
                    f"              to: ref('{tgt}')",
                    f"              field: {fld}",
                ]
            if tests:
                b.append("        tests:")
                b.extend(tests)
        blocks.append("\n".join(b))
    with open(os.path.join(MODELS, "schema.yml"), "w") as f:
        f.write("---\nversion: 2\n\nmodels:\n" + "\n".join(blocks) + "\n")
    print(
        f"built {len(TABLES)} directory models + schema.yml + architecture CSVs"
    )


if __name__ == "__main__":
    build()
