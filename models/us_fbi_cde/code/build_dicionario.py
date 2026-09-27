"""Build the dicionario table from the code lists the FBI ships in its bundles.

Every coded column in this dataset stores the FBI's published code, not the
bundle-local surrogate, so the dictionary is a straight harvest of the lookup
CSVs plus the handful of code sets the FBI documents only in prose (sex,
attempt/complete, method of entry, the Return A card types).

Codes are harvested from every bundle era, not just the most recent one: the
race list in particular gained categories over time, and a code that appears
only in the 1990s bundles still needs a label.
"""

from __future__ import annotations

import csv
import io
import os
import sys
import zipfile
from collections import OrderedDict
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

from pipelines.datasets.us_fbi_cde.constants import constants

OUT = Path(__file__).resolve().parent / "dicionario.csv"

# Which lookup file feeds which (table, column), and which of its columns hold
# the code and the label.
HARVEST = [
    (
        "nibrs_offense_type",
        "offense_code",
        "offense_name",
        [("offense", "offense_code"), ("arrestee", "offense_code")],
    ),
    (
        "nibrs_location_type",
        "location_code",
        "location_name",
        [("offense", "location_code")],
    ),
    (
        "nibrs_weapon_type",
        "weapon_code",
        "weapon_name",
        [("offense", "weapon_code"), ("arrestee", "weapon_code")],
    ),
    (
        "nibrs_bias_list",
        "bias_code",
        None,
        [("offense", "bias_motivation_code")],
    ),
    (
        "nibrs_cleared_except",
        "cleared_except_code",
        "cleared_except_name",
        [("incident", "cleared_except_code")],
    ),
    (
        "nibrs_age",
        "age_code",
        "age_name",
        [
            ("offender", "age_code"),
            ("victim", "age_code"),
            ("arrestee", "age_code"),
        ],
    ),
    (
        "ref_race",
        "race_code",
        "race_desc",
        [
            ("offender", "race_code"),
            ("victim", "race_code"),
            ("arrestee", "race_code"),
        ],
    ),
    (
        "nibrs_ethnicity",
        "ethnicity_code",
        "ethnicity_name",
        [
            ("offender", "ethnicity_code"),
            ("victim", "ethnicity_code"),
            ("arrestee", "ethnicity_code"),
        ],
    ),
    (
        "nibrs_victim_type",
        "victim_type_code",
        "victim_type_name",
        [("victim", "victim_type_code")],
    ),
    (
        "nibrs_assignment_type",
        "assignment_type_code",
        "assignment_type_name",
        [("victim", "assignment_type_code")],
    ),
    (
        "nibrs_activity_type",
        "activity_type_code",
        "activity_type_name",
        [("victim", "activity_type_code")],
    ),
    (
        "nibrs_injury",
        "injury_code",
        "injury_name",
        [("victim", "injury_code")],
    ),
    (
        "nibrs_relationship",
        "relationship_code",
        "relationship_name",
        [("victim_offender_relationship", "relationship_code")],
    ),
    (
        "nibrs_arrest_type",
        "arrest_type_code",
        "arrest_type_name",
        [("arrestee", "arrest_type_code")],
    ),
    (
        "nibrs_prop_loss_type",
        "prop_loss_id",
        "prop_loss_name",
        [("property", "property_loss_code")],
    ),
    (
        "nibrs_prop_desc_type",
        "prop_desc_code",
        "prop_desc_name",
        [("property", "property_description_code")],
    ),
    (
        "nibrs_suspected_drug_type",
        "suspected_drug_code",
        "suspected_drug_name",
        [("property", "suspected_drug_code")],
    ),
    (
        "nibrs_drug_measure_type",
        "drug_measure_code",
        "drug_measure_name",
        [("property", "drug_measure_code")],
    ),
]

# Code sets the FBI documents in the NIBRS user manual and the Return A record
# description rather than in a shipped lookup file.
LITERAL = {
    # The flag columns carry three different encodings across the bundle eras:
    # t/f in the recent files, Y/N in the older ones, and R for a report date.
    ("incident", "report_date_flag"): {
        "t": "The incident date is the report date",
        "f": "The incident date is the date the incident occurred",
        "R": "The incident date is the report date",
    },
    ("incident", "cargo_theft_flag"): {
        "t": "The incident involved cargo theft",
        "f": "The incident did not involve cargo theft",
        "Y": "The incident involved cargo theft",
        "N": "The incident did not involve cargo theft",
    },
    ("incident", "incident_status"): {
        "ACCEPTED": "Accepted by the FBI",
        "DELETED": "Deleted by the submitting agency",
        "ERRORS": "Submitted with errors",
        "WARNINGS": "Submitted with warnings",
        "0": "No status recorded",
        "7": "No status recorded",
    },
    ("offense", "attempt_complete_flag"): {
        "A": "Attempted",
        "C": "Completed",
    },
    ("offense", "method_entry_code"): {
        "F": "Force",
        "N": "No force",
    },
    ("arrestee", "arrest_group"): {
        "A": "Group A incident report arrest, linked to an incident",
        "B": "Group B arrest report, with no incident and no agency identifier",
    },
    ("arrestee", "multiple_arrestee_indicator"): {
        "M": "Multiple arrestee segments were submitted for this offender",
        "C": "Count arrestee",
        "N": "Not applicable",
        "m": "Multiple arrestee segments were submitted for this offender",
        "c": "Count arrestee",
        "n": "Not applicable",
    },
    ("arrestee", "under_18_disposition_code"): {
        "H": "Handled within the department and released",
        "R": "Referred to another authority",
    },
    ("agency", "core_city_flag"): {
        "Y": "Core city of a metropolitan statistical area",
        "N": "Not a core city",
    },
    ("agency", "nibrs_participated"): {
        "Y": "Reported to NIBRS in the year",
        "N": "Did not report to NIBRS in the year",
    },
    ("hate_crime", "multiple_offense_flag"): {
        "S": "Single offense",
        "M": "Multiple offenses",
    },
    ("hate_crime", "multiple_bias_flag"): {
        "S": "Single bias motivation",
        "M": "Multiple bias motivations",
    },
    ("ucr_summary", "record_type_code"): {
        "0": "Not updated",
        "2": "Adjustment to a previously filed month",
        "4": "Not available",
        "5": "Normal return",
        "8": "Included but unusable",
    },
    # Automatic-firearm weapon codes, present in the data but absent from the
    # weapon lookup the bundles ship.
    ("__weapon__", "__weapon__"): {
        "21": "Firearm (automatic)",
        "21A": "Firearm (automatic)",
        "22": "Handgun (automatic)",
        "23": "Rifle (automatic)",
        "24A": "Shotgun (automatic)",
        "25": "Other firearm (automatic)",
    },
    ("ucr_summary", "breakdown_reported_flag"): {
        "P": "The agency reported the breakdown offenses as well as the totals",
        "T": "The agency reported only the totals",
    },
}

# Sex and resident status are shared by several tables and documented in prose.
# Lower-case variants appear in the older bundles, and X in the recent ones.
SEX = {
    "M": "Male",
    "F": "Female",
    "U": "Unknown",
    "X": "Not specified",
    "m": "Male",
    "f": "Female",
    "u": "Unknown",
}
RESIDENT = {
    "R": "Resident of the locality",
    "N": "Non-resident",
    "U": "Unknown",
}
for table in ("offender", "victim", "arrestee"):
    LITERAL[(table, "sex_code")] = SEX
for table in ("victim", "arrestee"):
    LITERAL[(table, "resident_status_code")] = RESIDENT

# The 26 published Return A line items, labelled as they appear on the form.
RETA_LABELS = {
    "murder": "Murder and non-negligent manslaughter",
    "manslaughter": "Manslaughter by negligence",
    "rape_total": "Rape, total",
    "rape_by_force": "Rape by force",
    "attempted_rape": "Attempted rape",
    "robbery_total": "Robbery, total",
    "robbery_firearm": "Robbery with a firearm",
    "robbery_knife": "Robbery with a knife or cutting instrument",
    "robbery_other_weapon": "Robbery with another dangerous weapon",
    "robbery_strong_arm": "Strong-arm robbery, using hands, fists or feet",
    "assault_total": "Assault, total, aggravated and simple combined",
    "assault_firearm": "Aggravated assault with a firearm",
    "assault_knife": "Aggravated assault with a knife or cutting instrument",
    "assault_other_weapon": "Aggravated assault with another dangerous weapon",
    "assault_hands_feet": "Aggravated assault with hands, fists or feet",
    "assault_simple": "Simple assault",
    "burglary_total": "Burglary and breaking or entering, total",
    "burglary_forcible_entry": "Burglary with forcible entry",
    "burglary_unlawful_entry": "Burglary with unlawful entry and no force",
    "burglary_attempted": "Attempted forcible entry",
    "larceny_total": "Larceny-theft, total, excluding motor vehicle theft",
    "motor_vehicle_theft_total": "Motor vehicle theft, total",
    "motor_vehicle_theft_auto": "Theft of an automobile",
    "motor_vehicle_theft_truck_bus": "Theft of a truck or bus",
    "motor_vehicle_theft_other": "Theft of another type of vehicle",
    "grand_total": "Grand total of all offense line items",
}

# UCR population group codes, from the Return A record description.
POPULATION_GROUPS = {
    "0": "Possessions",
    "1": "Cities 250,000 or over",
    "1A": "Cities 1,000,000 or over",
    "1B": "Cities from 500,000 through 999,999",
    "1C": "Cities from 250,000 through 499,999",
    "2": "Cities from 100,000 through 249,999",
    "3": "Cities from 50,000 through 99,999",
    "4": "Cities from 25,000 through 49,999",
    "5": "Cities from 10,000 through 24,999",
    "6": "Cities from 2,500 through 9,999",
    "7": "Cities under 2,500",
    "8": "Non-metropolitan counties",
    "8A": "Non-metropolitan counties 100,000 or over",
    "8B": "Non-metropolitan counties from 25,000 through 99,999",
    "8C": "Non-metropolitan counties from 10,000 through 24,999",
    "8D": "Non-metropolitan counties under 10,000",
    "8E": "Non-metropolitan state police",
    "9": "Metropolitan counties",
    "9A": "Metropolitan counties 100,000 or over",
    "9B": "Metropolitan counties from 25,000 through 99,999",
    "9C": "Metropolitan counties from 10,000 through 24,999",
    "9D": "Metropolitan counties under 10,000",
    "9E": "Metropolitan state police",
}
for table in ("agency", "hate_crime"):
    LITERAL[(table, "population_group_code")] = POPULATION_GROUPS


def read_lookup(zip_path, logical):
    """Read one lookup CSV out of a bundle, matching the name case-insensitively."""
    with zipfile.ZipFile(zip_path) as zf:
        for name in zf.namelist():
            if os.path.basename(name).lower() == f"{logical}.csv":
                with zf.open(name) as handle:
                    text = io.TextIOWrapper(
                        handle, encoding="latin-1", newline=""
                    )
                    return list(csv.DictReader(text))
    return []


def bundle_sample():
    """One bundle per era, so codes retired before 2020 are still picked up."""
    root = constants.DATA_ROOT.value / "input" / "nibrs"
    chosen = []
    for year in (1991, 1995, 2000, 2005, 2010, 2015, 2019, 2022, 2025):
        matches = sorted(root.glob(f"*-{year}.zip"))
        if matches:
            chosen.append(matches[0])
    return chosen


def main():
    rows = OrderedDict()

    def add(table, column, key, value):
        key = (key or "").strip()
        value = (value or "").strip()
        if not key or not value:
            return
        rows.setdefault((table, column, key), value)

    for zip_path in bundle_sample():
        for logical, code_col, label_col, targets in HARVEST:
            for record in read_lookup(zip_path, logical):
                record = {k.strip().lower(): v for k, v in record.items() if k}
                code = record.get(code_col)
                if label_col is None:
                    # The bias list is named bias_name in the early bundles and
                    # bias_desc in the recent ones.
                    label = record.get("bias_desc") or record.get("bias_name")
                else:
                    label = record.get(label_col)
                for table, column in targets:
                    add(table, column, code, label)

    for (table, column), mapping in LITERAL.items():
        if table.startswith("__"):
            continue
        for code, label in mapping.items():
            add(table, column, code, label)
    for code, label in RETA_LABELS.items():
        add("ucr_summary", "offense_code", code, label)
    for code, label in LITERAL[("__weapon__", "__weapon__")].items():
        add("offense", "weapon_code", code, label)
        add("arrestee", "weapon_code", code, label)

    ordered = sorted(
        rows.items(), key=lambda kv: (kv[0][0], kv[0][1], kv[0][2])
    )
    with open(OUT, "w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle, lineterminator="\n")
        writer.writerow(
            [
                "id_tabela",
                "nome_coluna",
                "chave",
                "cobertura_temporal",
                "valor",
            ]
        )
        for (table, column, key), value in ordered:
            writer.writerow([table, column, key, "", value])
    print(f"wrote {len(ordered):,} dictionary entries to {OUT}")
    seen = {}
    for (table, column, _), _ in ordered:
        seen[(table, column)] = seen.get((table, column), 0) + 1
    for (table, column), count in sorted(seen.items()):
        print(f"  {table}.{column}: {count}")


if __name__ == "__main__":
    main()
