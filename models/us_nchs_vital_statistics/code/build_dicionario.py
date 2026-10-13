"""Build the `dicionario` table for us_nchs_vital_statistics.

Value labels are transcribed from the NCHS user guides published with each annual
public-use file (natality/mortality "User Guide to the ... Public Use File").
They are curated by hand rather than scraped: the guides are PDF-extracted text
in which value blocks bleed into one another and OCR damage is common, so an
automatic parse produced demonstrably wrong labels (e.g. attendant categories
landing on `sex`). Publishing a wrong label onto a health code is worse than
publishing none, so only verified entries appear here.

Coverage is deliberately partial and is stated in the table description:
the coded columns whose labels are reproduced below, for the eras shown. Columns
absent here carry `covered_by_dictionary = no`; their code sets are documented in
the per-year NCHS user guides linked from the dataset's raw data sources.
"""

import csv
from pathlib import Path

OUT = Path(__file__).parent / "layouts" / "dicionario.csv"

# column -> {coverage: {code: label}}
D: dict[tuple[str, str], dict[str, dict[str, str]]] = {}


def add(table, column, coverage, mapping):
    D.setdefault((table, column), {})[coverage] = mapping


# --------------------------------------------------------------------------- #
# BIRTH
# --------------------------------------------------------------------------- #
add("birth", "sex", "2003(1)2024", {"M": "Male", "F": "Female"})
add("birth", "sex", "1968(1)2002", {"1": "Male", "2": "Female"})

add(
    "birth",
    "residence_status",
    "1968(1)2024",
    {
        "1": "Resident: state and county of occurrence and residence are the same",
        "2": "Intrastate nonresident: state of occurrence and residence are the same but county differs",
        "3": "Interstate nonresident: state of occurrence and residence differ, both in the United States",
        "4": "Foreign resident: residence is outside the 50 states and the District of Columbia",
    },
)

add(
    "birth",
    "mother_nativity_code",
    "2003(1)2024",
    {
        "1": "Born in the United States (50 states)",
        "2": "Born outside the United States (includes possessions)",
        "3": "Unknown or not stated",
    },
)

add(
    "birth",
    "mother_marital_status",
    "2003(1)2024",
    {"1": "Married", "2": "Unmarried", "9": "Unknown or not stated"},
)
add(
    "birth",
    "mother_marital_status",
    "1978(1)2002",
    {"1": "Married", "2": "Unmarried"},
)

_RACE6 = {
    "1": "White only",
    "2": "Black only",
    "3": "American Indian or Alaska Native only",
    "4": "Asian only",
    "5": "Native Hawaiian or Other Pacific Islander only",
    "6": "More than one race",
}
add("birth", "mother_race_recode_6", "2014(1)2024", _RACE6)
add("birth", "father_race_recode_6", "2014(1)2024", _RACE6)

add(
    "birth",
    "mother_race_recode_31",
    "2014(1)2024",
    {
        "01": "White only",
        "02": "Black only",
        "03": "American Indian or Alaska Native only",
        "04": "Asian only",
        "05": "Native Hawaiian or Other Pacific Islander only",
        "06": "Black and White",
        "07": "Black and American Indian or Alaska Native",
        "08": "Black and Asian",
        "09": "Black and Native Hawaiian or Other Pacific Islander",
        "10": "American Indian or Alaska Native and White",
        "11": "American Indian or Alaska Native and Asian",
        "12": "American Indian or Alaska Native and Native Hawaiian or Other Pacific Islander",
        "13": "Asian and White",
        "14": "Asian and Native Hawaiian or Other Pacific Islander",
        "15": "Native Hawaiian or Other Pacific Islander and White",
        "16": "Black, American Indian or Alaska Native, and White",
        "17": "Black, American Indian or Alaska Native, and Asian",
        "18": "Black, American Indian or Alaska Native, and Native Hawaiian or Other Pacific Islander",
        "19": "Black, Asian, and White",
        "20": "Black, Asian, and Native Hawaiian or Other Pacific Islander",
        "21": "Black, Native Hawaiian or Other Pacific Islander, and White",
        "22": "American Indian or Alaska Native, Asian, and White",
        "23": "American Indian or Alaska Native, Native Hawaiian or Other Pacific Islander, and White",
        "24": "American Indian or Alaska Native, Asian, and Native Hawaiian or Other Pacific Islander",
        "25": "Asian, Native Hawaiian or Other Pacific Islander, and White",
        "26": "Black, American Indian or Alaska Native, Asian, and White",
        "27": "Black, American Indian or Alaska Native, Asian, and Native Hawaiian or Other Pacific Islander",
        "28": "Black, American Indian or Alaska Native, Native Hawaiian or Other Pacific Islander, and White",
        "29": "Black, Asian, Native Hawaiian or Other Pacific Islander, and White",
        "30": "American Indian or Alaska Native, Asian, Native Hawaiian or Other Pacific Islander, and White",
        "31": "Black, American Indian or Alaska Native, Asian, Native Hawaiian or Other Pacific Islander, and White",
    },
)

add(
    "birth",
    "mother_hispanic_origin_code",
    "2014(1)2024",
    {
        "0": "Non-Hispanic",
        "1": "Mexican",
        "2": "Puerto Rican",
        "3": "Cuban",
        "4": "Central and South American",
        "5": "Other and unknown Hispanic origin",
        "9": "Hispanic origin not stated",
    },
)

add(
    "birth",
    "mother_race_hispanic_code",
    "2003(1)2024",
    {
        "1": "Non-Hispanic White only",
        "2": "Non-Hispanic Black only",
        "3": "Non-Hispanic American Indian or Alaska Native only",
        "4": "Non-Hispanic Asian only",
        "5": "Non-Hispanic Native Hawaiian or Other Pacific Islander only",
        "6": "Non-Hispanic more than one race",
        "7": "Hispanic",
        "8": "Origin unknown or not stated",
    },
)

add(
    "birth",
    "mother_education_code",
    "2003(1)2024",
    {
        "1": "8th grade or less",
        "2": "9th through 12th grade with no diploma",
        "3": "High school graduate or GED completed",
        "4": "Some college credit but not a degree",
        "5": "Associate degree",
        "6": "Bachelor's degree",
        "7": "Master's degree",
        "8": "Doctorate or professional degree",
        "9": "Unknown",
    },
)

add(
    "birth",
    "birth_attendant",
    "2003(1)2024",
    {
        "1": "Doctor of Medicine",
        "2": "Doctor of Osteopathy",
        "3": "Certified Nurse Midwife or Certified Midwife",
        "4": "Other midwife",
        "5": "Other",
        "9": "Unknown or not stated",
    },
)

add(
    "birth",
    "delivery_method_recode",
    "2005(1)2024",
    {
        "1": "Vaginal, excluding vaginal after previous caesarean",
        "2": "Vaginal after previous caesarean",
        "3": "Primary caesarean",
        "4": "Repeat caesarean",
        "5": "Vaginal, unknown if previous caesarean",
        "6": "Caesarean, unknown if previous caesarean",
        "9": "Not stated",
    },
)

add(
    "birth",
    "plurality",
    "2003(1)2024",
    {"1": "Single", "2": "Twin", "3": "Triplet", "4": "Quadruplet or higher"},
)
add(
    "birth",
    "plurality",
    "1968(1)2002",
    {
        "1": "Single",
        "2": "Twin",
        "3": "Triplet",
        "4": "Quadruplet",
        "5": "Quintuplet or higher",
    },
)

add(
    "birth",
    "gestation_recode_3",
    "2003(1)2024",
    {"1": "Under 37 weeks", "2": "37 weeks and over", "3": "Not stated"},
)

add(
    "birth",
    "birth_weight_recode_4",
    "2003(1)2024",
    {
        "1": "227 to 1499 grams",
        "2": "1500 to 2499 grams",
        "3": "2500 to 8165 grams",
        "4": "Unknown or not stated",
    },
)

add(
    "birth",
    "birth_day_of_week",
    "2003(1)2024",
    {
        "1": "Sunday",
        "2": "Monday",
        "3": "Tuesday",
        "4": "Wednesday",
        "5": "Thursday",
        "6": "Friday",
        "7": "Saturday",
    },
)

# --------------------------------------------------------------------------- #
# DEATH
# --------------------------------------------------------------------------- #
add("death", "sex", "2003(1)2024", {"M": "Male", "F": "Female"})
add("death", "sex", "1968(1)2002", {"1": "Male", "2": "Female"})

add(
    "death",
    "residence_status",
    "1968(1)2024",
    {
        "1": "Residents: state and county of occurrence and residence are the same",
        "2": "Intrastate nonresidents: state of occurrence and residence are the same but county differs",
        "3": "Interstate nonresidents: state of occurrence and residence differ, both in the United States",
        "4": "Foreign residents: place of residence is outside the United States",
    },
)

add(
    "death",
    "death_day_of_week",
    "1989(1)2024",
    {
        "1": "Sunday",
        "2": "Monday",
        "3": "Tuesday",
        "4": "Wednesday",
        "5": "Thursday",
        "6": "Friday",
        "7": "Saturday",
        "9": "Unknown",
    },
)

add(
    "death",
    "place_of_death",
    "2003(1)2024",
    {
        "1": "Hospital, clinic or medical center: inpatient",
        "2": "Hospital, clinic or medical center: outpatient or admitted to emergency room",
        "3": "Hospital, clinic or medical center: dead on arrival",
        "4": "Decedent's home",
        "5": "Hospice facility",
        "6": "Nursing home or long-term care",
        "7": "Other",
        "9": "Place of death unknown",
    },
)

add(
    "death",
    "marital_status",
    "2003(1)2024",
    {
        "S": "Never married, single",
        "M": "Married",
        "W": "Widowed",
        "D": "Divorced",
        "U": "Marital status unknown",
    },
)

add(
    "death",
    "manner_of_death",
    "1999(1)2024",
    {
        "1": "Accident",
        "2": "Suicide",
        "3": "Homicide",
        "4": "Pending investigation",
        "5": "Could not determine",
        "6": "Self-inflicted",
        "7": "Natural",
    },
)

add("death", "autopsy", "2003(1)2024", {"Y": "Yes", "N": "No", "U": "Unknown"})
add(
    "death",
    "injury_at_work",
    "1993(1)2024",
    {"Y": "Yes", "N": "No", "U": "Unknown"},
)

add(
    "death",
    "education_code",
    "2003(1)2024",
    {
        "1": "8th grade or less",
        "2": "9th to 12th grade, no diploma",
        "3": "High school graduate or GED completed",
        "4": "Some college credit, but no degree",
        "5": "Associate degree",
        "6": "Bachelor's degree",
        "7": "Master's degree",
        "8": "Doctorate or professional degree",
        "9": "Unknown",
    },
)

add(
    "death",
    "education_reporting_flag",
    "2003(1)2024",
    {
        "0": "1989 revision of education item on certificate",
        "1": "2003 revision of education item on certificate",
        "2": "No education item on certificate",
    },
)

add(
    "death",
    "race_recode_3",
    "1968(1)2020",
    {"1": "White", "2": "Races other than White or Black", "3": "Black"},
)

add(
    "death",
    "race_recode_5",
    "2004(1)2020",
    {
        "0": "Other (Puerto Rico only)",
        "1": "White",
        "2": "Black",
        "3": "American Indian",
        "4": "Asian or Pacific Islander",
    },
)

add(
    "death",
    "race_recode_6",
    "2022(1)2024",
    {
        "1": "White only",
        "2": "Black only",
        "3": "American Indian and Alaska Native only",
        "4": "Asian only",
        "5": "Native Hawaiian or Other Pacific Islander only",
        "6": "More than one race",
    },
)

add(
    "death",
    "race_recode_40",
    "2019(1)2024",
    {
        "01": "White",
        "02": "Black",
        "03": "American Indian and Alaska Native",
        "04": "Asian Indian",
        "05": "Chinese",
        "06": "Filipino",
        "07": "Japanese",
        "08": "Korean",
        "09": "Vietnamese",
        "10": "Other or multiple Asian",
        "11": "Hawaiian",
        "12": "Guamanian or Chamorro",
        "13": "Samoan",
        "14": "Other or multiple Pacific Islander",
        "15": "Black and White",
        "16": "Black and American Indian or Alaska Native",
        "17": "Black and Asian",
        "18": "Black and Native Hawaiian or Other Pacific Islander",
        "19": "American Indian or Alaska Native and White",
        "20": "American Indian or Alaska Native and Asian",
        "21": "American Indian or Alaska Native and Native Hawaiian or Other Pacific Islander",
        "22": "Asian and White",
        "23": "Asian and Native Hawaiian or Other Pacific Islander",
        "24": "Native Hawaiian or Other Pacific Islander and White",
        "25": "Black, American Indian or Alaska Native and White",
        "26": "Black, American Indian or Alaska Native and Asian",
        "27": "Black, American Indian or Alaska Native and Native Hawaiian or Other Pacific Islander",
        "28": "Black, Asian and White",
        "29": "Black, Asian and Native Hawaiian or Other Pacific Islander",
        "30": "Black, Native Hawaiian or Other Pacific Islander and White",
        "31": "American Indian or Alaska Native, Asian and White",
        "32": "American Indian or Alaska Native, Native Hawaiian or Other Pacific Islander and White",
        "33": "American Indian or Alaska Native, Asian and Native Hawaiian or Other Pacific Islander",
        "34": "Asian, Native Hawaiian or Other Pacific Islander and White",
        "35": "Black, American Indian or Alaska Native, Asian and White",
        "36": "Black, American Indian or Alaska Native, Asian and Native Hawaiian or Other Pacific Islander",
        "37": "Black, American Indian or Alaska Native, Native Hawaiian or Other Pacific Islander and White",
        "38": "Black, Asian, Native Hawaiian or Other Pacific Islander and White",
        "39": "American Indian or Alaska Native, Asian, Native Hawaiian or Other Pacific Islander and White",
        "40": "Black, American Indian or Alaska Native, Asian, Native Hawaiian or Other Pacific Islander and White",
    },
)

add(
    "death",
    "icd_revision",
    "1968(1)2024",
    {
        "8": "ICD-8, in force for deaths occurring 1968 to 1978",
        "9": "ICD-9, in force for deaths occurring 1979 to 1998",
        "10": "ICD-10, in force for deaths occurring 1999 onward",
    },
)


def main():
    rows = []
    for (table, column), by_cov in D.items():
        for coverage, mapping in by_cov.items():
            for code, label in mapping.items():
                rows.append([table, column, code, coverage, label])
    rows.sort(key=lambda r: (r[0], r[1], r[3], r[2]))
    OUT.parent.mkdir(parents=True, exist_ok=True)
    with open(OUT, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(
            [
                "id_tabela",
                "nome_coluna",
                "chave",
                "cobertura_temporal",
                "valor",
            ]
        )
        w.writerows(rows)
    cols = sorted({(r[0], r[1]) for r in rows})
    print(f"{len(rows)} dictionary rows across {len(cols)} columns -> {OUT}")
    for t, c in cols:
        print(f"   {t:6s} {c}")


if __name__ == "__main__":
    main()
