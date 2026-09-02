#!/usr/bin/env python3
"""
Build the Data Basis architecture tables for us_ed_college_scorecard.

Writes, under code/architecture/:

    <table>.csv     the 10-column Data Basis architecture sheet (description
                    in Portuguese, the language the site renders)
    i18n.csv        table, column, description_pt/en/es -- consumed at
                    metadata-registration time by bulk_upsert_columns, which
                    is the only writer that sets all three languages
    ROUTING.md      where every one of the 3,486 published variables ended up

Column types follow the house rule "type by arithmetic meaning": a value is
INT64/FLOAT64 only when summing or averaging it is meaningful and it has a
nameable unit. Identifiers, FIPS/geographic codes, categorical codes and
0/1 flags are STRING even though they are stored as digits.

Usage:
    /tmp/cs_venv/bin/python models/us_ed_college_scorecard/code/build_architecture.py
"""

import csv
import logging
import os
import pathlib
import re
import sys

# pyrefly: ignore [untyped-import]
import yaml

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))
# pyrefly: ignore [missing-import]
import i18n

# pyrefly: ignore [missing-import]
import spec

DATA_DIR = pathlib.Path(
    os.environ.get(
        "SCORECARD_DATA_DIR",
        pathlib.Path.home() / "Downloads/us_ed_college_scorecard_data",
    )
)
RAW_DIR = DATA_DIR / "input" / "raw"
ARCH_DIR = pathlib.Path(__file__).resolve().parent / "architecture"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("arch")

ARCH_COLUMNS = [
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

TIME_DIRECTORY = "br_bd_diretorios_data_tempo.ano:ano"

# The house US directory already types UNITID as STRING, so the STRING choice
# here matches it; only the older us_ed_ipeds dataset types the key as INT64.
DIRECTORY = {
    "unitid": "br_bd_diretorios_us.higher_education_institution:id_institution",
    "state_abbreviation": "br_bd_diretorios_us.state:abbreviation",
    "state_fips": "br_bd_diretorios_us.state:id_state",
}

# Institution columns whose value set is documented in the `dicionario` table.
DICT_COVERED = {
    "control",
    "control_peps",
    "ownership_peps",
    "scorecard_sector",
    "institution_level",
    "predominant_degree",
    "predominant_degree_recoded",
    "highest_degree",
    "main_campus",
    "carnegie_basic",
    "carnegie_undergraduate_profile",
    "carnegie_size_setting",
    "online_only",
    "currently_operating",
    "open_admissions_policy",
    "religious_affiliation",
    "men_only",
    "women_only",
    "historically_black",
    "predominantly_black",
    "alaska_native_hawaiian_serving",
    "tribal_college",
    "asian_pacific_islander_serving",
    "hispanic_serving",
    "native_american_non_tribal",
    "title_iv_eligibility_type",
    "heightened_cash_monitoring",
    "dol_provider",
    "region",
    "locale",
    "locale_degree_urbanization",
    "test_score_requirement",
}

# (type, unit) for the wide institution columns that are genuine quantities.
INSTITUTION_QUANTITIES = {
    "latitude": ("FLOAT64", "degrees"),
    "longitude": ("FLOAT64", "degrees"),
    "branch_campuses": ("INT64", "unit"),
    "undergraduate_enrollment": ("INT64", "person"),
    "undergraduate_enrollment_all": ("INT64", "person"),
    "tuition_revenue_per_fte": ("INT64", "USD"),
    "instructional_expenditure_per_fte": ("INT64", "USD"),
    "average_faculty_salary": ("INT64", "USD"),
    "endowment_begin": ("INT64", "USD"),
    "endowment_end": ("INT64", "USD"),
    "full_time_faculty_rate": ("FLOAT64", "ratio"),
    "admission_rate": ("FLOAT64", "ratio"),
    "admission_rate_all_campuses": ("FLOAT64", "ratio"),
    "admission_rate_suppressed": ("FLOAT64", "ratio"),
    "sat_average": ("FLOAT64", "score_point"),
    "sat_average_all_campuses": ("FLOAT64", "score_point"),
}

FOS_UNITID_NOTE_PT = (
    "Nulo em 58.103 linhas (3,3%): a fonte também publica linhas agregadas por "
    "entidade, identificadas apenas por opeid6 e com UNITID igual a NA. Essas linhas "
    "trazem dados reais e por isso foram mantidas; a chave da tabela inclui opeid6"
)

SUPPRESSION_NOTE_PT = (
    "Células suprimidas por sigilo (PrivacySuppressed) foram convertidas em nulo; "
    "nesta tabela larga não é possível distingui-las de células nunca coletadas"
)

BAND_NOTE_PT = (
    "A fonte publica esta coluna ora como proporção, ora como intervalo de "
    "arredondamento (ex.: 0.30-0.39, <=0.10) para evitar reidentificação; por isso o "
    "tipo é STRING. Nos arquivos de 2015-2022, 64% dos valores publicados são intervalos"
)


def load_dictionary():
    d = yaml.safe_load((RAW_DIR / "data.yaml").read_text())
    by_source = {}
    for api_name, entry in d["dictionary"].items():
        if isinstance(entry, dict) and entry.get("source"):
            by_source[entry["source"].upper()] = {
                "api_name": api_name,
                **entry,
            }
    return by_source, {f["name"]: f["key"] for f in d["files"]}


def header(path):
    with open(path, newline="") as fh:
        return next(csv.reader(fh))


def row(
    name,
    bq_type,
    desc_pt,
    *,
    dictionary="no",
    directory="",
    unit="",
    observations="",
    original="",
):
    return {
        "name": name,
        "bigquery_type": bq_type,
        "description": desc_pt,
        "temporal_coverage": "",
        "covered_by_dictionary": dictionary,
        "directory_column": directory,
        "measurement_unit": unit,
        "has_sensitive_data": "no",
        "observations": observations,
        "original_name": original,
    }


# ------------------------------------------------------------ per-table CSV


def build_institution(by_source):
    rows, i18n_rows = [], []
    for name, raw in spec.INSTITUTION_COLUMNS:
        pt, en, es = i18n.INSTITUTION[name]
        if name == "year":
            rows.append(
                row(
                    name,
                    "INT64",
                    pt,
                    directory=TIME_DIRECTORY,
                    unit="year",
                    observations="Coluna de particionamento",
                )
            )
        elif name == "unitid":
            rows.append(
                row(
                    name,
                    "STRING",
                    pt,
                    original=raw,
                    directory=DIRECTORY["unitid"],
                    observations="Identificador; a aritmética não faz sentido, por isso STRING",
                )
            )
        elif name == "title_iv_approval_date":
            rows.append(
                row(
                    name,
                    "DATE",
                    pt,
                    original=raw,
                    observations="Convertida de MM/DD/AAAA para ISO na limpeza",
                )
            )
        elif name in INSTITUTION_QUANTITIES:
            bq, unit = INSTITUTION_QUANTITIES[name]
            rows.append(row(name, bq, pt, unit=unit, original=raw))
        else:
            rows.append(
                row(
                    name,
                    "STRING",
                    pt,
                    original=raw,
                    directory=DIRECTORY.get(name, ""),
                    dictionary="yes" if name in DICT_COVERED else "no",
                    observations=SUPPRESSION_NOTE_PT
                    if name in DICT_COVERED
                    else "",
                )
            )
        i18n_rows.append(("institution", name, pt, en, es))
    return rows, i18n_rows


def build_long(table):
    rows, i18n_rows = [], []
    for name, (pt, en, es) in i18n.LONG_COLUMNS:
        if name == "year":
            rows.append(
                row(
                    name,
                    "INT64",
                    pt,
                    directory=TIME_DIRECTORY,
                    unit="year",
                    observations="Coluna de particionamento",
                )
            )
        elif name == "unitid":
            rows.append(
                row(
                    name,
                    "STRING",
                    pt,
                    original="UNITID",
                    directory=DIRECTORY["unitid"],
                )
            )
        elif name == "value":
            rows.append(
                row(
                    name,
                    "FLOAT64",
                    pt,
                    unit="",
                    observations="A unidade varia conforme a variável; ver a coluna "
                    "measurement_unit da tabela variable",
                )
            )
        else:
            rows.append(row(name, "STRING", pt))
        i18n_rows.append((table, name, pt, en, es))
    return rows, i18n_rows


def fos_type_and_unit(bd_name, entry):
    """Type a field-of-study column by what its values actually are."""
    upper = bd_name.upper()
    if bd_name in (
        "unitid",
        "opeid6",
        "institution_name",
        "control",
        "main_campus",
        "cip_code",
        "cip_description",
        "credential_level",
        "credential_description",
        "distance_education",
    ):
        return "STRING", ""
    # BBRR rate columns publish interval bands as often as numbers -- see
    # BAND_NOTE_PT. Typed FLOAT64 they would safe_cast to NULL silently.
    if upper.startswith("BBRR") and not upper.endswith("_N"):
        return "STRING", ""
    if upper.startswith("IPEDSCOUNT"):
        return "INT64", "unit"
    if re.search(
        r"(_N|COUNT\d?|CNTOVER150|IPEDSCOUNT\d|_GT_THRESHOLD_\d+YR|"
        r"_HIGH_CRED_\d+YR|_IN_STATE_\d+YR)$",
        upper,
    ):
        return "INT64", "person"
    if "MDN10YRPAY" in upper:
        return "FLOAT64", "USD"
    return "FLOAT64", "USD"


def build_field_of_study(by_source, fos_header):
    rows, i18n_rows = [], []
    pt, en, es = i18n.YEAR
    rows.append(
        row(
            "year",
            "INT64",
            pt,
            directory=TIME_DIRECTORY,
            unit="year",
            observations="Coluna de particionamento; corresponde ao segundo ano "
            "letivo da coorte agrupada publicada pela fonte",
        )
    )
    i18n_rows.append(("field_of_study", "year", pt, en, es))
    for raw in fos_header:
        name = spec.bd_name_field_of_study(raw)
        entry = by_source.get("P_" + raw.upper()) or by_source[raw.upper()]
        pt, en, es = i18n.translate_fos(entry.get("description") or "")
        if name == "unitid":
            # Keep the IPEDS-join note identical in all three languages; the
            # source's own one-line label loses it in English.
            pt, en, es = i18n.UNITID
        bq, unit = fos_type_and_unit(name, entry)
        directory = DIRECTORY.get(name, "")
        note = ""
        if name == "unitid":
            note = FOS_UNITID_NOTE_PT
        elif raw.upper().startswith("BBRR") and not raw.upper().endswith("_N"):
            note = BAND_NOTE_PT
        elif bq != "STRING":
            note = SUPPRESSION_NOTE_PT
        rows.append(
            row(
                name,
                bq,
                pt,
                original=raw.upper(),
                unit=unit,
                directory=directory,
                observations=note,
                dictionary="yes"
                if name
                in (
                    "control",
                    "main_campus",
                    "credential_level",
                    "distance_education",
                )
                else "no",
            )
        )
        i18n_rows.append(("field_of_study", name, pt, en, es))
    return rows, i18n_rows


def build_catalogue(table, columns):
    rows, i18n_rows = [], []
    for name, (pt, en, es) in columns:
        rows.append(row(name, "STRING", pt))
        i18n_rows.append((table, name, pt, en, es))
    return rows, i18n_rows


# ------------------------------------------------------------------ routing


def write_routing(by_source, inst_header, fos_header, file_years):
    counts = {}
    lines = []
    for raw in inst_header:
        table = (
            "institution"
            if raw.upper() in spec.PROMOTED_TO_WIDE
            else (
                "institution"
                if by_source[raw.upper()]["api_name"].split(".")[0]
                in spec.WIDE_NAMESPACES
                else spec.LONG_TABLES[
                    by_source[raw.upper()]["api_name"].split(".")[0]
                ]
            )
        )
        counts[table] = counts.get(table, 0) + 1
        lines.append(
            f"| {raw.upper()} | institution file | {table} | {by_source[raw.upper()]['api_name']} |"
        )
    for raw in fos_header:
        counts["field_of_study"] = counts.get("field_of_study", 0) + 1
        entry = by_source.get("P_" + raw.upper()) or by_source[raw.upper()]
        lines.append(
            f"| {raw.upper()} | field-of-study file | field_of_study | {entry['api_name']} |"
        )

    years = sorted(
        {
            int(v)
            for k, v in file_years.items()
            if not k.startswith("Most-Recent")
        }
    )
    body = (
        f"""# Variable routing — us_ed_college_scorecard

**No published variable was dropped.** All {len(inst_header)} institution-level and
{len(fos_header)} field-of-study columns are loaded. The institution file is normalised
from ~3,300 wide columns into one wide table plus seven long tables, one per
namespace of the source's own API hierarchy, because those columns are
mechanically generated cross-tabs of measure x subgroup x horizon: held wide the
table is unusable and no single measure can be documented.

## Where the columns went

| table | columns from the source |
|---|---:|
"""
        + "\n".join(f"| `{t}` | {n} |" for t, n in sorted(counts.items()))
        + f"""
| `variable` | catalogue of all {len(inst_header) + len(fos_header)} variables |
| `dicionario` | value labels for coded columns |

## Routing rule

A column's table is the first segment of its name in the source's API
hierarchy (`data.yaml`). Namespaces `{"`, `".join(sorted(spec.WIDE_NAMESPACES))}`
are the wide `institution` table; every other namespace becomes the long table
of the same name.

**One documented exception:** `UGDS` and `UG` sit in the `student` namespace but
are loaded into the wide `institution` table as `undergraduate_enrollment` and
`undergraduate_enrollment_all`. Institution size is the most-used filter in the
dataset and an `institution` table that cannot report it is a poor product. Both
remain absent from `student_body` so no value is duplicated.

## Files not loaded

`Most-Recent-Cohorts-Institution.csv` and `Most-Recent-Cohorts-Field-of-Study.csv`
are **excluded by design**. They are not a cohort year: each column carries the
most recent *non-missing* value, drawn from a different year per column (see the
workbook's `Most_Recent_Inst_Cohort_Map` tab). Loading them as a year partition
would silently corrupt the panel. Every value they contain is already present in
the cohort file it came from.

Cohort years loaded: {years[0]}-{years[-1]} ({len(years)} institution files,
8 field-of-study files).

## Full column list

| variable | source file | table | API name |
|---|---|---|---|
"""
        + "\n".join(lines)
        + "\n"
    )
    (ARCH_DIR / "ROUTING.md").write_text(body)
    return counts


def main():
    ARCH_DIR.mkdir(parents=True, exist_ok=True)
    by_source, file_years = load_dictionary()
    inst_header = header(sorted(RAW_DIR.glob("MERGED*_PP.csv"))[-1])
    fos_header = header(sorted(RAW_DIR.glob("FieldOfStudyData*_PP.csv"))[-1])

    tables = {}
    all_i18n = []
    for builder, name in (
        (lambda: build_institution(by_source), "institution"),
        (
            lambda: build_field_of_study(by_source, fos_header),
            "field_of_study",
        ),
        (lambda: build_catalogue("variable", i18n.VARIABLE_TABLE), "variable"),
        (
            lambda: build_catalogue("dicionario", i18n.DICIONARIO_TABLE),
            "dicionario",
        ),
    ):
        rows, i18n_rows = builder()
        tables[name] = rows
        all_i18n += i18n_rows
    for long_table in sorted(set(spec.LONG_TABLES.values())):
        rows, i18n_rows = build_long(long_table)
        tables[long_table] = rows
        all_i18n += i18n_rows

    for name, rows in tables.items():
        with open(ARCH_DIR / f"{name}.csv", "w", newline="") as fh:
            w = csv.DictWriter(
                fh, fieldnames=ARCH_COLUMNS, lineterminator="\n"
            )
            w.writeheader()
            w.writerows(rows)
        log.info("%-16s %3d columns", name, len(rows))

    with open(ARCH_DIR / "i18n.csv", "w", newline="") as fh:
        w = csv.writer(fh, lineterminator="\n")
        w.writerow(
            [
                "table",
                "name",
                "description_pt",
                "description_en",
                "description_es",
            ]
        )
        w.writerows(all_i18n)

    counts = write_routing(by_source, inst_header, fos_header, file_years)
    log.info("routing: %s", counts)
    log.info("total described columns: %d", len(all_i18n))


if __name__ == "__main__":
    main()
