"""Generate the architecture CSVs and backend column payloads for us_dol_oflc.

The architecture CSV is the schema source of truth: one row per column, in
publication order, with the BigQuery type, the English description, dictionary
coverage, measurement unit and free-text observations. ``columns_json/`` holds
the same information shaped for ``mcp__databasis__bulk_upsert_columns``, with
the Portuguese and Spanish descriptions attached.

Usage:
    uv run python models/us_dol_oflc/code/build_architecture.py
"""

from __future__ import annotations

import csv
import json
from pathlib import Path

HERE = Path(__file__).resolve().parent

from pipelines.datasets.us_dol_oflc import canonical_map as cm  # noqa: E402
from pipelines.datasets.us_dol_oflc import descriptions as ds  # noqa: E402

ARCH = HERE / "architecture"
JSON_OUT = HERE / "columns_json"

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

# Columns whose stored values are codes or short tokens resolved by the
# dictionary table. Readable free text (employer names, job titles) is not.
DICTIONARY = {
    "case_status",
    "visa_class",
    "full_time_position",
    "wage_unit_of_pay",
    "prevailing_wage_unit_of_pay",
    "prevailing_wage_level",
    "prevailing_wage_source",
    "agent_representing_employer",
    "h1b_dependent",
    "willful_violator",
    "support_h1b",
    "withdrawn",
    "secondary_entity",
    "application_type",
    "refile",
    "schedule_a_sheepherder",
    "required_experience",
    "is_multiple_worksites",
    "type_of_employer_application",
    "type_of_employer",
    "h2a_labor_contractor",
    "nature_of_temporary_need",
    "emergency_filing",
    "cap_exempt",
    "meals_provided",
    "frequency_of_pay",
    "class_of_admission",
    "minimum_education",
    "education_level",
    "foreign_worker_education",
}

UNITS = {
    "year": "year",
    "wage_offered_from": "usd",
    "wage_offered_to": "usd",
    "wage_offered_from_annual": "usd",
    "wage_offered_to_annual": "usd",
    "prevailing_wage": "usd",
    "prevailing_wage_annual": "usd",
    "overtime_rate_from": "usd",
    "overtime_rate_to": "usd",
    "piece_rate_offer": "usd",
    "total_workers": "person",
    "workers_requested": "person",
    "workers_certified": "person",
    "worksite_workers": "person",
    "new_employment": "person",
    "continued_employment": "person",
    "change_previous_employment": "person",
    "new_concurrent_employment": "person",
    "change_employer": "person",
    "amended_petition": "person",
    "employer_num_employees": "person",
    "housing_total_occupancy": "person",
    "employer_year_commenced_business": "year",
    "required_experience_months": "month",
    "work_experience_months": "month",
    "anticipated_number_of_hours": "hour",
    "total_worksite_locations": "unit",
    "total_worksite_records": "unit",
}
# "unit" is not a Data Basis measurement unit; those two are plain counts of
# records with no physical unit, so they carry none.
UNITS = {k: v for k, v in UNITS.items() if v != "unit"}

DIRECTORY = {
    "year": "diretorios_data_tempo.ano:ano",
}

OBSERVATIONS = {
    "year": "Partition column. Fiscal year, 1 October to 30 September, taken from the source file rather than from any date in the row",
    "case_number": "Case number formats differ across the eFile, iCERT and FLAG systems; uniqueness is guaranteed only within a fiscal year",
    "soc_id": "SOC vintage follows the year of filing (SOC 2000, 2010 or 2018), so codes are not comparable across the whole series and are not linked to a directory",
    "naics_id": "NAICS vintage follows the year of filing and code length varies from 2 to 6 digits, so codes are not linked to a directory",
    "swa_state": "USPS abbreviation. Normalised by Data Basis — the source writes full state names in some years and abbreviations in others",
    "employer_state": "USPS abbreviation, normalised by Data Basis — the source writes full state names in some years and abbreviations in others. Not linked to the US state directory, which is keyed on the FIPS code; referential coverage is checked by a dbt test instead",
    "worksite_state": "USPS abbreviation, normalised by Data Basis — the source writes full state names in some years and abbreviations in others. Not linked to the US state directory, which is keyed on the FIPS code; referential coverage is checked by a dbt test instead",
    "housing_state": "USPS abbreviation, normalised by Data Basis in the same way as worksite_state",
    "wage_offered_from_annual": "Derived by Data Basis: the offered wage multiplied by 2080 for hourly, 260 for daily, 52 for weekly, 26 for bi-weekly, 24 for semi-monthly and 12 for monthly pay. Null when the unit is absent or is a piece rate",
    "wage_offered_to_annual": "Derived by Data Basis using the same factors as wage_offered_from_annual",
    "prevailing_wage_annual": "Derived by Data Basis using the same factors as wage_offered_from_annual",
    "wage_unit_of_pay": "Harmonised by Data Basis from vocabularies that differ by year and program (Hour, HR, Hourly all become hour)",
    "prevailing_wage_unit_of_pay": "Harmonised by Data Basis in the same way as wage_unit_of_pay",
    "worksite_city": "Primary worksite only. Applications covering several worksites list the rest in the companion Appendix A and Addendum B files, which are not part of this table",
    "total_worksite_locations": "Count of worksite locations. Left without a measurement unit because the Data Basis unit vocabulary has no entry for a worksite; the value is a plain count",
    "total_worksite_records": "Count of worksite records attached to the application. Left without a measurement unit for the same reason as total_worksite_locations",
    "source_file": "Provenance. Names the Department of Labor workbook the row came from, which identifies the form version behind the row",
}

PROGRAM_OBS = {
    (
        "lca",
        "wage_unit_of_pay",
    ): "Absent from the FY2010 source file, so every FY2010 annualised wage is null",
    (
        "lca",
        "case_number",
    ): "From FY2020 the source publishes one file per quarter; a fiscal year is the union of its quarterly files",
    (
        "perm",
        "prevailing_wage",
    ): "Not published from FY2025, when the redesigned ETA-9089 replaced the wage amount with a reference to the prevailing wage determination case number",
    (
        "h2b",
        "prevailing_wage",
    ): "Not published from FY2016; only the determination case number survives in prevailing_wage_tracking_number",
}


def observation(name: str, program: str) -> str:
    return PROGRAM_OBS.get((program, name), OBSERVATIONS.get(name, ""))


def main() -> int:
    ARCH.mkdir(parents=True, exist_ok=True)
    JSON_OUT.mkdir(parents=True, exist_ok=True)

    for program in ["lca", "perm", "h2a", "h2b"]:
        rows, payload = [], []
        for name, btype in cm.columns(program):
            en, pt, es = ds.get(name, program)
            covered = "yes" if name in DICTIONARY else "no"
            rows.append(
                {
                    "name": name,
                    "bigquery_type": btype,
                    "description": en,
                    "temporal_coverage": "",
                    "covered_by_dictionary": covered,
                    "directory_column": DIRECTORY.get(name, ""),
                    "measurement_unit": UNITS.get(name, ""),
                    "has_sensitive_data": "no",
                    "observations": observation(name, program),
                    "original_name": "",
                }
            )
            entry = {
                "name": name,
                "bigquery_type": btype,
                "description_pt": pt,
                "description_en": en,
                "description_es": es,
                "covered_by_dictionary": name in DICTIONARY,
                "has_sensitive_data": False,
            }
            if DIRECTORY.get(name):
                entry["directory_column"] = DIRECTORY[name]
            if UNITS.get(name):
                entry["measurement_unit"] = UNITS[name]
            payload.append(entry)
        with open(ARCH / f"{program}.csv", "w", newline="") as fh:
            w = csv.DictWriter(fh, fieldnames=FIELDS, lineterminator="\n")
            w.writeheader()
            w.writerows(rows)
        (JSON_OUT / f"{program}.json").write_text(
            json.dumps(payload, ensure_ascii=False, indent=2) + "\n"
        )
        print(f"{program}: {len(rows)} columns")

    # dictionary table
    dict_cols = [
        (
            "table_id",
            "STRING",
            "Slug of the us_dol_oflc table the entry describes",
            "Slug da tabela us_dol_oflc que a entrada descreve",
            "Slug de la tabla us_dol_oflc que describe la entrada",
        ),
        (
            "column_name",
            "STRING",
            "Name of the column covered by the dictionary",
            "Nome da coluna coberta pelo dicionário",
            "Nombre de la columna cubierta por el diccionario",
        ),
        (
            "key",
            "STRING",
            "Coded value exactly as stored in the data table",
            "Valor codificado exatamente como armazenado na tabela de dados",
            "Valor codificado exactamente como está almacenado en la tabla de datos",
        ),
        (
            "temporal_coverage",
            "STRING",
            "Fiscal years the mapping applies to",
            "Anos fiscais aos quais o mapeamento se aplica",
            "Años fiscales a los que se aplica el mapeo",
        ),
        (
            "value",
            "STRING",
            "Meaning of the coded value",
            "Significado do valor codificado",
            "Significado del valor codificado",
        ),
    ]
    with open(ARCH / "dictionary.csv", "w", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=FIELDS, lineterminator="\n")
        w.writeheader()
        for name, btype, en, _pt, _es in dict_cols:
            w.writerow(
                {
                    "name": name,
                    "bigquery_type": btype,
                    "description": en,
                    "temporal_coverage": "",
                    "covered_by_dictionary": "no",
                    "directory_column": "",
                    "measurement_unit": "",
                    "has_sensitive_data": "no",
                    "observations": "",
                    "original_name": "",
                }
            )
    (JSON_OUT / "dictionary.json").write_text(
        json.dumps(
            [
                {
                    "name": n,
                    "bigquery_type": t,
                    "description_pt": pt,
                    "description_en": en,
                    "description_es": es,
                    "covered_by_dictionary": False,
                    "has_sensitive_data": False,
                }
                for n, t, en, pt, es in dict_cols
            ],
            ensure_ascii=False,
            indent=2,
        )
        + "\n"
    )
    print("dictionary: 5 columns")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
