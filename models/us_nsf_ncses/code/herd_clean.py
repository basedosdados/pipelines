"""Clean the NCSES HERD public use data files into Data Basis tables.

Source
------
NCSES publishes institution-level public use microdata for the Higher Education
Research and Development (HERD) Survey and for its predecessor, the Survey of
R&D Expenditures at Universities and Colleges (the "Academic R&D" survey), at
https://ncses.nsf.gov/explore-data/microdata/higher-education-research-development

One ZIP per fiscal year (FY1972-FY2024), each holding a CSV and a SAS file, plus
a separate "short form" ZIP per year from FY2012. Every file is already in long
form: one row per (institution, fiscal year, questionnaire item, row, column).

Three source layouts exist and are normalised here:

* FY1972-FY2009 (38 files, 20 columns) keyed on ``fice``; no IPEDS UNITID.
* FY2010-FY2024 standard form (15 files, 23 columns) keyed on ``inst_id``,
  with ``ncses_inst_id`` and ``ipeds_unitid``.
* FY2012-FY2024 short form (13 files, 21 columns); same keys, fewer items.

``inst_id`` and the older ``fice`` are the same NCSES institution code, so the
IPEDS UNITID observed from FY2010 is carried back to the pre-FY2010 rows of the
same institution (see ``build_unitid_map``).

Output
------
Five tables, hive-partitioned by ``year``, written all-STRING (the dbt models
``safe_cast`` each column to its architecture type):

* ``herd_institution``   one row per institution x fiscal year
* ``herd_expenditure``   every monetary questionnaire item, in USD
* ``herd_personnel``     headcount and full-time-equivalent R&D personnel
* ``herd_survey_item``   non-monetary items and capitalisation thresholds
* ``dicionario``         value -> label for every coded column

Run
---
    python models/us_nsf_ncses/code/herd_clean.py

Set ``NCSES_DATA_DIR`` to override the scratch location (default
``~/Downloads/us_nsf_ncses_data``); ``input/`` holds the downloaded ZIPs and
``output/`` receives the parquet.
"""

from __future__ import annotations

import csv
import io
import os
import re
import sys
import zipfile
from collections import defaultdict
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

DATA_DIR = Path(
    os.environ.get(
        "NCSES_DATA_DIR", os.path.expanduser("~/Downloads/us_nsf_ncses_data")
    )
)
INPUT_DIR = DATA_DIR / "input"
OUTPUT_DIR = DATA_DIR / "output"

# NCSES reports every expenditure item in thousands of current dollars.
THOUSANDS = 1000

FILE_RE = re.compile(r"^herd_(\d{4})(_short)?\.zip$")

# Questionnaire items that are not expenditures. Everything else is monetary.
PERSONNEL_ITEMS = {
    "15": "headcount",  # Headcount of personnel (FY2022+)
    "16": "full_time_equivalent",  # FTEs (FY2022+)
    "NA_02": "headcount",  # Personnel (FY2010-FY2019)
    "NA_03": "headcount",  # Postdocs (FY2010-FY2015)
}
SURVEY_ITEMS = {
    "01.1",  # Inclusion of institution funds (response code)
    "05.1",  # Inclusion of clinical trials in the FY2009 report (FY2010 only)
    "13",  # Capitalisation thresholds (a dollar amount, not an expenditure)
}
# Items in SURVEY_ITEMS whose value is a dollar amount rather than a code.
SURVEY_ITEM_AMOUNTS = {"13"}


def read_rows(zip_path: Path):
    """Yield dict rows from the single CSV inside a HERD public use ZIP.

    Five of the files are cp1252 rather than UTF-8; both are tried in turn.
    """
    with zipfile.ZipFile(zip_path) as zf:
        names = [n for n in zf.namelist() if n.lower().endswith(".csv")]
        if len(names) != 1:
            raise RuntimeError(
                f"{zip_path.name}: expected one CSV, got {names}"
            )
        raw = zf.read(names[0])
    for encoding in ("utf-8-sig", "cp1252"):
        try:
            text = raw.decode(encoding)
            break
        except UnicodeDecodeError:
            continue
    else:
        raise RuntimeError(f"{zip_path.name}: undecodable")
    yield from csv.DictReader(io.StringIO(text))


def clean(value) -> str:
    """Strip a source field, mapping the missing markers to the empty string."""
    if value is None:
        return ""
    value = value.strip()
    # '??' state and '?????' ZIP mark aggregations of institutions, not values.
    if value in {"??", "?????"}:
        return ""
    return value


def normalise_status(value) -> str:
    """Lowercase the status code; FY1972-FY2009 mixes 'I'/'i' and 'E'/'e'."""
    return clean(value).lower()


def to_usd(value) -> str:
    """Convert a reported value in thousands of dollars to dollars."""
    value = clean(value)
    if value == "":
        return ""
    return repr(float(value) * THOUSANDS)


def source_files() -> list[tuple[Path, int, str]]:
    """Return (path, year, survey_form) for every HERD public use ZIP."""
    out = []
    for name in sorted(os.listdir(INPUT_DIR)):
        m = FILE_RE.match(name)
        if not m:
            continue
        year = int(m.group(1))
        form = "short" if m.group(2) else "standard"
        out.append((INPUT_DIR / name, year, form))
    return out


def build_unitid_map(files) -> dict[str, str]:
    """Map NCSES institution id -> IPEDS UNITID, learned from FY2010 onwards.

    The pre-FY2010 files carry no UNITID. ``inst_id`` is stable across the two
    survey eras (Charles R. Drew is ``000166`` in both FY1990 and FY2024), so
    the mapping observed from FY2010 is carried back. An institution that left
    the survey before FY2010 keeps a null UNITID.

    Raises if an institution id ever maps to two different UNITIDs, which would
    make the carry-back unsafe.
    """
    seen: dict[str, set[str]] = defaultdict(set)
    for path, year, _form in files:
        if year < 2010:
            continue
        for row in read_rows(path):
            inst = clean(row.get("inst_id"))
            unitid = clean(row.get("ipeds_unitid"))
            if inst and unitid:
                seen[inst].add(unitid)
    conflicts = {k: v for k, v in seen.items() if len(v) > 1}
    if conflicts:
        raise RuntimeError(
            f"institution id maps to several UNITIDs: {conflicts}"
        )
    return {k: next(iter(v)) for k, v in seen.items()}


def institution_fields(
    row: dict, year: int, form: str, unitid_map: dict
) -> dict:
    """Normalise the institution half of a source row across the three layouts."""
    if year >= 2010:
        inst_id = clean(row["inst_id"])
        return {
            "year": str(year),
            "institution_id": inst_id,
            "ncses_institution_id": clean(row.get("ncses_inst_id")),
            "unitid": clean(row.get("ipeds_unitid"))
            or unitid_map.get(inst_id, ""),
            "combined_institution_id": "",
            "survey_form": form,
            "institution_name": clean(row.get("inst_name_long")),
            "institution_city": clean(row.get("inst_city")),
            "state_abbreviation": clean(row.get("inst_state_code")),
            "zip_code": clean(row.get("inst_zip")),
            "hbcu_indicator": clean(row.get("hbcu_flag")),
            "medical_school_indicator": clean(row.get("med_sch_flag")),
            "high_hispanic_enrollment_indicator": clean(row.get("hhe_flag")),
            "institution_type_code": clean(row.get("toi_code")),
            "highest_degree_code": clean(row.get("hdg_code")),
            "control_type_code": clean(row.get("toc_code")),
            "fy09_pilot_indicator": "",
        }
    inst_id = clean(row["fice"])
    combined = clean(row.get("fice_combined"))
    return {
        "year": str(year),
        "institution_id": inst_id,
        "ncses_institution_id": "",
        "unitid": unitid_map.get(inst_id, ""),
        # "000000" means "not to be combined" — treat it as absent.
        "combined_institution_id": "" if combined == "000000" else combined,
        "survey_form": form,
        "institution_name": clean(row.get("inst_name_long")),
        "institution_city": clean(row.get("inst_city")),
        "state_abbreviation": clean(row.get("inst_state")),
        "zip_code": clean(row.get("inst_zip")),
        "hbcu_indicator": clean(row.get("hbcu_flag")),
        "medical_school_indicator": clean(row.get("has_med_sch_flag")),
        "high_hispanic_enrollment_indicator": clean(row.get("hhe_flag")),
        "institution_type_code": clean(row.get("toi_code")),
        "highest_degree_code": clean(row.get("hdg_code")),
        "control_type_code": clean(row.get("toc_code")),
        "fy09_pilot_indicator": clean(row.get("pilot_fy09_flag")),
    }


INSTITUTION_COLUMNS = [
    "year",
    "institution_id",
    "ncses_institution_id",
    "unitid",
    "combined_institution_id",
    "survey_form",
    "institution_name",
    "institution_city",
    "state_abbreviation",
    "zip_code",
    "hbcu_indicator",
    "medical_school_indicator",
    "high_hispanic_enrollment_indicator",
    "institution_type_code",
    "highest_degree_code",
    "control_type_code",
    "fy09_pilot_indicator",
]

EXPENDITURE_COLUMNS = [
    "year",
    "institution_id",
    "unitid",
    "survey_form",
    "question_code",
    "question",
    "row_label",
    "column_label",
    "expenditure",
    "status_code",
    "other_information",
    "other_information_status_code",
    "standardized_agency_name",
]

PERSONNEL_COLUMNS = [
    "year",
    "institution_id",
    "unitid",
    "survey_form",
    "personnel_group",
    "personnel_function",
    "headcount",
    "headcount_status_code",
    "full_time_equivalent",
    "full_time_equivalent_status_code",
]

SURVEY_ITEM_COLUMNS = [
    "year",
    "institution_id",
    "unitid",
    "survey_form",
    "question_code",
    "question",
    "row_label",
    "column_label",
    "response_code",
    "amount",
    "status_code",
    "other_information",
]

DICIONARIO_COLUMNS = [
    "id_tabela",
    "nome_coluna",
    "chave",
    "cobertura_temporal",
    "valor",
]


def write_partition(
    table: str, year: int, columns: list[str], rows: list[dict]
):
    """Write one year of one table as all-STRING snappy parquet."""
    if not rows:
        return 0
    schema = pa.schema([(c, pa.string()) for c in columns])
    arrays = [
        pa.array(
            [r.get(c) if r.get(c) != "" else None for r in rows],
            type=pa.string(),
        )
        for c in columns
    ]
    out = OUTPUT_DIR / table / f"year={year}"
    out.mkdir(parents=True, exist_ok=True)
    pq.write_table(
        pa.Table.from_arrays(arrays, schema=schema),
        out / "data.parquet",
        compression="snappy",
    )
    return len(rows)


def build_dicionario() -> list[dict]:
    """Value -> label for every coded column, per survey era.

    Codes are taken from the NCSES "Guide for Public Use Data Files" (FY2024).
    The two eras use different code sets for the same concepts, so each entry
    carries its own temporal coverage.
    """
    new = "2010(1)2024"
    old = "1972(1)2009"
    entries: list[tuple[str, str, str, str, str]] = []

    def add(tables, column, coverage, mapping):
        for table in tables:
            for key, label in mapping.items():
                entries.append((table, column, key, coverage, label))

    facts = ["herd_expenditure", "herd_personnel", "herd_survey_item"]
    all_tables = ["herd_institution", *facts]

    add(
        all_tables,
        "survey_form",
        "1972(1)2024",
        {
            "standard": "Standard form questionnaire",
            "short": "Short form questionnaire (institutions under $1 million "
            "in total R&D; FY2012 onwards)",
        },
    )
    add(
        ["herd_institution"],
        "hbcu_indicator",
        new,
        {
            "0": "Not a historically black college or university",
            "1": "Historically black college or university",
        },
    )
    add(
        ["herd_institution"],
        "hbcu_indicator",
        old,
        {
            "F": "Not a historically black college or university",
            "T": "Historically black college or university",
        },
    )
    add(
        ["herd_institution"],
        "medical_school_indicator",
        "1972(1)2024",
        {
            "F": "Does not have a medical school",
            "N": "Null; information was not included",
            "T": "Has a medical school",
        },
    )
    add(
        ["herd_institution"],
        "high_hispanic_enrollment_indicator",
        new,
        {
            "0": "Not a high Hispanic enrollment institution",
            "1": "High Hispanic enrollment institution",
        },
    )
    add(
        ["herd_institution"],
        "high_hispanic_enrollment_indicator",
        old,
        {
            "F": "Not a high Hispanic enrollment institution",
            "N": "Null; information was not included",
            "T": "High Hispanic enrollment institution",
        },
    )
    add(
        ["herd_institution"],
        "institution_type_code",
        "1972(1)2024",
        {"1": "Academic"},
    )
    add(
        ["herd_institution"],
        "highest_degree_code",
        new,
        {
            "1": "Doctorate",
            "2": "Master's",
            "3": "Bachelor's",
            "4": "Associate's",
            "5": "No degree",
            "6": "Professional degree",
        },
    )
    add(
        ["herd_institution"],
        "highest_degree_code",
        old,
        {
            "1": "Doctorate",
            "2": "Master's",
            "3": "Bachelor's",
            "4": "No science and engineering degree (may grant a bachelor's or "
            "higher degree in a non-science program)",
            "8": "Two-year program",
            "9": "No degree assigned; aggregation of institutions",
        },
    )
    add(
        ["herd_institution"],
        "control_type_code",
        "1972(1)2024",
        {
            "1": "Public",
            "2": "Private",
            "?": "No institutional control assigned; aggregation of institutions",
        },
    )
    add(
        ["herd_institution"],
        "fy09_pilot_indicator",
        old,
        {
            "T": "Institution took part in the FY2009 HERD pilot survey",
            "F": "Institution did not take part in the FY2009 HERD pilot survey",
        },
    )
    status = {
        "e": "Estimated by NCSES",
        "i": "Imputed by computer for nonresponse",
        "n": "Data not available",
        "c": "Undocumented code present in the source files",
        "u": "Undocumented code present in the source files",
        "0": "Undocumented code present in the source files",
    }
    for table, column in [
        ("herd_expenditure", "status_code"),
        ("herd_expenditure", "other_information_status_code"),
        ("herd_personnel", "headcount_status_code"),
        ("herd_personnel", "full_time_equivalent_status_code"),
        ("herd_survey_item", "status_code"),
    ]:
        add([table], column, "1972(1)2024", status)
    add(
        ["herd_survey_item"],
        "response_code",
        "2010(1)2024",
        {"-1": "Don't know", "0": "No", "1": "Yes"},
    )
    # SED published tables mix units within one table, so sed_estimate carries
    # the unit per cell. Codes are assigned by models/us_nsf_ncses/code/sed_clean.py.
    add(
        ["sed_estimate"],
        "unit",
        "2024(1)2024",
        {
            "number": "Count of doctorate recipients or institutions",
            "percent": "Percentage, on a 0 to 100 scale",
            "dollars": "Current U.S. dollars",
            "median_years": "Median number of years",
            "median": "Median of the measure named in the column label",
            "mean": "Mean of the measure named in the column label",
        },
    )
    return [dict(zip(DICIONARIO_COLUMNS, e, strict=True)) for e in entries]


def main() -> int:
    files = source_files()
    if not files:
        raise SystemExit(f"no HERD ZIPs under {INPUT_DIR}")
    print(f"{len(files)} source files", flush=True)

    print("building institution id -> UNITID map from FY2010+ ...", flush=True)
    unitid_map = build_unitid_map(files)
    print(f"  {len(unitid_map)} institutions with a UNITID", flush=True)

    files_by_year: dict[int, list[tuple[Path, str]]] = defaultdict(list)
    for path, year, form in files:
        files_by_year[year].append((path, form))

    totals: dict[str, int] = defaultdict(int)

    for year in sorted(files_by_year):
        institutions: dict[str, dict] = {}
        expenditure: list[dict] = []
        personnel: dict[tuple, dict] = {}
        items: list[dict] = []

        for path, form in files_by_year[year]:
            n = 0
            for row in read_rows(path):
                n += 1
                inst = institution_fields(row, year, form, unitid_map)
                key = inst["institution_id"]
                prev = institutions.get(key)
                if prev is None:
                    institutions[key] = inst
                elif prev != inst:
                    raise RuntimeError(
                        f"{path.name}: institution {key} described two ways in {year}"
                    )

                qcode = clean(row.get("questionnaire_no"))
                common = {
                    "year": str(year),
                    "institution_id": key,
                    "unitid": inst["unitid"],
                    "survey_form": form,
                }
                status = normalise_status(row.get("status"))

                if qcode in PERSONNEL_ITEMS:
                    measure = PERSONNEL_ITEMS[qcode]
                    group = (
                        "Postdocs"
                        if qcode == "NA_03"
                        else clean(row.get("row"))
                    )
                    function = clean(row.get("column"))
                    rec = personnel.setdefault(
                        (key, group, function),
                        {
                            **common,
                            "personnel_group": group,
                            "personnel_function": function,
                            "headcount": "",
                            "headcount_status_code": "",
                            "full_time_equivalent": "",
                            "full_time_equivalent_status_code": "",
                        },
                    )
                    rec[measure] = clean(row.get("data"))
                    rec[f"{measure}_status_code"] = status
                elif qcode in SURVEY_ITEMS:
                    value = clean(row.get("data"))
                    is_amount = qcode in SURVEY_ITEM_AMOUNTS
                    items.append(
                        {
                            **common,
                            "question_code": qcode,
                            "question": clean(row.get("question")),
                            "row_label": clean(row.get("row")),
                            "column_label": clean(row.get("column")),
                            "response_code": "" if is_amount else value,
                            "amount": to_usd(value) if is_amount else "",
                            "status_code": status,
                            "other_information": clean(row.get("othinfo")),
                        }
                    )
                else:
                    expenditure.append(
                        {
                            **common,
                            "question_code": qcode,
                            "question": clean(row.get("question")),
                            "row_label": clean(row.get("row")),
                            "column_label": clean(row.get("column")),
                            "expenditure": to_usd(row.get("data")),
                            "status_code": status,
                            "other_information": clean(row.get("othinfo")),
                            "other_information_status_code": normalise_status(
                                row.get("othinfo_s")
                            ),
                            "standardized_agency_name": clean(
                                row.get("standardized_agency_names")
                            ),
                        }
                    )
            totals["source"] += n

        inst_rows = sorted(
            institutions.values(), key=lambda r: r["institution_id"]
        )
        pers_rows = sorted(
            personnel.values(),
            key=lambda r: (
                r["institution_id"],
                r["personnel_group"],
                r["personnel_function"],
            ),
        )
        totals["herd_institution"] += write_partition(
            "herd_institution", year, INSTITUTION_COLUMNS, inst_rows
        )
        totals["herd_expenditure"] += write_partition(
            "herd_expenditure", year, EXPENDITURE_COLUMNS, expenditure
        )
        totals["herd_personnel"] += write_partition(
            "herd_personnel", year, PERSONNEL_COLUMNS, pers_rows
        )
        totals["herd_survey_item"] += write_partition(
            "herd_survey_item", year, SURVEY_ITEM_COLUMNS, items
        )
        print(
            f"  {year}: inst={len(inst_rows):<5} exp={len(expenditure):<7} "
            f"pers={len(pers_rows):<5} item={len(items)}",
            flush=True,
        )

    dicionario = build_dicionario()
    out = OUTPUT_DIR / "dicionario"
    out.mkdir(parents=True, exist_ok=True)
    schema = pa.schema([(c, pa.string()) for c in DICIONARIO_COLUMNS])
    pq.write_table(
        pa.Table.from_arrays(
            [
                pa.array([r[c] for r in dicionario], type=pa.string())
                for c in DICIONARIO_COLUMNS
            ],
            schema=schema,
        ),
        out / "data.parquet",
        compression="snappy",
    )
    totals["dicionario"] = len(dicionario)

    print("\n== row counts ==")
    for name in (
        "source",
        "herd_institution",
        "herd_expenditure",
        "herd_personnel",
        "herd_survey_item",
        "dicionario",
    ):
        print(f"  {name:<20} {totals[name]:>9,}")
    facts = (
        totals["herd_expenditure"]
        + totals["herd_personnel"]
        + totals["herd_survey_item"]
    )
    # Question 15 (headcount) and Question 16 (FTE) share a personnel row, so
    # the fact tables hold fewer rows than the source; nothing else is dropped.
    print(f"  fact rows {facts:,} vs source {totals['source']:,}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
