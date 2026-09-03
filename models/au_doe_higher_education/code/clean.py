"""One-shot onboarding build for au_doe_higher_education.

Reads the downloaded source workbooks and writes one partitioned Parquet
dataset per table. The transform itself lives in
``pipelines/datasets/au_doe_higher_education/utils.py`` so the recurring flow
and this bootstrap cannot drift apart.

Scratch data lives outside the repo and outside Dropbox:
``~/Downloads/au_doe_higher_education_data/{input,output}``.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

from pipelines.datasets.au_doe_higher_education.utils import (
    build_institution_directory,
    clean_application_offer,
    clean_attrition,
    clean_completion_rate,
    clean_equity_group,
    clean_equity_performance,
    clean_equity_reference_value,
    collect_provider_codes,
    stack_vintages,
)

DATA = Path(
    os.environ.get(
        "AU_DOE_DATA",
        Path.home() / "Downloads" / "au_dese_higher_education_data",
    )
)
INPUT = DATA / "input"
OUTPUT = DATA / "output"

DIMENSIONS = ["year", "institution_id", "state_abbreviation"]

CUBES = {
    "student_enrolment": (
        "enrol",
        {"enrolments": "INT64"},
        [
            *DIMENSIONS,
            "citizenship",
            "commencing",
            "course_level_broad",
            "course_level_detailed",
            "gender",
            "attendance_mode",
            "attendance_type",
            "special_course",
            "field_of_education_primary",
            "field_of_education_secondary",
        ],
    ),
    "student_load": (
        "load",
        {"student_load_eftsl": "FLOAT64"},
        [
            *DIMENSIONS,
            "citizenship",
            "commencing",
            "course_level_broad",
            "course_level_detailed",
            "discipline",
            "gender",
            "liability_status",
        ],
    ),
    "award_course_completion": (
        "compl",
        {"completions": "INT64"},
        [
            *DIMENSIONS,
            "citizenship",
            "course_level_broad",
            "course_level_detailed",
            "gender",
            "attendance_mode",
            "attendance_type",
            "special_course",
            "field_of_education_primary",
            "field_of_education_secondary",
        ],
    ),
    "staff": (
        "staff",
        {"staff_headcount": "INT64", "staff_fte": "FLOAT64"},
        [
            *DIMENSIONS,
            "gender",
            "duties_classification",
            "function",
            "organisational_unit",
            "work_contract",
        ],
    ),
}


def write_partitioned(
    frame: pd.DataFrame, table: str, partition: str = "year"
) -> int:
    """Write hive-partitioned Parquet with every column a string.

    Staging is all-STRING by house convention and the dbt model ``safe_cast``s
    each column to its architecture type. Casting through arrow rather than
    ``astype(str)`` matters twice over: ``astype(str)`` renders null as the
    literal "nan", which ``safe_cast`` will not turn back into NULL, and it
    would render an Int64 year as "2024.0".
    """
    target = OUTPUT / table
    frame = frame.copy()
    for column in frame.columns:
        if column == partition:
            continue
        values = frame[column]
        if (
            pd.api.types.is_float_dtype(values)
            or str(values.dtype) == "Float64"
        ):
            frame[column] = values.map(
                lambda value: None if pd.isna(value) else repr(float(value))
            )
        elif str(values.dtype) in ("Int64", "int64"):
            frame[column] = values.map(
                lambda value: None if pd.isna(value) else str(int(value))
            )
        else:
            frame[column] = values.astype("object").where(values.notna(), None)

    schema = pa.schema(
        [
            (name, pa.int64() if name == partition else pa.string())
            for name in frame.columns
        ]
    )
    table_arrow = pa.Table.from_pandas(
        frame, schema=schema, preserve_index=False
    )
    pq.write_to_dataset(
        table_arrow,
        root_path=str(target),
        partition_cols=[partition],
        compression="snappy",
        existing_data_behavior="delete_matching",
    )
    return len(frame)


def main() -> None:
    OUTPUT.mkdir(parents=True, exist_ok=True)
    built: dict[str, pd.DataFrame] = {}

    for table, (prefix, measures, dimensions) in CUBES.items():
        paths = sorted(INPUT.glob(f"{prefix}_v*.xlsx"))
        built[table] = stack_vintages(paths, measures, dimensions)
        print(f"{table:36} {len(built[table]):>8,} rows")

    built["student_equity_group"] = clean_equity_group(
        INPUT / "sec11_equity_2024.xlsx"
    )
    built["student_equity_performance"] = clean_equity_performance(
        INPUT / "sec16_equityperf_2024.xlsx"
    )
    built["equity_reference_value"] = clean_equity_reference_value(
        INPUT / "sec16_equityperf_2024.xlsx"
    )
    built["student_attrition_retention_success"] = clean_attrition(
        INPUT / "sec15_attrition_2024.xlsx"
    )
    built["student_completion_rate"] = clean_completion_rate(
        INPUT / "sec17_complrate_2024.xlsx"
    )
    built["application_offer"] = clean_application_offer(
        INPUT / "uao_2025_appendices.xlsx", INPUT / "uao_2021_appendices.xlsx"
    )

    observed = pd.concat(
        [
            frame[["institution_id", "state_abbreviation"]]
            for frame in built.values()
            if "institution_id" in frame.columns
        ]
    ).dropna(subset=["institution_id"])
    observed = observed.drop_duplicates("institution_id")
    observed["institution_name"] = (
        observed["institution_id"].str.replace("_", " ").str.title()
    )

    codes: dict[str, str] = {}
    codes.update(
        collect_provider_codes(
            INPUT / "sec15_attrition_2024.xlsx",
            [f"15.{n}" for n in range(1, 10)],
            1,
        )
    )
    codes.update(
        collect_provider_codes(
            INPUT / "sec16_equityperf_2024.xlsx",
            [f"16.{n}" for n in range(1, 14)],
            1,
        )
    )
    directory = build_institution_directory(
        INPUT / "inst_list_2020.xls", observed, codes
    )

    missing = set(observed["institution_id"]) - set(
        directory["id_higher_education_institution"]
    )
    if missing:
        raise SystemExit(
            f"institutions missing from the directory: {sorted(missing)}"
        )

    for table, frame in built.items():
        partition = (
            "cohort_start_year"
            if table == "student_completion_rate"
            else "year"
        )
        rows = write_partitioned(frame, table, partition)
        print(f"wrote {table:36} {rows:>8,} rows")

    directory.to_parquet(OUTPUT / "higher_education_institution.parquet")
    print(
        f"wrote {'higher_education_institution':36} {len(directory):>8,} rows"
    )
    print(
        f"provider codes resolved: {directory['provider_code'].notna().sum()}"
    )


if __name__ == "__main__":
    main()
