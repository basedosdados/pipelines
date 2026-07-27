import difflib
from argparse import ArgumentParser
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import basedosdados as bd
import pandas as pd
from databasers_utils import get_architecture_table_from_api

_TYPE_ALIASES: dict[str, str] = {
    "boolean": "bool",
    "integer": "int64",
    "int": "int64",
    "float": "float64",
}


def get_datasets_tables_from_modified_files(
    modified_files: list[str],  # type: ignore
) -> list[tuple[str, str, bool, bool]]:
    """
    Returns a list of (dataset_id, table_id) from the list of modified files.

    Args:
        modified_files (list[str]): List of modified files.

    Returns:
        list[tuple[str, str, bool, bool]]: List of tuples with dataset IDs and table IDs.
        List of tuples will also contain two booleans: the first boolean indicates
        whether the file has been deleted, and the second boolean indicates whether
        the table_id has an alias.
    """
    # Convert to Path
    modified_files_paths: list[Path] = [Path(file) for file in modified_files]
    # Get SQL files
    sql_files: list[Path] = [
        file for file in modified_files_paths if file.suffix == ".sql"
    ]

    datasets_tables: list[tuple[str, str, bool, bool]] = [
        (file.parent.name, file.stem, file.exists(), False)
        for file in sql_files
    ]

    # Post-process table_id:
    # - Some of `table_id` will have the format `{dataset_id}__{table_id}`. We must
    #   remove the `{dataset_id}__` part.
    new_datasets_tables: list[tuple[str, str, bool, bool]] = []

    for dataset_id, table_id, exists, _ in datasets_tables:
        alias = False
        crop_str = f"{dataset_id}__"
        if table_id.startswith(crop_str):
            table_id = table_id[len(crop_str) :]
            alias = True
        new_datasets_tables.append((dataset_id, table_id, exists, alias))

    return new_datasets_tables


def get_datasets_and_tables_for_modified_files(
    modified_files: list[str],
) -> list[tuple[str, str, bool]]:
    datasets_tables = get_datasets_tables_from_modified_files(modified_files)

    existing_datasets_tables = []

    for dataset_id, table_id, exists, alias in datasets_tables:
        if exists:
            existing_datasets_tables.append((dataset_id, table_id, alias))

    return existing_datasets_tables


def get_bigquery_columns(
    dataset: str, table: str, billing_project_id: str = "basedosdados"
) -> pd.DataFrame:
    """
    Fetch columns metadata from BigQuery INFORMATION_SCHEMA for a given dataset and table.
    """
    query = f"""
    SELECT
        table_catalog,
        table_schema,
        table_name,
        column_name,
        data_type,
        description
    FROM `basedosdados-dev.{dataset}.INFORMATION_SCHEMA.COLUMN_FIELD_PATHS`
    WHERE table_name = '{table}'
    """

    columns = bd.read_sql(
        query=query, billing_project_id=billing_project_id, from_file=True
    )
    return columns


def normalize_type(t: str) -> str:
    """
    Normalize a BigQuery or API type string to a canonical form for comparison.

    Args:
        t (str): Type string returned by BigQuery or the backend API.

    Returns:
        str: Canonical type string (e.g. "boolean" -> "bool", "int" -> "int64").
    """
    t = t.lower()
    return _TYPE_ALIASES.get(t, t)


@dataclass
class DescriptionError:
    lhs: str
    rhs: str


@dataclass
class TypeError:
    lhs: str
    rhs: str


@dataclass
class NotFoundError:
    message: str


@dataclass
class Evaluate:
    column_name: str
    errors: list[NotFoundError | TypeError | DescriptionError]


def evaluate_row(row: pd.Series) -> Evaluate:
    """
    Evaluate a merged row from BigQuery vs API and return column status.
    """
    errors: list[NotFoundError | TypeError | DescriptionError] = []

    if row["_merge"] == "left_only":
        errors.append(NotFoundError(message="Column not found in API"))
    elif row["_merge"] == "right_only":
        errors.append(NotFoundError(message="Column not found in BigQuery"))
    else:
        bq_type = normalize_type(str(row["data_type"]))
        api_type = normalize_type(str(row["bigquery_type"]))
        if bq_type != api_type:
            errors.append(TypeError(lhs=bq_type, rhs=api_type))

        bq_desc = row.get("description_bq", "")
        api_desc = row.get("description_api", "")

        if bq_desc != api_desc:
            errors.append(
                DescriptionError(
                    lhs=bq_desc if pd.notna(bq_desc) else "",
                    rhs=api_desc if pd.notna(api_desc) else "",
                )
            )

    # pyrefly: ignore [bad-assignment]
    column_name: str = row.get("column_name", row.get("name", ""))
    return Evaluate(column_name=column_name, errors=errors)


def merge_metadata(dataset: str, table_name: str) -> pd.DataFrame:
    """
    Merge BigQuery and API metadata for a given table.
    """
    bq_columns = get_bigquery_columns(dataset, table_name)
    df_bq = bq_columns[
        (bq_columns["table_schema"] == dataset)
        & (bq_columns["table_name"] == table_name)
    ]

    df_api = get_architecture_table_from_api(dataset, table_name)

    df_bq["column_name_norm"] = df_bq["column_name"].str.lower()
    df_api["name_norm"] = df_api["name"].str.lower()

    df_merged = df_bq.merge(
        df_api,
        left_on="column_name_norm",
        right_on="name_norm",
        how="outer",
        indicator=True,
        suffixes=("_bq", "_api"),
    )

    return df_merged


def validate_table_metadata(
    dataset: str, table_name: str
) -> tuple[str, str, list[Evaluate]]:
    """
    Validate metadata of a single table.
    """
    df_merged = merge_metadata(dataset, table_name)
    results = [evaluate_row(row) for _, row in df_merged.iterrows()]

    return (dataset, table_name, results)


RED = "\033[31m"
GREEN = "\033[32m"
RESET = "\033[0m"


def colored_char_diff(lhs: str, rhs: str) -> tuple[str, str]:
    def esc(s):
        return s.replace("\r", "\\r").replace("\n", "\\n")

    sm = difflib.SequenceMatcher(None, lhs, rhs)
    out_lhs, out_rhs = [], []
    for op, i1, i2, j1, j2 in sm.get_opcodes():
        if op == "equal":
            out_lhs.append(esc(lhs[i1:i2]))
            out_rhs.append(esc(rhs[j1:j2]))
        elif op == "replace":
            out_lhs.append(f"{RED}{esc(lhs[i1:i2])}{RESET}")
            out_rhs.append(f"{GREEN}{esc(rhs[j1:j2])}{RESET}")
        elif op == "delete":
            out_lhs.append(f"{RED}{esc(lhs[i1:i2])}{RESET}")
        elif op == "insert":
            out_rhs.append(f"{GREEN}{esc(rhs[j1:j2])}{RESET}")

    return "".join(out_lhs), "".join(out_rhs)


def raise_if_metadata_errors(results: list[tuple[str, str, list[Evaluate]]]):
    """
    Check validation results and raise an Exception with detailed info if errors exist.
    """

    error_exists = False

    for dataset, table_name, evaluated_columns in results:
        has_erros = any(i for i in evaluated_columns if len(i.errors) > 0)
        if has_erros:
            error_exists = True
            print(f"Metadata errros for {dataset}.{table_name}")
            for col in evaluated_columns:
                if len(col.errors) > 0:
                    print(f"  Column `{col.column_name}`")
                    for err in col.errors:
                        match err:
                            case NotFoundError(message):
                                print(f"     Cloumn not found: {message}")
                            case DescriptionError(lhs, rhs):
                                bq, api = colored_char_diff(lhs, rhs)
                                print(f"     BQ: `{bq}`")
                                print(f"    API: `{api}`\n")
                            case TypeError(lhs, rhs):
                                print(f"     BQ: `{lhs}`")
                                print(f"    API: `{rhs}`\n")

    if error_exists:
        print("⚠️ Metadata discrepancies found. See the output")
        exit(1)
    else:
        print("✅ All tables are consistent with the API.")
        exit(0)


def check_all_metadata_errors(tables_to_validate: list[tuple[str, str, Any]]):
    """
    Validate all tables and raise exception with detailed errors if any.
    """

    all_results = [
        validate_table_metadata(dataset_id, table_id)
        for dataset_id, table_id, _ in tables_to_validate
    ]

    raise_if_metadata_errors(all_results)


# --- Main execution ---
if __name__ == "__main__":
    parser = ArgumentParser()
    parser.add_argument(
        "--modified-files",
        type=str,
        required=False,
        help="Comma-separated list of modified files.",
    )

    args = parser.parse_args()

    modified_files = (
        [] if args.modified_files is None else args.modified_files.split(" ")
    )
    tables_to_validate = get_datasets_and_tables_for_modified_files(
        modified_files
    )

    check_all_metadata_errors(tables_to_validate)
