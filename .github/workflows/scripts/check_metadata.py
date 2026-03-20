from argparse import ArgumentParser
from pathlib import Path
from typing import Any

import basedosdados as bd
import pandas as pd
from databasers_utils import get_architecture_table_from_api


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
    modified_files: list[Path] = [Path(file) for file in modified_files]
    # Get SQL files
    sql_files: list[Path] = [
        file for file in modified_files if file.suffix == ".sql"
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


def evaluate_row(row: pd.Series) -> dict:
    """
    Evaluate a merged row from BigQuery vs API and return column status.
    """
    status = []

    if row["_merge"] == "left_only":
        status.append("Column not found in API")
    elif row["_merge"] == "right_only":
        status.append("Column not found in BigQuery")
    else:
        bq_type = str(row["data_type"]).lower()
        api_type = str(row["bigquery_type"]).lower()
        if bq_type != api_type:
            status.append(f"Type differs (BQ: {bq_type} | API: {api_type})")

        bq_desc = str(row.get("description_bq", "")).strip()
        api_desc = str(row.get("description_api", "")).strip()
        if bq_desc != api_desc:
            status.append(
                f"Description differs (BQ: {bq_desc} | API: {api_desc})"
            )

    return {
        "column_name": row.get("column_name", row.get("name", "")),
        "status": "; ".join(status) if status else "OK",
    }


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


def validate_table_metadata(dataset: str, table_name: str) -> pd.DataFrame:
    """
    Validate metadata of a single table.
    """
    df_merged = merge_metadata(dataset, table_name)
    results = [evaluate_row(row) for _, row in df_merged.iterrows()]

    df_results = pd.DataFrame(results)
    df_results["dataset"] = dataset
    df_results["table"] = table_name

    return df_results


def raise_if_metadata_errors(df_results: pd.DataFrame):
    """
    Check validation results and raise an Exception with detailed info if errors exist.
    """
    df_errors = df_results[df_results["status"] != "OK"]

    if not df_errors.empty:
        error_lines = [
            f"Dataset: {row['dataset']}, Table: {row['table']}, "
            f"Column: {row['column_name']}, Issue: {row['status']}"
            for _, row in df_errors.iterrows()
        ]
        error_message = "⚠️ Metadata discrepancies found:\n" + "\n".join(
            error_lines
        )
        raise Exception(error_message)
    else:
        print("✅ All tables are consistent with the API.")


def check_all_metadata_errors(tables_to_validate: list[tuple[str, str, Any]]):
    """
    Validate all tables and raise exception with detailed errors if any.
    """

    all_results = [
        validate_table_metadata(dataset_id, table_id)
        for dataset_id, table_id, _ in tables_to_validate
    ]

    df_all = pd.concat(all_results, ignore_index=True)
    raise_if_metadata_errors(df_all)


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
