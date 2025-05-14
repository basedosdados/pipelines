from pathlib import Path
from typing import List, Tuple

from backend import Backend


def expand_alls(
    dataset_id: str, table_id: str, backend: Backend
) -> List[Tuple[str, str]]:
    """
    Expands `__all__` tables into all tables in the dataset.

    Args:
        dataset_id (str): Dataset ID.
        table_id (str): Table ID.
        backend (Backend): Backend class for interacting with the backend.

    Returns:
        List[Tuple[str, str]]: List of tuples with dataset IDs and table IDs.
    """

    if table_id == "__all__":
        table_ids = []
        tables = backend._get_tables_for_dataset(dataset_id)["tables"]
        for table in tables:
            for cloudtable in table["cloudTables"]:
                table_ids.append(cloudtable["gcpTableId"])
    else:
        table_ids = [table_id]
    return [(dataset_id, table_id) for table_id in table_ids]


def get_datasets_tables_from_modified_files(
    modified_files: List[str],
) -> List[Tuple[str, str, bool, bool]]:
    """
    Returns a list of (dataset_id, table_id) from the list of modified files.

    Args:
        modified_files (List[str]): List of modified files.

    Returns:
        Union[List[Tuple[str, str]], List[Tuple[str, str, bool, bool]]]: List of
            tuples with dataset IDs and table IDs. If `show_details` is `True`, then
            the list of tuples will also contain two booleans: the first boolean
            indicates whether the file has been deleted, and the second boolean
            indicates whether the table_id has an alias.
    """
    # Convert to Path
    modified_files: List[Path] = [Path(file) for file in modified_files]
    # Get SQL files
    sql_files: List[Path] = [
        file for file in modified_files if file.suffix == ".sql"
    ]

    datasets_tables: List[Tuple[str, str, bool, bool]] = [
        (file.parent.name, file.stem, file.exists(), False)
        for file in sql_files
    ]

    # Post-process table_id:
    # - Some of `table_id` will have the format `{dataset_id}__{table_id}`. We must
    #   remove the `{dataset_id}__` part.
    new_datasets_tables: List[Tuple[str, str, bool, bool]] = []

    for dataset_id, table_id, exists, _ in datasets_tables:
        alias = False
        crop_str = f"{dataset_id}__"
        if table_id.startswith(crop_str):
            table_id = table_id[len(crop_str) :]
            alias = True
        new_datasets_tables.append((dataset_id, table_id, exists, alias))

    datasets_for_schema_files = [
        Path(file).parent.name
        for file in modified_files
        if file.name.startswith("schema")
        and (file.suffix == ".yaml" or file.suffix == ".yml")
    ]

    for dataset_id in datasets_for_schema_files:
        # Remove dataset_table if schema for this dataset
        for index, new_dataset_table in enumerate(new_datasets_tables):
            if new_dataset_table[0] == dataset_id:
                new_datasets_tables.pop(index)

        for file in (Path("models") / dataset_id).iterdir():
            if file.suffix == ".sql":
                is_table_alias = file.name.startswith(f"{dataset_id}__")
                table_id = file.stem

                if is_table_alias:
                    table_id = table_id.replace(f"{dataset_id}__", "")

                new_datasets_tables.append(
                    (dataset_id, table_id, file.exists(), is_table_alias)
                )

    return new_datasets_tables
