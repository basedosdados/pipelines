# -*- coding: utf-8 -*-
import shutil
import sys
import traceback
from argparse import ArgumentParser
from pathlib import Path

from basedosdados import Backend, Dataset, Storage, Table, read_sql
from dbt.cli.main import dbtRunner


def expand_alls(dataset_id: str, table_id: str, backend: Backend) -> list[str]:
    """
    Expands `__all__` tables into all tables in the dataset.

    Args:
        dataset_id (str): Dataset ID.
        table_id (str): Table ID.
        backend (Backend): Backend class for interacting with the backend.

    Returns:
        list[str]: List of table IDs.
    """

    if table_id == "__all__":
        dataset_id_from_name = backend._get_dataset_id_from_name(dataset_id)
        query = """
            query ($dataset_id: ID!) {
                allDataset (id: $dataset_id) {
                    edges {
                        node {
                            tables {
                                edges {
                                    node {
                                        _id,
                                        slug,
                                        cloudTables {
                                            edges {
                                                node {
                                                    gcpTableId
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        """
        variables = {"dataset_id": dataset_id_from_name}
        tables = backend._execute_query(query, variables=variables)[  # type: ignore
            "allDataset"
        ]["items"][0]["tables"]["items"]
        return [i["cloudTables"]["items"][0]["gcpTableId"] for i in tables]

    else:
        return [table_id]


def get_datasets_tables_from_modified_files(
    modified_files: list[str],
) -> list[tuple[str, str, bool, bool]]:
    """
    Returns a list of (dataset_id, table_id) from the list of modified files.

    Args:
        modified_files (list[str]): List of modified files.

    Returns:
        list[tuple[str, str, bool, bool]]: List of tuples with dataset IDs and
        table IDs. The list of tuples will also contain two booleans: the first
        boolean indicates whether the file has been deleted, and the second
        boolean indicates whether the table_id has an alias.
    """

    # Convert to Path
    modified_files_path = [Path(file) for file in modified_files]

    # Get SQL files
    sql_files = [file for file in modified_files_path if file.suffix == ".sql"]

    datasets_tables = [
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

    datasets_tables = new_datasets_tables

    return datasets_tables


def sync_bucket(
    source_bucket_name: str,
    dataset_id: str,
    table_id: str,
    destination_bucket_name: str,
    backup_bucket_name: str,
    mode: str = "staging",
) -> bool:
    """Copies proprosed data between storage buckets.
    Creates a backup of old data, then delete it and copies new data into the destination bucket.

    Args:
        source_bucket_name (str):
            The bucket name from which to copy data.
        dataset_id (str):
            Dataset id available in basedosdados. It should always come with table_id.
        table_id (str):
            Table id available in basedosdados.dataset_id.
            It should always come with dataset_id.
        destination_bucket_name (str):
            The bucket name which data will be copied to.
            If None, defaults to the bucket initialized when instantianting Storage object
            (check it with the Storage.bucket proprerty)
        backup_bucket_name (str):
            The bucket name for where backup data will be stored.
        mode (str): Optional
            Folder of which dataset to update.[raw|staging|header|auxiliary_files|architecture]

    Raises:
        ValueError:
            If there are no files corresponding to the given dataset_id and table_id on the source bucket
    """

    ref = Storage(dataset_id=dataset_id, table_id=table_id)

    prefix = f"{mode}/{dataset_id}/{table_id}/"

    source_ref = (
        ref.client["storage_staging"]
        .bucket(source_bucket_name)  # type: ignore
        .list_blobs(prefix=prefix)
    )

    destination_ref = ref.bucket.list_blobs(prefix=prefix)

    if len(list(source_ref)) == 0:
        print(
            f"No objects found on the source bucket {source_bucket_name}.{prefix}"
        )
        return False

    if len(list(destination_ref)):
        backup_bucket_blobs = list(
            ref.client["storage_staging"]
            .bucket(backup_bucket_name)  # type: ignore
            .list_blobs(prefix=prefix)
        )
        if len(backup_bucket_blobs):
            print(f"{mode.upper()}: DELETE BACKUP DATA")
            ref.delete_table(
                not_found_ok=True, mode=mode, bucket_name=backup_bucket_name
            )

        print(f"{mode.upper()}: BACKUP OLD DATA")
        ref.copy_table(
            source_bucket_name=destination_bucket_name,
            destination_bucket_name=backup_bucket_name,
            mode=mode,
        )

        print(f"{mode.upper()}: DELETE OLD DATA")
        ref.delete_table(
            not_found_ok=True, mode=mode, bucket_name=destination_bucket_name
        )

    print(f"{mode.upper()}: TRANSFER NEW DATA")
    ref.copy_table(
        source_bucket_name=source_bucket_name,
        destination_bucket_name=destination_bucket_name,
        mode=mode,
    )
    return False


def save_header_files(dataset_id: str, table_id: str) -> str:
    print("GET FIRST BLOB PATH")

    ref = Storage(dataset_id=dataset_id, table_id=table_id)
    blobs = (
        ref.client["storage_staging"]
        .bucket("basedosdados-dev")  # type: ignore
        .list_blobs(prefix=f"staging/{dataset_id}/{table_id}/")
    )

    ## only needs the first bloob
    partitions = []
    for blob in blobs:
        blob_name = str(blob.name)
        if blob_name.endswith((".csv", ".parquet")):
            blob_path = blob_name.replace(
                f"staging/{dataset_id}/{table_id}/", "./downloaded_data/"
            )
            for folder in blob.name.split("/"):
                if "=" in folder:
                    partitions.append(folder.split("=")[0])
            print("Found blob: ", str(blob.name))
            print("Renamed blob: ", blob_path)
            break

    file_name = blob_path.split("/")[-1]
    file_type = file_name.split(".")[-1]

    path = Path(blob_path.replace(f"/{file_name}", ""))
    path.mkdir(parents=True, exist_ok=True)

    ### save table header in storage
    if file_type == "csv":
        print(
            f"DOWNLOAD HEADER FILE FROM basedosdados-dev.{dataset_id}_staging.{table_id}"
        )
        query = f"""
        SELECT * FROM `basedosdados-dev.{dataset_id}_staging.{table_id}` LIMIT 1
        """
        df = read_sql(query, billing_project_id="basedosdados", from_file=True)
        df = df.drop(columns=partitions)

        file_path = f"./{path}/table_approve_temp_file_271828.csv"
        df.to_csv(file_path, index=False)
    elif file_type == "parquet":
        file_path = f"./{path}/table_approve_temp_file_271828.parquet"
        blob.download_to_filename(file_path)
    print("SAVE HEADER FILE: ", file_path)
    return file_path


def push_table_to_bq(
    dataset_id: str,
    table_id: str,
    source_bucket_name: str = "basedosdados-dev",
    destination_bucket_name: str = "basedosdados",
    backup_bucket_name: str = "basedosdados-backup",
) -> bool:
    # copy proprosed data between storage buckets
    # create a backup of old data, then delete it and copies new data into the destination bucket
    # modes = ["staging", "raw", "auxiliary_files", "architecture", "header"]
    modes = ["staging"]

    for mode in modes:
        try:
            table_not_exists_in_storage = sync_bucket(
                source_bucket_name=source_bucket_name,
                dataset_id=dataset_id,
                table_id=table_id,
                destination_bucket_name=destination_bucket_name,
                backup_bucket_name=backup_bucket_name,
                mode=mode,
            )
        except Exception as error:
            print(f"DATA ERROR ON {mode}.{dataset_id}.{table_id}")
            traceback.print_exc(file=sys.stderr)
            table_not_exists_in_storage = True
            print(error)

    if table_not_exists_in_storage:
        print(f"Table {dataset_id}.{table_id} does not have data in storage.")
        return False

    file_path = save_header_files(dataset_id, table_id)

    # create table object of selected table and dataset ID
    tb = Table(dataset_id=dataset_id, table_id=table_id)

    # delete table from staging and prod if exists
    print("DELETE TABLE FROM STAGING AND PROD")
    tb.delete(mode="staging")

    file_format = file_path.split(".")[-1]
    print("CREATE NEW TABLE IN STAGING WITH FILE FORMAT: ", file_format)
    # create the staging table in bigquery

    tb.create(
        path="./downloaded_data/",
        if_table_exists="replace",
        if_storage_data_exists="pass",
        dataset_is_public=True,
        source_format=file_format,
    )

    print("UPDATE DATASET DESCRIPTION")
    # updates the dataset description
    Dataset(dataset_id).update(mode="prod")
    delete_storage_path = file_path.replace("./downloaded_data/", "")
    print(
        f"DELETE HEADER FILE FROM basedosdados/staging/{dataset_id}_staging/{table_id}/{delete_storage_path}"
    )
    st = Storage(dataset_id=dataset_id, table_id=table_id)
    st.delete_file(filename=delete_storage_path, mode="staging")
    shutil.rmtree("./downloaded_data/")
    return True


if __name__ == "__main__":
    arg_parser = ArgumentParser()

    # Add list of modified files argument
    arg_parser.add_argument(
        "--modified-files",
        type=str,
        required=True,
        help="List of modified files.",
    )

    # Add source bucket name argument
    arg_parser.add_argument(
        "--source-bucket-name",
        type=str,
        required=True,
        help="Source bucket name.",
    )

    # Add destination bucket name argument
    arg_parser.add_argument(
        "--destination-bucket-name",
        type=str,
        required=True,
        help="Destination bucket name.",
    )

    # Add backup bucket name argument
    arg_parser.add_argument(
        "--backup-bucket-name",
        type=str,
        required=True,
        help="Backup bucket name.",
    )

    # Add dbt command
    arg_parser.add_argument(
        "--dbt-command",
        type=str,
        required=True,
        default="run",
        help="dbt commadn. `run` or `test`",
    )

    # Add target dbt mode argument
    arg_parser.add_argument(
        "--target",
        type=str,
        required=True,
        default="prod",
        help="Target mode. `dev` or `prod`",
    )

    # Get arguments
    args = arg_parser.parse_args()

    # Get datasets and tables from modified files
    modified_files: list[str] = args.modified_files.split(",")

    datasets_tables = get_datasets_tables_from_modified_files(
        modified_files,
    )

    # Split deleted datasets and tables
    deleted_datasets_tables: list[tuple[str, str, bool]] = []
    existing_datasets_tables: list[tuple[str, str, bool]] = []

    for dataset_id, table_id, exists, alias in datasets_tables:
        if exists:
            existing_datasets_tables.append((dataset_id, table_id, alias))
        else:
            deleted_datasets_tables.append((dataset_id, table_id, alias))

    # Expand `__all__` tables
    backend = Backend(args.graphql_url)

    expanded_existing_datasets_tables: list[tuple[str, str, bool]] = []

    for dataset_id, table_id, alias in existing_datasets_tables:
        expanded_table_ids = expand_alls(dataset_id, table_id, backend)
        for expanded_table_id in expanded_table_ids:
            expanded_existing_datasets_tables.append(
                (dataset_id, expanded_table_id, alias)
            )

    existing_datasets_tables = expanded_existing_datasets_tables

    # Sync and create tables
    for dataset_id, table_id, _ in existing_datasets_tables:
        print(
            f"\n\n\n\n************   START CREATING TABLE {dataset_id}.{table_id}...   ************"
        )
        table_was_created = push_table_to_bq(
            dataset_id=dataset_id,
            table_id=table_id,
            source_bucket_name=args.source_bucket_name,
            destination_bucket_name=args.destination_bucket_name,
            backup_bucket_name=args.backup_bucket_name,
        )
        if table_was_created:
            print(
                f"============   TABLE CREATED: basedosdados-staging.{dataset_id}_staging.{table_id}   ============"
            )
        else:
            print(
                f"============   TABLE was NOT created: basedosdados-staging.{dataset_id}_staging.{table_id}   ============"
            )

    dbt_runner = dbtRunner()

    is_run_command: bool = args.dbt_command == "run"
    msg_prefix = "run" if is_run_command else "test"

    for dataset_id, table_id, alias in existing_datasets_tables:
        print(
            f"Running DBT {msg_prefix} for {dataset_id}.{table_id} (alias={alias})..."
        )

        selected_table = (
            f"{dataset_id}.{dataset_id}__{table_id}"
            if alias
            else f"{dataset_id}.{table_id}"
        )

        dbt_args = (
            [
                "run",
                f"--select {selected_table}",
                f"--target {args.target}",
            ]
            if is_run_command
            else ["test", f"--select {selected_table}"]
        )

        dbt_result = dbt_runner.invoke(dbt_args)

        if dbt_result.success:
            print(
                f"DBT {msg_prefix} for {selected_table} finished successfully"
            )
        else:
            raise Exception(
                f"DBT {msg_prefix} for {selected_table} failed. {dbt_result.result}. {dbt_args=}"
            )
