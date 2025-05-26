import shutil
import sys
import traceback
from argparse import ArgumentParser
from pathlib import Path
from time import sleep

import basedosdados as bd

PREFECT_BASE_URL = "https://prefect.basedosdados.org"


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


def get_flow_run_state(
    flow_run_id: str, backend: bd.Backend, auth_token: str
) -> str:
    query = """
    query ($flow_run_id: uuid!) {
        flow_run_by_pk (id: $flow_run_id) {
            state
        }
    }
    """
    response = backend._execute_query(
        query,
        variables={"flow_run_id": flow_run_id},
        headers={"Authorization": f"Bearer {auth_token}"},
    )
    return response["flow_run_by_pk"]["state"]


def get_flow_log_error_messages(
    flow_run_id: str, backend: bd.Backend, auth_token: str
) -> list[str]:
    query = """query ($flow_run_id: uuid!) {
        log(where: {flow_run_id: {_eq: $flow_run_id}, level: {_eq: "ERROR"}}) {
            message,
        }
    }
    """
    response = backend._execute_query(
        query,
        variables={"flow_run_id": flow_run_id},
        headers={"Authorization": f"Bearer {auth_token}"},
    )
    return [i["message"] for i in response["log"]]


def get_materialization_flow_id(
    backend: bd.Backend, auth_token: str, project: str = "main"
) -> str:
    """
    Get DBT Flow.
    """
    query = """
    query ($projectName: String!) {
        flow (where: {
            name: {
                _like: "BD template: Executa DBT model"
            },
            archived: {
                _eq: false
            },
            project: {
                name: {_eq: $projectName}
            }
        }) {
            id,
            version,
            run_config
        }
    }
    """
    response = backend._execute_query(
        query,
        headers={"Authorization": f"Bearer {auth_token}"},
        variables={"projectName": project},
    )
    flow_data = response["flow"][0]
    print(f"get_materialization_flow_id: {flow_data}")
    return flow_data["id"]


def push_table_to_bq(
    dataset_id: str,
    table_id: str,
    source_bucket_name="basedosdados-dev",
    destination_bucket_name="basedosdados",
    backup_bucket_name="basedosdados-backup",
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
        except Exception:
            print(f"DATA ERROR ON {mode}.{dataset_id}.{table_id}")
            traceback.print_exc(file=sys.stderr)
            table_not_exists_in_storage = True

    if table_not_exists_in_storage:  # type: ignore
        print(f"Table {dataset_id}.{table_id} does not have data in storage.")
        return False

    file_path = save_header_files(dataset_id, table_id)

    # create table object of selected table and dataset ID
    tb = bd.Table(dataset_id=dataset_id, table_id=table_id)

    # delete table from staging and prod if exists
    print(f"Delete {dataset_id}.{table_id} from staging and prod")
    tb.delete(mode="staging")

    file_format = file_path.split(".")[-1]
    print(
        f"Create {dataset_id}.{table_id} in staging with file format: {file_format}"
    )

    # Create the staging table in bigquery
    tb.create(
        path="./downloaded_data/",
        if_table_exists="replace",
        if_storage_data_exists="pass",
        dataset_is_public=True,
        source_format=file_format,
    )

    print("UPDATE DATASET DESCRIPTION")
    # Updates the dataset description
    bd.Dataset(dataset_id).update(mode="prod")
    delete_storage_path = file_path.replace("./downloaded_data/", "")
    print(
        f"DELETE HEADER FILE FROM basedosdados/staging/{dataset_id}_staging/{table_id}/{delete_storage_path}"
    )
    st = bd.Storage(dataset_id=dataset_id, table_id=table_id)
    st.delete_file(filename=delete_storage_path, mode="staging")
    shutil.rmtree("./downloaded_data/")
    return True


def save_header_files(dataset_id: str, table_id: str) -> str:
    print("GET FIRST BLOB PATH")

    ref = bd.Storage(dataset_id=dataset_id, table_id=table_id)
    blobs = (
        ref.client["storage_staging"]
        .bucket("basedosdados-dev")
        .list_blobs(prefix=f"staging/{dataset_id}/{table_id}/")
    )

    # if len(blobs) == 0:
    #     raise ValueError(f"No blobs found in staging/{dataset_id}/{table_id}/")

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
        df = bd.read_sql(
            query, billing_project_id="basedosdados", from_file=True
        )
        df = df.drop(columns=partitions)

        file_path = f"./{path}/table_approve_temp_file_271828.csv"
        df.to_csv(file_path, index=False)
    elif file_type == "parquet":
        file_path = f"./{path}/table_approve_temp_file_271828.parquet"
        blob.download_to_filename(file_path)
    print("SAVE HEADER FILE: ", file_path)
    return file_path


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

    ref = bd.Storage(dataset_id=dataset_id, table_id=table_id)

    prefix = f"{mode}/{dataset_id}/{table_id}/"

    source_ref = (
        ref.client["storage_staging"]
        .bucket(source_bucket_name)
        .list_blobs(prefix=prefix)
    )

    destination_ref = ref.bucket.list_blobs(prefix=prefix)

    if len(list(source_ref)) == 0:
        print(
            f"No objects found on the source bucket {source_bucket_name}.{prefix}"
        )
        return True

    if len(list(destination_ref)):
        backup_bucket_blobs = list(
            ref.client["storage_staging"]
            .bucket(backup_bucket_name)
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


def get_datasets_and_tables_for_modified_files(
    modified_files: str,
) -> list[tuple[str, str, bool]]:
    modified_files_list = modified_files.split(",")
    datasets_tables = get_datasets_tables_from_modified_files(
        modified_files_list
    )

    existing_datasets_tables = []

    for dataset_id, table_id, exists, alias in datasets_tables:
        if exists:
            existing_datasets_tables.append((dataset_id, table_id, alias))

    return existing_datasets_tables


if __name__ == "__main__":
    # Start argument parser
    arg_parser = ArgumentParser()

    # Add list of modified files argument
    arg_parser.add_argument(
        "--dbt-command",
        type=str,
        required=True,
        help="dbt command",
    )

    arg_parser.add_argument(
        "--modified-files",
        type=str,
        required=False,
        help="List of modified files.",
    )

    # Add argument to skip sync bucket step
    arg_parser.add_argument(
        "--sync-bucket",
        action="store_true",  # Implies default is false, i.e, dont sync buckets
        help="Sync buckets",
    )

    # Add source bucket name argument
    arg_parser.add_argument(
        "--source-bucket-name",
        type=str,
        required=False,
        default="basedosdados-dev",
        help="Source bucket name.",
    )

    # Add destination bucket name argument
    arg_parser.add_argument(
        "--destination-bucket-name",
        type=str,
        required=False,
        default="basedosdados",
        help="Destination bucket name.",
    )

    # Add backup bucket name argument
    arg_parser.add_argument(
        "--backup-bucket-name",
        type=str,
        required=False,
        default="basedosdados-backup",
        help="Backup bucket name.",
    )

    # Add Prefect API token argument
    arg_parser.add_argument(
        "--prefect-backend-token",
        type=str,
        required=True,
        help="Prefect backend token.",
    )

    # Add materialization mode argument
    arg_parser.add_argument(
        "--materialization-target",
        type=str,
        required=False,
        default="prod",
        help="Materialization target.",
    )

    # Add materialization label argument
    arg_parser.add_argument(
        "--materialization-label",
        type=str,
        required=False,
        default="basedosdados",
        help="Materialization label.",
    )

    arg_parser.add_argument(
        "--dataset-id",
        type=str,
        required=False,
        help="Run dbt model for dataset",
    )

    # Get arguments
    args = arg_parser.parse_args()

    # Get datasets and tables from modified files
    existing_datasets_tables_from_modified_files = (
        []
        if args.modified_files is None
        else get_datasets_and_tables_for_modified_files(
            args.modified_files.split(",")
        )
    )

    # Sync and create tables
    if args.sync_bucket:
        for (
            dataset_id,
            table_id,
            _,
        ) in existing_datasets_tables_from_modified_files:
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
    else:
        print("Skipping sync bucket because --sync-bucket was not set")

    # Launch materialization flows
    backend_prefect = bd.Backend(graphql_url=f"{PREFECT_BASE_URL}/api")

    flow_id = get_materialization_flow_id(
        backend=backend_prefect,
        auth_token=args.prefect_backend_token,
        project="staging"
        if args.materialization_target == "dev"
        and args.materialization_label == "basedosdados-dev"
        else "main",
    )

    datastes_tables_for_flow_run = existing_datasets_tables_from_modified_files

    if args.dataset_id is not None:
        datastes_tables_for_flow_run.append((args.dataset_id, "", False))

    launched_flow_run_ids = []

    for (
        dataset_id,
        table_id,
        alias,
    ) in datastes_tables_for_flow_run:
        print(
            f"Launching materialization flow for {dataset_id}.{table_id} (alias={alias})..."
        )
        parameters = {
            "dataset_id": dataset_id,
            "table_id": table_id,
            "target": args.materialization_target,
            "dbt_command": args.dbt_command,
            "dbt_alias": alias,
            # Download csv file is true by default
            # We disable when materialization target is not prod
            "download_csv_file": args.materialization_target == "prod",
            # Disable elementary is true by default
            # We disable when run dbt for elementary dataset
            "disable_elementary": dataset_id != "elementary",
        }
        mutation = """
        mutation ($flow_id: UUID, $parameters: JSON, $label: String!) {
            create_flow_run (input: {
                flow_id: $flow_id,
                parameters: $parameters,
                labels: [$label],
            }) {
                id
            }
        }
        """
        variables = {
            "flow_id": flow_id,
            "parameters": parameters,
            "label": args.materialization_label,
        }
        response = backend_prefect._execute_query(
            mutation,
            variables,
            headers={"Authorization": f"Bearer {args.prefect_backend_token}"},
        )
        flow_run_id = response["create_flow_run"]["id"]
        launched_flow_run_ids.append(flow_run_id)
        flow_run_url = f"{PREFECT_BASE_URL}/flow-run/{flow_run_id}"
        print(f" - Materialization flow run launched: {flow_run_url}")

    # Keep monitoring the launched flow runs until they are finished
    for launched_flow_run_id in launched_flow_run_ids:
        print(f"Monitoring flow run {launched_flow_run_id}...")
        flow_run_state = get_flow_run_state(
            flow_run_id=launched_flow_run_id,
            backend=backend_prefect,
            auth_token=args.prefect_backend_token,
        )
        while flow_run_state not in ["Success", "Failed", "Cancelled"]:
            sleep(5)
            flow_run_state = get_flow_run_state(
                flow_run_id=launched_flow_run_id,
                backend=backend_prefect,
                auth_token=args.prefect_backend_token,
            )
        if flow_run_state != "Success":
            messages = get_flow_log_error_messages(
                flow_run_id=launched_flow_run_id,
                backend=backend_prefect,
                auth_token=args.prefect_backend_token,
            )
            for message in messages:
                print(message)

            flow_run_url = (
                f"{PREFECT_BASE_URL}/flow-run/{launched_flow_run_id}"
            )

            raise Exception(
                f'Flow run {launched_flow_run_id} finished with state "{flow_run_state}". '
                f"Check the full logs at {flow_run_url}"
            )

        print(f"Flow run {launched_flow_run_id} finished successfully.")
