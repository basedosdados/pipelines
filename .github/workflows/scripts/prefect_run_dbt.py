import asyncio
import logging as stdlib_logging
import os
import shutil
import sys
import traceback
from argparse import ArgumentParser
from pathlib import Path

import basedosdados as bd
from prefect.deployments import run_deployment

# Prefect 3 deployment triggered for each modified table.
# Slug = "<flow name>/<deployment name>": the flow's @flow(name=...) is
# "BD template: Executa DBT model" and deploy_flows.py registers it under the
# python identifier "run_dbt_model_flow".
DBT_MODEL_DEPLOYMENT = "BD template: Executa DBT model/run_dbt_model_flow"

# Base UI URL for human-readable flow-run links, derived from PREFECT_API_URL.
PREFECT_BASE_URL = os.environ.get(
    "PREFECT_API_URL", "https://prefect3.basedosdados.org/api"
).removesuffix("/api")


async def _read_flow_run_logs(flow_run_id: str) -> list:
    from prefect.client.orchestration import get_client
    from prefect.client.schemas.filters import LogFilter, LogFilterFlowRunId

    async with get_client() as client:
        return await client.read_logs(
            log_filter=LogFilter(
                flow_run_id=LogFilterFlowRunId(any_=[flow_run_id])
            ),
            limit=10000,
        )


def print_flow_run_logs(flow_run_id: str) -> None:
    logs = asyncio.run(_read_flow_run_logs(str(flow_run_id)))
    for log in logs:
        level_name = stdlib_logging.getLevelName(log.level)
        print(f"[{level_name}] {log.timestamp}: {log.message}")


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


def push_table_to_bq(
    dataset_id: str,
    table_id: str,
    source_bucket_name="basedosdados-dev",
    destination_bucket_name="basedosdados",
    backup_bucket_name="basedosdados-backup",
    user_project: str = "basedosdados",
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
                user_project=user_project,
                mode=mode,
            )
        except Exception:
            print(f"DATA ERROR ON {mode}.{dataset_id}.{table_id}")
            traceback.print_exc(file=sys.stderr)
            table_not_exists_in_storage = True

    if table_not_exists_in_storage:  # type: ignore
        print(f"Table {dataset_id}.{table_id} does not have data in storage.")
        return False

    file_path = save_header_files(
        dataset_id, table_id, user_project=user_project
    )

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


def save_header_files(
    dataset_id: str, table_id: str, user_project: str
) -> str:
    print("GET FIRST BLOB PATH")

    ref = bd.Storage(dataset_id=dataset_id, table_id=table_id)
    blobs = (
        ref.client["storage_staging"]
        .bucket("basedosdados-dev", user_project=user_project)
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
    user_project: str = "basedosdados",
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
        .bucket(source_bucket_name, user_project=user_project)
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
            .bucket(backup_bucket_name, user_project=user_project)
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
    modified_files: list[str],
) -> list[tuple[str, str, bool]]:
    datasets_tables = get_datasets_tables_from_modified_files(modified_files)

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

    # Deprecated: Prefect 3 auth comes from the PREFECT_API_KEY env var read by
    # the client. Kept (optional, ignored) so the legacy cd.yaml caller that
    # still passes it does not fail at argument parsing.
    arg_parser.add_argument(
        "--prefect-backend-token",
        type=str,
        required=False,
        help="Deprecated and ignored; use PREFECT_API_KEY env var.",
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

    # Launch materialization flows on Prefect 3.
    # run_deployment reads PREFECT_API_URL / PREFECT_API_KEY from the
    # environment and blocks (timeout=None) until each run reaches a terminal
    # state, polling every 5s.
    datasets_tables_for_flow_run = existing_datasets_tables_from_modified_files

    if args.dataset_id is not None:
        datasets_tables_for_flow_run.append((args.dataset_id, "", False))

    for (
        dataset_id,
        table_id,
        alias,
    ) in datasets_tables_for_flow_run:
        print(
            f"Launching materialization flow for {dataset_id}.{table_id} (alias={alias})..."
        )
        flow_run = run_deployment(
            name=DBT_MODEL_DEPLOYMENT,
            parameters={
                "dataset_id": dataset_id,
                "table_id": table_id,
                "dbt_command": args.dbt_command,
                "dbt_alias": alias,
                "target": args.materialization_target,
                # Download csv file is true by default.
                # We disable when materialization target is not prod.
                "download_csv_file": args.materialization_target == "prod",
            },
            timeout=None,
            as_subflow=False,
        )
        flow_run_url = f"{PREFECT_BASE_URL}/runs/flow-run/{flow_run.id}"
        print(f" - Materialization flow run launched: {flow_run_url}")

        print_flow_run_logs(str(flow_run.id))

        state = flow_run.state
        if state is None or not state.is_completed():
            state_name = state.name if state is not None else "Unknown"
            raise Exception(
                f"Flow run {flow_run.id} for {dataset_id}.{table_id} finished "
                f'with state "{state_name}". Check the full logs at {flow_run_url}'
            )

        print(
            f"Flow run {flow_run.id} ({dataset_id}.{table_id}) finished successfully."
        )
