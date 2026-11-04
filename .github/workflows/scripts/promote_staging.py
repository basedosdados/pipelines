"""Promote a dataset's staging MIRRORS from the dev bucket to prod, then materialise.

WHY THIS EXISTS
---------------
`table-approve` syncs exactly one bucket prefix per PUBLISHED table --
`staging/<dataset_id>/<table_id>/`, with `table_id` read off a changed
`models/<dataset_id>/<model>.sql`. Most datasets keep one staging table per
published table, so that inference matches. A dataset that harmonises several
sources into fewer published tables does not: `world_wb_mides` has 40
`raw_*_mg` mirrors, one per SOURCE stream, feeding 43 published models. The sync
matched none of them, so merging the MG onboarding PR left `basedosdados-staging`
without a single MG mirror and the prod dbt run failed on the first model with

    Not found: Table basedosdados-staging:world_wb_mides_staging.raw_contrato_mg

`br_bd_execucao_estadual` hit the same wall on its own onboarding and solved it
with a bespoke Prefect seed flow that downloads and re-uploads every mirror
through a pod. This script does it by server-side bucket copy instead, reusing
`copy_staging_to_prod`/`sync_prod_staging_table`
(`pipelines.utils.materialize_prod.tasks`) -- the same shared tasks
`transfer_files_to_prod_flow` uses -- via `push_table_to_bq()` below, driven
from an explicit list of prefixes rather than inferred from changed `.sql`
files. Nothing here is dataset-specific.

RESUMABLE, BECAUSE IT IS SLOW
-----------------------------
`Storage.copy_table` batches 999 blobs per request and measured ~54 blobs/s on
the table-approve run of 2026-09-29 (`licitacao_item`, 10,380 blobs, 3m13s).
`world_wb_mides`'s 40 MG mirrors are 433,837 blobs -- 32.5 GiB of
one-file-per-município-per-exercise parquet -- so a full pass is roughly 2h15m.

Re-running after a failure is WORSE than the first pass, not better: once data
sits in the destination, `copy_staging_to_prod` adds a backup pass and a
delete pass before copying, close to tripling the work per mirror. So a
mirror whose destination blob count already equals the source's is skipped.
`--force` copies it regardless.

MATERIALISATION
---------------
`--all-models` launches ONE flow run with `table_id=None`, which makes
`run_dbt_task` select the whole `models/<dataset_id>` directory and lets dbt
resolve the DAG -- the right choice when parents and children are built in the
same pass. `--models a,b,c` launches one flow run per table instead, matching
table-approve, which additionally exports each table's public CSV (the
`download_data_to_gcs` step, itself skipped for anything over 1 GB).

USAGE
-----
Dispatched from `.github/workflows/promote-staging.yaml`, which supplies the
prod and staging credentials. Locally it needs those same credentials, which a
contributor machine does not have -- see `.claude/rules/onboarding-workflow.md`.
"""

import sys
from argparse import ArgumentParser

import basedosdados as bd
from prefect.client.schemas.objects import FlowRun
from prefect.deployments import run_deployment

# Same directory; reuse rather than restate the error-log printing.
from prefect_run_dbt import (  # pyrefly: ignore [missing-import]
    DBT_MODEL_DEPLOYMENT,
    PREFECT_UI_URL,
    print_flow_run_error_logs,
)

from pipelines.utils.materialize_prod.tasks import (
    copy_staging_to_prod,
    sync_prod_staging_table,
)


def push_table_to_bq(
    dataset_id: str,
    table_id: str,
    source_bucket_name="basedosdados-dev",
    destination_bucket_name="basedosdados",
    backup_bucket_name="basedosdados-backup",
    user_project: str = "basedosdados",
) -> bool:
    """Syncs staging data from dev to prod bucket and recreates the BigQuery table.

    Mesma lógica usada pelo `pipelines` (`build_and_promote`, via
    `transfer_files_to_prod_flow`) — ver `ftwca/investigacoes/pipelines/
    promocao-dev-prod-pipelines-vs-table-approve-2026-11-03.md` pro desenho
    completo. Chamada sem `folders`: promove a tabela inteira (backup do
    staging de prod atual + recriação da definição).

    Args:
        dataset_id: Dataset ID in basedosdados.
        table_id: Table ID in basedosdados.
        source_bucket_name: Bucket containing the approved data. Defaults to
            ``basedosdados-dev``.
        destination_bucket_name: Bucket to promote the data to. Defaults to
            ``basedosdados``.
        backup_bucket_name: Bucket for backing up the previous prod data. Defaults
            to ``basedosdados-backup``.
        user_project: GCP project to bill for bucket requests. Defaults to
            ``basedosdados``.

    Returns:
        True if the table was successfully created; False if no data was found in
        the source bucket.
    """
    try:
        sample_path = copy_staging_to_prod.fn(
            dataset_id=dataset_id,
            table_id=table_id,
            source_bucket=source_bucket_name,
            destination_bucket=destination_bucket_name,
            backup_bucket=backup_bucket_name,
            billing_project=user_project,
        )
    except FileNotFoundError:
        print(f"Table {dataset_id}.{table_id} does not have data in storage.")
        return False

    sync_prod_staging_table.fn(
        dataset_id=dataset_id,
        table_id=table_id,
        sample_path=sample_path,
        bucket_name=destination_bucket_name,
        recreate=True,
    )
    return True


def blob_count(
    dataset_id: str, table_id: str, bucket_name: str, user_project: str
) -> int:
    """Number of blobs under `staging/<dataset_id>/<table_id>/` in a bucket.

    Args:
        dataset_id: Dataset ID in basedosdados.
        table_id: Staging table ID (a mirror name, not necessarily published).
        bucket_name: Bucket to count in.
        user_project: GCP project billed for the requester-pays listing.

    Returns:
        Blob count, 0 when the prefix does not exist.
    """
    ref = bd.Storage(dataset_id=dataset_id, table_id=table_id)
    blobs = (
        ref.client["storage_staging"]
        .bucket(bucket_name, user_project=user_project)
        .list_blobs(prefix=f"staging/{dataset_id}/{table_id}/")
    )
    return sum(1 for _ in blobs)


def promote(
    dataset_id: str,
    staging_tables: list[str],
    source_bucket: str,
    destination_bucket: str,
    backup_bucket: str,
    user_project: str,
    force: bool,
    dry_run: bool,
) -> list[str]:
    """Copy each staging mirror to the prod bucket and recreate its BQ table.

    Args:
        dataset_id: Dataset ID in basedosdados.
        staging_tables: Staging table IDs to promote, in any order.
        source_bucket: Bucket holding the approved data.
        destination_bucket: Bucket to promote the data to.
        backup_bucket: Bucket for backing up previous destination data.
        user_project: GCP project billed for requester-pays reads.
        force: Copy even when the destination blob count already matches.
        dry_run: Report what would happen and copy nothing.

    Returns:
        The staging table IDs that failed, empty when all succeeded.
    """
    failed: list[str] = []
    for i, table_id in enumerate(staging_tables, 1):
        head = f"[{i}/{len(staging_tables)}] {dataset_id}.{table_id}"
        src = blob_count(dataset_id, table_id, source_bucket, user_project)
        dst = blob_count(
            dataset_id, table_id, destination_bucket, user_project
        )

        if src == 0:
            print(f"{head}: MISSING in {source_bucket} -- nothing to promote")
            failed.append(table_id)
            continue
        if dst == src and not force:
            print(
                f"{head}: already in {destination_bucket} ({src} blobs), skipped"
            )
            continue
        if dry_run:
            verb = "re-copy" if dst else "copy"
            print(f"{head}: would {verb} {src} blobs (destination has {dst})")
            continue

        print(
            f"\n\n***  {head}: copying {src} blobs (destination has {dst})  ***"
        )
        created = push_table_to_bq(
            dataset_id=dataset_id,
            table_id=table_id,
            source_bucket_name=source_bucket,
            destination_bucket_name=destination_bucket,
            backup_bucket_name=backup_bucket,
            user_project=user_project,
        )
        if created:
            print(
                f"===  CREATED basedosdados-staging.{dataset_id}_staging.{table_id}"
            )
        else:
            print(f"===  FAILED  {dataset_id}.{table_id}")
            failed.append(table_id)
    return failed


def materialise(
    dataset_id: str, models: list[str], all_models: bool, target: str
) -> None:
    """Launch prod dbt materialisation, waiting on each flow run.

    Args:
        dataset_id: Dataset ID in basedosdados.
        models: Published table IDs to build one at a time; ignored when
            `all_models` is set.
        all_models: Build the whole dataset in one flow run, letting dbt order
            the DAG.
        target: dbt target, normally `prod`.

    Raises:
        Exception: If any flow run finishes in a state other than completed.
            Later tables are not attempted, exactly as table-approve behaves.
    """
    # `table_id=None` makes run_dbt_task select `models/<dataset_id>`, so dbt
    # builds parents before children. Per-table runs cannot do that, but they do
    # export each table's public CSV.
    runs: list[str | None] = [None] if all_models else list(models)

    for table_id in runs:
        label = table_id or "<whole dataset, dbt DAG order>"
        print(f"Launching materialization flow for {dataset_id}.{label}...")
        # `run_deployment` is synchronous here -- there is no running loop -- but
        # its stub types the return as a Coroutine. Annotating narrows it once,
        # instead of a false positive on every attribute read below.
        # pyrefly: ignore [bad-assignment]
        flow_run: FlowRun = run_deployment(
            name=DBT_MODEL_DEPLOYMENT,
            parameters={
                "dataset_id": dataset_id,
                "table_id": table_id,
                "dbt_command": "run",
                "target": target,
                "download_csv_file": target == "prod" and table_id is not None,
            },
            timeout=None,
            as_subflow=False,
        )
        url = f"{PREFECT_UI_URL}/runs/flow-run/{flow_run.id}"
        print(f" - Materialization flow run launched: {url}")

        state = flow_run.state
        if state is None or not state.is_completed():
            print_flow_run_error_logs(str(flow_run.id))
            raise Exception(
                f"Flow run {flow_run.id} for {dataset_id}.{label} finished with "
                f'state "{state.name if state else "Unknown"}". Logs at {url}'
            )
        print(
            f"Flow run {flow_run.id} ({dataset_id}.{label}) finished successfully."
        )


def run_promote_staging() -> None:
    """Parse arguments, promote the named mirrors, then materialise."""
    parser = ArgumentParser(description=__doc__)
    parser.add_argument("--dataset-id", required=True)
    parser.add_argument(
        "--staging-tables",
        default="",
        help="comma-separated staging table IDs to copy dev -> prod",
    )
    parser.add_argument(
        "--models",
        default="",
        help="comma-separated published table IDs to dbt run, one flow each",
    )
    parser.add_argument(
        "--all-models",
        action="store_true",
        help="dbt run the whole dataset in one flow run, in DAG order",
    )
    parser.add_argument("--source-bucket", default="basedosdados-dev")
    parser.add_argument("--destination-bucket", default="basedosdados")
    parser.add_argument("--backup-bucket", default="basedosdados-backup")
    parser.add_argument("--user-project", default="basedosdados")
    parser.add_argument("--materialization-target", default="prod")
    parser.add_argument(
        "--force",
        action="store_true",
        help="copy a mirror even when the destination blob count matches",
    )
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    def split(value: str) -> list[str]:
        return [p.strip() for p in value.split(",") if p.strip()]

    staging_tables = split(args.staging_tables)
    models = split(args.models)

    if not staging_tables and not models and not args.all_models:
        parser.error(
            "nothing to do: pass --staging-tables, --models or --all-models"
        )

    failed: list[str] = []
    if staging_tables:
        failed = promote(
            dataset_id=args.dataset_id,
            staging_tables=staging_tables,
            source_bucket=args.source_bucket,
            destination_bucket=args.destination_bucket,
            backup_bucket=args.backup_bucket,
            user_project=args.user_project,
            force=args.force,
            dry_run=args.dry_run,
        )
        print(
            f"\n{len(staging_tables) - len(failed)}/{len(staging_tables)} mirrors in "
            f"{args.destination_bucket}"
        )

    # A missing mirror is exactly what breaks the prod dbt run, so stop here
    # rather than spending a materialisation that is certain to fail.
    if failed:
        print(
            f"NOT materialising -- these mirrors did not reach prod: {failed}"
        )
        sys.exit(1)

    if args.dry_run:
        target = "whole dataset" if args.all_models else models
        print(f"would materialise: {target or 'nothing'}")
        return

    if models or args.all_models:
        materialise(
            dataset_id=args.dataset_id,
            models=models,
            all_models=args.all_models,
            target=args.materialization_target,
        )


if __name__ == "__main__":
    run_promote_staging()
