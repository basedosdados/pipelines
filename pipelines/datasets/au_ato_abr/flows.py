"""Flows for au_ato_abr — Prefect 3.

Australian Business Register "ABN Bulk Extract" (data.gov.au, ATO). The source
republishes a **full snapshot weekly**. We stack snapshots (CNPJ model): each run
uploads the new snapshot to staging with ``dump_mode="overwrite"`` and the
**incremental** dbt models append its ``extraction_date`` partition to the prod
tables, so history accumulates.

The run polls cheaply first (an HTTP HEAD on the ZIPs, compared against
``Table.Update.latest``) and only downloads the ~1 GB payload when the source has
actually republished — so a scheduled run is a cheap no-op between weekly releases.

Deploy: `.github/scripts/deploy_flows.py` auto-discovers ``au_ato_abr_flow``; the
dev pool ignores the schedule, the prod pool activates it (deployed paused).
"""

import shutil
import tempfile

from prefect import flow

from pipelines.datasets.au_ato_abr.constants import constants
from pipelines.datasets.au_ato_abr.tasks import (
    check_source_abr,
    clean_abr,
    download_abr,
)
from pipelines.utils.metadata.domain import (
    DateFormat,
    DateOnly,
    FreeLag,
    PartBdpro,
)
from pipelines.utils.metadata.tasks import (
    commit_source_update_task,
    poll_source_for_update_task,
    register_table_materialization_task,
)
from pipelines.utils.tasks import (
    rename_flow_run_dataset_table,
    run_dbt,
    upload_to_gcs,
)

DATASET_ID = constants.DATASET_ID.value

# Coverage spec per table.
#
# The register refreshes weekly, so the three data tables carry the BD Pro
# rolling window: the most recent `free_lag` of snapshots are pro-only, older
# snapshots stay free. Each run recomputes free_end = source_end - free_lag,
# rewrites both DateTimeRanges, and re-issues the BigQuery Row Access Policies,
# so the window slides forward on its own.
#
# part_bdpro requires BOTH a free (is_closed=False) and a pro (is_closed=True)
# Coverage to already exist on the table, or assert_coverage_topology raises
# before anything is written. The static onboard registered only the free
# Coverage, so the pro Coverage must be created on each of entity/other_name/dgr
# BEFORE this flow is armed (see the pipeline PR notes / ONBOARDING_PLAN.md).
#
# free_lag is a business choice: with weekly snapshots and a full register per
# snapshot, the free tier always holds a complete (if lagged) register. 6 months
# mirrors br_rf_cnpj; a shorter lag (e.g. FreeLag("weeks", 4)) narrows the
# initial free-tier lockout at arm time. Confirm before arming.
#
# `dicionario` has no date column, so it takes no coverage spec.
_PART_BDPRO = PartBdpro(
    date_column=DateOnly(col="extraction_date"),
    date_format=DateFormat.YEAR_MD,
    free_lag=FreeLag(unit="months", value=6),
)
_COVERAGE = {
    "entity": _PART_BDPRO,
    "other_name": _PART_BDPRO,
    "dgr": _PART_BDPRO,
}


@flow(name="au_ato_abr", log_prints=True)
def au_ato_abr_flow(
    materialize_to_prod: bool = True,
    update_metadata: bool = True,
    force_run: bool = False,
) -> None:
    """Refresh au_ato_abr with the latest weekly ABN Bulk Extract snapshot.

    Polls the source (cheap HEAD) and short-circuits when nothing new has been
    published, unless ``force_run``. On new data, downloads + cleans the snapshot,
    uploads it to staging (``dump_mode="overwrite"``) and lets the incremental dbt
    models append its ``extraction_date`` partition to the prod tables.

    Args:
        materialize_to_prod: Continue past the dev materialization to write the
            prod staging bucket and run dbt against ``target="prod"``. Set False
            to exercise only the dev half — required for a safe test run, since
            the default writes production.
        update_metadata: After a successful prod materialization, register table
            coverage (rolling BD Pro window) and commit the source update. Has no
            effect when ``materialize_to_prod`` is False.
        force_run: Download and materialize even when the source poll reports no
            new snapshot.
    """
    # pyrefly: ignore [unused-coroutine]
    rename_flow_run_dataset_table(
        prefix="Dump: ", dataset_id=DATASET_ID, table_id="entity"
    )

    # Cheap poll first: is the source's publication newer than our last refresh?
    source_date = check_source_abr()
    has_new_data = poll_source_for_update_task(
        dataset_id=DATASET_ID,
        table_id="entity",
        source_max_date=source_date,
        env="prod",
        date_format="%Y-%m-%d",
        compare_against="table_update",
    )
    if not has_new_data and not force_run:
        return

    work_dir = tempfile.mkdtemp(prefix="au_ato_abr_")
    try:
        input_dir = download_abr(work_dir=work_dir)
        result = clean_abr(work_dir=work_dir, input_dir=input_dir)
        max_date = result["max_extraction_date"]

        tables = constants.ALL_TABLES.value

        # Dev: upload staging (overwrite with the new snapshot) + materialize/test.
        for table in tables:
            upload_to_gcs(
                data_path=result[table],
                dataset_id=DATASET_ID,
                table_id=table,
                bucket_name="basedosdados-dev",
                dump_mode="overwrite",
                source_format="parquet",
            )
            run_dbt(
                dataset_id=DATASET_ID,
                table_id=table,
                dbt_command="run/test",
                target="dev",
            )

        if not materialize_to_prod:
            return

        # Prod: upload staging + materialize/test (incremental dbt appends the
        # new extraction_date partition, keeping history).
        for table in tables:
            upload_to_gcs(
                data_path=result[table],
                dataset_id=DATASET_ID,
                table_id=table,
                bucket_name="basedosdados",
                dump_mode="overwrite",
                source_format="parquet",
            )
            run_dbt(
                dataset_id=DATASET_ID,
                table_id=table,
                dbt_command="run/test",
                target="prod",
            )

        if update_metadata:
            for table, coverage in _COVERAGE.items():
                register_table_materialization_task(
                    dataset_id=DATASET_ID,
                    table_id=table,
                    coverage=coverage,
                    env="prod",
                    bq_project="basedosdados",
                )
            # Record the source Update (its max coverage date = the snapshot date).
            commit_source_update_task(
                dataset_id=DATASET_ID,
                table_id="entity",
                source_max_date=max_date,
                env="prod",
                date_format="%Y-%m-%d",
                update_metadata=update_metadata,
                materialize_after_dump=materialize_to_prod,
            )
    finally:
        shutil.rmtree(work_dir, ignore_errors=True)


# The source republishes weekly; the exact weekday drifts, so poll on several
# days at 16:00 BRT. The HEAD-based source poll no-ops (no download) until a new
# snapshot actually appears.
# pyrefly: ignore [missing-attribute]
au_ato_abr_flow.deploy_schedules = [
    {"cron": "0 16 * * 1,2,3,4", "timezone": "America/Sao_Paulo"}
]
# The clean step streams from the ZIPs and flushes in 400k-row chunks, but the
# download is ~1 GB; give the worker headroom.
# pyrefly: ignore [missing-attribute]
au_ato_abr_flow.job_variables = {"memory": "8Gi"}
