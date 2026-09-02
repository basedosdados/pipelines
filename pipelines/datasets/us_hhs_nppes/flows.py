"""Flows for us_hhs_nppes — Prefect 3.

NPPES (National Plan and Provider Enumeration System), the US registry of every
health care provider and organization holding an NPI. CMS republishes a **full
replacement snapshot monthly**. We stack snapshots (CNPJ model): each run uploads
the new snapshot to staging with ``dump_mode="overwrite"`` and the **incremental**
dbt models append its ``extraction_date`` partition to the prod tables, so the
panel accumulates. Weekly incrementals also exist upstream and are deliberately
ignored — each monthly file already supersedes them.

The run polls cheaply first (an HTTP HEAD on the monthly ZIP, compared against
``Table.Update.latest``) and only downloads the ~1.1 GB payload once CMS has
actually republished, so a scheduled run between releases is a cheap no-op.

Deploy: `.github/scripts/deploy_flows.py` auto-discovers ``us_hhs_nppes_flow``;
the dev pool ignores the schedule, the prod pool activates it (deployed paused).
"""

import shutil
import tempfile

from prefect import flow

from pipelines.datasets.us_hhs_nppes.constants import constants
from pipelines.datasets.us_hhs_nppes.tasks import (
    check_source_nppes,
    clean_nppes,
    download_nppes,
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
# NPPES refreshes monthly, so the six data tables carry the BD Pro rolling
# window: the most recent `free_lag` of snapshots are pro-only, older snapshots
# stay free. Each run recomputes free_end = source_end - free_lag, rewrites both
# DateTimeRanges and re-issues the BigQuery Row Access Policies, so the window
# slides forward without intervention.
#
# part_bdpro requires BOTH a free (is_closed=False) and a pro (is_closed=True)
# Coverage to already exist on the table, or assert_coverage_topology raises
# before anything is written. Both are created at metadata registration; verify
# they are present before arming.
#
# `dicionario` has no date column, so it takes no coverage spec.
_PART_BDPRO = PartBdpro(
    date_column=DateOnly(col="extraction_date"),
    date_format=DateFormat.YEAR_MD,
    free_lag=FreeLag(unit="months", value=6),
)
_COVERAGE = {
    table: _PART_BDPRO for table in constants.PARTITIONED_TABLES.value
}


@flow(name="us_hhs_nppes", log_prints=True)
def us_hhs_nppes_flow(
    materialize_to_prod: bool = True,
    update_metadata: bool = True,
    force_run: bool = False,
) -> None:
    """Refresh us_hhs_nppes with the latest monthly NPPES snapshot.

    Polls the source (cheap HEAD) and short-circuits when nothing new has been
    published, unless ``force_run``. On new data, downloads and cleans the
    bundle, uploads it to staging (``dump_mode="overwrite"``) and lets the
    incremental dbt models append its ``extraction_date`` partition to prod.

    Args:
        materialize_to_prod: Continue past the dev materialization to write the
            prod staging bucket and run dbt against ``target="prod"``. Set False
            to exercise only the dev half — required for a safe test run, since
            the default writes production.
        update_metadata: After a successful prod materialization, register table
            coverage (the rolling BD Pro window) and commit the source update.
            Has no effect when ``materialize_to_prod`` is False.
        force_run: Download and materialize even when the source poll reports no
            new snapshot.
    """
    # pyrefly: ignore [unused-coroutine]
    rename_flow_run_dataset_table(
        prefix="Dump: ", dataset_id=DATASET_ID, table_id="provider"
    )

    # Cheap poll first: has CMS published a bundle newer than our last refresh?
    source_date = check_source_nppes()
    has_new_data = poll_source_for_update_task(
        dataset_id=DATASET_ID,
        table_id="provider",
        source_max_date=source_date,
        env="prod",
        date_format="%Y-%m-%d",
        compare_against="table_update",
    )
    if not has_new_data and not force_run:
        return

    work_dir = tempfile.mkdtemp(prefix="us_hhs_nppes_")
    try:
        input_dir = download_nppes(work_dir=work_dir)
        result = clean_nppes(work_dir=work_dir, input_dir=input_dir)
        max_date = result["max_extraction_date"]

        tables = constants.TABLES.value

        # The dev materialization is the pre-arm validation path, not part of a
        # production run: it rebuilds and re-tests every table in
        # basedosdados-dev, which nothing downstream reads. Running it on an
        # armed run would double the BigQuery bytes billed for no signal — prod
        # runs the same models and the same tests seconds later.
        if not materialize_to_prod:
            # Dev: upload staging (overwrite with the new snapshot), then
            # materialize every table BEFORE testing any of them, so the
            # dictionary-coverage tests can see their sibling model.
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
                    dbt_command="run",
                    target="dev",
                )
            for table in tables:
                run_dbt(
                    dataset_id=DATASET_ID,
                    table_id=table,
                    dbt_command="test",
                    target="dev",
                )
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
                dbt_command="run",
                target="prod",
            )
        for table in tables:
            run_dbt(
                dataset_id=DATASET_ID,
                table_id=table,
                dbt_command="test",
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
            # Record the source Update: its max coverage date is the snapshot's
            # own reference date, not today.
            commit_source_update_task(
                dataset_id=DATASET_ID,
                table_id="provider",
                source_max_date=max_date,
                env="prod",
                date_format="%Y-%m-%d",
                update_metadata=update_metadata,
                materialize_after_dump=materialize_to_prod,
            )
    finally:
        shutil.rmtree(work_dir, ignore_errors=True)


# CMS posts the monthly bundle in the first half of the month; the exact day
# drifts (August 2026 landed on the 10th). Poll on several days at 15:23 BRT —
# a minute nobody else uses. The HEAD-based source poll no-ops (no download)
# until a new bundle actually appears.
# pyrefly: ignore [missing-attribute]
us_hhs_nppes_flow.deploy_schedules = [
    {"cron": "23 15 8,10,12,14,16 * *", "timezone": "America/Sao_Paulo"}
]
# The clean step streams the 11.6 GB main file in record batches and flushes in
# 500k-row chunks, but the bundle unzips to ~12 GB on disk and the download is
# ~1.1 GB; give the worker headroom.
# pyrefly: ignore [missing-attribute]
us_hhs_nppes_flow.job_variables = {"memory": "8Gi"}
