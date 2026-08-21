"""Flows for au_geoscape_gnaf — Prefect 3.

Geoscape **G-NAF** (Geocoded National Address File, data.gov.au). The source
republishes a **full snapshot quarterly** (Feb/May/Aug/Nov). We stack snapshots
(CNPJ model): each run uploads the new snapshot to staging with
``dump_mode="overwrite"`` and the **incremental** dbt models append its
``snapshot_date`` partition to the prod tables, so history accumulates.

G-NAF is Open-G-NAF/CC-BY, so every table is ``AllFree`` — no BD Pro rolling
window, no Row Access Policies. The quarterly cadence is well below the
monthly-or-more paywall threshold.

The run resolves the current release from the CKAN API and polls cheaply first
(the resolved ``snapshot_date`` vs the free ``Coverage``), only downloading the
~1.6 GB payload when a newer quarterly snapshot has actually been published — so
a scheduled run is a cheap no-op between quarterly releases.

Deploy: `.github/scripts/deploy_flows.py` auto-discovers ``au_geoscape_gnaf_flow``;
the dev pool ignores the schedule, the prod pool activates it (deployed paused).
"""

import shutil
import tempfile

from prefect import flow

from pipelines.datasets.au_geoscape_gnaf.constants import constants
from pipelines.datasets.au_geoscape_gnaf.tasks import (
    check_source_gnaf,
    clean_gnaf,
    download_gnaf,
)
from pipelines.utils.metadata.domain import (
    AllFree,
    DateFormat,
    DateOnly,
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
CORE_TABLE = constants.CORE_TABLE.value

# Coverage spec per table. G-NAF is CC-BY / Open-G-NAF, so every dated table is
# AllFree — the whole stacked history is public. `snapshot_date` (a DATE) is the
# coverage column. `dicionario` has no date column, so it takes no coverage spec.
_ALL_FREE = AllFree(
    date_column=DateOnly(col="snapshot_date"),
    date_format=DateFormat.YEAR_MD,
)
_COVERAGE = {
    "address_detail": _ALL_FREE,
    "street_locality": _ALL_FREE,
    "locality": _ALL_FREE,
}


@flow(name="au_geoscape_gnaf", log_prints=True)
def au_geoscape_gnaf_flow(
    materialize_to_prod: bool = True,
    update_metadata: bool = True,
    force_run: bool = False,
) -> None:
    """Refresh au_geoscape_gnaf with the latest quarterly G-NAF snapshot.

    Resolves the current release from CKAN, polls the source (the resolved
    ``snapshot_date`` vs the free ``Coverage``) and short-circuits when nothing
    newer has been published, unless ``force_run``. On new data, downloads +
    cleans the snapshot, uploads it to staging (``dump_mode="overwrite"``) and
    lets the incremental dbt models append its ``snapshot_date`` partition to the
    prod tables.

    Args:
        materialize_to_prod: Continue past the dev materialization to write the
            prod staging bucket and run dbt against ``target="prod"``. Set False
            to exercise only the dev half — required for a safe test run, since
            the default writes production.
        update_metadata: After a successful prod materialization, register table
            coverage (AllFree) and commit the source update. Has no effect when
            ``materialize_to_prod`` is False.
        force_run: Download and materialize even when the source poll reports no
            new snapshot.
    """
    # pyrefly: ignore [unused-coroutine]
    rename_flow_run_dataset_table(
        prefix="Dump: ", dataset_id=DATASET_ID, table_id=CORE_TABLE
    )

    # Cheap poll first: resolve the current release and ask whether its snapshot
    # is newer than the coverage we already publish.
    source = check_source_gnaf()
    snapshot_date = source["snapshot_date"]
    has_new_data = poll_source_for_update_task(
        dataset_id=DATASET_ID,
        table_id=CORE_TABLE,
        source_max_date=snapshot_date,
        env="prod",
        date_format="%Y-%m-%d",
        compare_against="coverage",
    )
    if not has_new_data and not force_run:
        return

    work_dir = tempfile.mkdtemp(prefix="au_geoscape_gnaf_")
    try:
        zip_path = download_gnaf(work_dir=work_dir, url=source["url"])
        result = clean_gnaf(
            work_dir=work_dir,
            zip_path=zip_path,
            snapshot_date=snapshot_date,
        )
        max_date = result["snapshot_date"]

        tables = constants.ALL_TABLES.value

        # The dev materialization is the pre-arm validation path, not part of a
        # production run: it rebuilds and re-tests every table in
        # basedosdados-dev, which nothing downstream reads. Running it on an
        # armed run doubled the BigQuery bytes billed for no signal — prod
        # runs the same models and the same tests seconds later.
        if not materialize_to_prod:
            # Dev: upload staging (overwrite with the new snapshot) + run ALL tables
            # first, THEN test. address_detail/street_locality/locality each carry a
            # custom_dictionary_coverage test that references the dicionario model, so
            # a per-table run/test would test a table while dicionario is not yet
            # materialized — it errors on a fresh target (no dicionario table). Split
            # run and test into separate passes so every table exists before any test.
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

        # Prod: upload staging + run ALL tables first, THEN test (see the dev-phase
        # note). Incremental dbt appends the new snapshot_date partition.
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
            # Record the source Update (its max coverage date = the snapshot date).
            commit_source_update_task(
                dataset_id=DATASET_ID,
                table_id=CORE_TABLE,
                source_max_date=max_date,
                env="prod",
                date_format="%Y-%m-%d",
                update_metadata=update_metadata,
                materialize_after_dump=materialize_to_prod,
            )
    finally:
        shutil.rmtree(work_dir, ignore_errors=True)


# The source republishes quarterly (Feb/May/Aug/Nov), landing mid-month; the
# exact day drifts (the Aug 2026 release landed on the 17th). Poll on several
# days across the second half of each release month at 16:00 BRT. The
# coverage-based source poll no-ops (no download) until a new snapshot appears.
# pyrefly: ignore [missing-attribute]
au_geoscape_gnaf_flow.deploy_schedules = [
    {"cron": "0 16 14,17,20,23,26 2,5,8,11 *", "timezone": "America/Sao_Paulo"}
]
# The clean step builds one state's frames at a time (NSW is the largest) and the
# download is ~1.6 GB; give the worker headroom.
# pyrefly: ignore [missing-attribute]
au_geoscape_gnaf_flow.job_variables = {"memory": "16Gi"}
