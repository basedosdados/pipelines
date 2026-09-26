"""
Flows for au_apra_adi — Prefect 3.

The source is one APRA workbook, "Quarterly authorised deposit-taking
institution performance statistics", republished every quarter with the full
history embedded. A single flow downloads it once and rebuilds all eight tables
(three wide financial statements, four long liquidity/quality statements, and
the dictionary). Because every release ships the whole back-series, each run is
a **full replace**, not an incremental append (see ``_DUMP_MODE`` for why that
is spelled ``"append"``).

Every table refreshes only quarterly, so all stay ``AllFree``: the BD Pro
rolling window applies only to tables refreshed monthly or more often, and none
of these issues Row Access Policies.

The source poll short-circuits a run until APRA publishes a newer quarter, which
makes a scheduled run a cheap no-op between releases.

Deploy: ``.github/scripts/deploy_flows.py`` auto-discovers ``au_apra_adi_flow``;
the dev pool ignores the schedule, the prod pool activates it.
"""

import shutil
import tempfile

from prefect import flow
from prefect.utilities.asyncutils import run_coro_as_sync

from pipelines.datasets.au_apra_adi.constants import constants
from pipelines.datasets.au_apra_adi.tasks import clean_adi, download_adi
from pipelines.utils.metadata.domain import (
    AllFree,
    DateFormat,
    YearQuarter,
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

# The table the poll and source-update commit anchor on. All tables come from
# one workbook and share the same latest quarter, so polling one is enough.
_POLL_TABLE = "financial_performance"

# Every APRA release republishes the full history, so each run is a full
# replace. That is `append`, not `overwrite`, despite the names.
#
# `dump_mode="overwrite"` calls `tb.delete(mode="all")`, which drops the
# MATERIALIZED PRODUCTION table, not just the staging external table -- and
# `bd.Table` resolves its projects from the worker's config.toml rather than
# from `bucket_name`, so it fires from the dev half too. A dev-only validation
# run (`materialize_to_prod=False`) would delete the prod table and then return
# before rebuilding it, reporting every task Completed.
#
# `append` ends in `st.upload(..., if_exists="replace")`, which replaces each
# blob wholesale. Every partition writes one `data.parquet` under `year=YYYY/`
# and each release ships the whole history, so the file names are identical run
# to run and the semantics are the same -- without the delete.
_DUMP_MODE = "append"

# Coverage spec per table. Every table is quarterly, so all are AllFree. The
# poll compares a "%Y-%m" string against the registered coverage, and the
# coverage for a YearQuarter column is stored as MAX(DATE(year, quarter * 3, 1))
# formatted the same way -- so `clean_all` reports the release's latest period
# as a year-MONTH (see tasks._yq_to_ym). `dicionario` has no date column, so it
# takes no coverage spec.
_COVERAGE = {
    table: AllFree(
        date_column=YearQuarter(year="year", quarter="quarter"),
        date_format=DateFormat.YEAR_MONTH,
    )
    for table in constants.DATA_TABLES.value
}


@flow(name="au_apra_adi", log_prints=True)
def au_apra_adi_flow(
    materialize_to_prod: bool = True,
    update_metadata: bool = True,
    force_run: bool = False,
) -> None:
    """Download the current APRA ADI workbook, rebuild all tables, materialize them.

    APRA ships the full history on every release, so each run is a full replace.
    The source poll short-circuits the run when APRA has not published a new
    quarter, making a scheduled run a cheap no-op between releases.

    Args:
        materialize_to_prod: Continue past the dev materialization to write the
            prod staging bucket and run dbt against ``target="prod"``. Set False
            to exercise only the dev half — required for a safe test run, since
            the default writes production.
        update_metadata: After a successful prod materialization, register table
            coverage and commit the source update. No effect when
            ``materialize_to_prod`` is False.
        force_run: Materialize even when the source poll reports no new quarter.
    """
    run_coro_as_sync(
        rename_flow_run_dataset_table(
            prefix="Dump: ", dataset_id=DATASET_ID, table_id="adi"
        )
    )

    work_dir = tempfile.mkdtemp(prefix="au_apra_adi_")
    try:
        input_dir = download_adi(work_dir=work_dir)
        result = clean_adi(work_dir=work_dir, input_dir=input_dir)
        max_ym = result["max_year_month"]

        # Skip the run when APRA has not published a newer quarter (unless forced).
        has_new_data = poll_source_for_update_task(
            dataset_id=DATASET_ID,
            table_id=_POLL_TABLE,
            source_max_date=max_ym,
            env="prod",
            date_format="%Y-%m",
            compare_against="coverage",
        )
        if not has_new_data and not force_run:
            return

        # Commit the source Update before materializing: if the flow dies
        # midway, the source metadata still records that APRA had published a
        # new quarter, even though the tables were not refreshed.
        commit_source_update_task(
            dataset_id=DATASET_ID,
            table_id=_POLL_TABLE,
            source_max_date=max_ym,
            env="prod",
            date_format="%Y-%m",
            update_metadata=update_metadata,
            materialize_after_dump=materialize_to_prod,
        )

        tables = constants.TABLES.value

        # The dev materialization is the pre-arm validation path, not part of a
        # production run: it rebuilds and re-tests every table in
        # basedosdados-dev, which nothing downstream reads. An armed run skips
        # it -- prod runs the same models and the same tests seconds later.
        bucket = "basedosdados" if materialize_to_prod else "basedosdados-dev"
        target = "prod" if materialize_to_prod else "dev"

        # Upload + run every table first, then test every table: cross-table
        # tests (relationships, dictionary coverage) read sibling models, which
        # must all exist before any test runs.
        for table in tables:
            upload_to_gcs(
                data_path=result[table],
                dataset_id=DATASET_ID,
                table_id=table,
                bucket_name=bucket,
                dump_mode=_DUMP_MODE,
                source_format="parquet",
            )
            run_dbt(
                dataset_id=DATASET_ID,
                table_id=table,
                dbt_command="run",
                target=target,
            )
        for table in tables:
            run_dbt(
                dataset_id=DATASET_ID,
                table_id=table,
                dbt_command="test",
                target=target,
            )

        if not materialize_to_prod:
            return

        if update_metadata:
            for table, coverage in _COVERAGE.items():
                register_table_materialization_task(
                    dataset_id=DATASET_ID,
                    table_id=table,
                    coverage=coverage,
                    env="prod",
                    bq_project="basedosdados",
                )
    finally:
        # Covers early returns (no new data, dev-only) and any exception.
        shutil.rmtree(work_dir, ignore_errors=True)


# APRA releases the quarterly ADI performance statistics roughly 9 weeks after
# quarter end -- early-to-mid March, June, September and December. Poll across
# the first half of those months at 15:00 BRT; the source-poll guard no-ops
# until a new quarter lands.
# pyrefly: ignore [missing-attribute]
au_apra_adi_flow.deploy_schedules = [
    {
        "cron": "11 15 4,5,6,7,8,9,10,11,12,13,14 3,6,9,12 *",
        "timezone": "America/Sao_Paulo",
    }
]
# The clean step holds the full history (~52k rows) in pandas; 4Gi is ample,
# but be explicit so the pod is not silently capped by the pool default.
# pyrefly: ignore [missing-attribute]
au_apra_adi_flow.job_variables = {
    "memory_limit": "4Gi",
    "memory_request": "2Gi",
}
