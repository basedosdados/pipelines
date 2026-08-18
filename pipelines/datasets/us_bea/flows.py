"""Flows for us_bea — Prefect 3.

US Bureau of Economic Analysis (BEA) economic accounts, pulled from the BEA REST
API. BEA benchmark revisions rewrite historical values, so each run re-fetches
the full history and overwrites staging (``dump_mode="overwrite"``), rather than
appending. A single flow downloads once and rebuilds all six tables.

Coverage tier: all six tables are ``AllFree``. ``nipa`` is a mixed-frequency
table (annual/quarterly rows have a NULL ``month``), so a monthly Row Access
Policy would paywall those rows forever (``DATE(year, month, 1)`` is NULL) — it
is therefore NOT paywalled. ``nipa``'s ``(year, month)`` coverage still drives
the monthly source poll; it just does not gate access. ``dicionario`` has no
date column, so it takes no coverage spec. If a rolling BD Pro paywall is wanted
later, add an end-of-period ``date`` column to ``nipa`` and key the policy on it.

Deploy: ``.github/scripts/deploy_flows.py`` auto-discovers ``us_bea_flow``; the
dev pool ignores the schedule, the prod pool activates it (paused).
"""

import shutil
import tempfile

from prefect import flow

from pipelines.datasets.us_bea.constants import constants
from pipelines.datasets.us_bea.tasks import clean_bea
from pipelines.utils.metadata.domain import (
    AllFree,
    DateFormat,
    YearMonth,
    YearOnly,
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

# The table whose coverage drives the source poll: nipa is the high-frequency
# (monthly) table.
POLL_TABLE = "nipa"

# Coverage spec per table (see module docstring). `dicionario` is intentionally
# absent — no date column, no coverage.
_COVERAGE = {
    # nipa mixes A/Q/M frequencies (annual/quarterly rows have NULL month), so a
    # monthly Row Access Policy would paywall those rows forever. Kept AllFree;
    # the YearMonth date column still drives the monthly source poll.
    "nipa": AllFree(
        date_column=YearMonth(year="year", month="month"),
        date_format=DateFormat.YEAR_MONTH,
    ),
    "gdp_by_industry": AllFree(
        date_column=YearOnly(col="year"), date_format=DateFormat.YEAR
    ),
    "regional_state": AllFree(
        date_column=YearOnly(col="year"), date_format=DateFormat.YEAR
    ),
    "regional_county": AllFree(
        date_column=YearOnly(col="year"), date_format=DateFormat.YEAR
    ),
    "regional_metro": AllFree(
        date_column=YearOnly(col="year"), date_format=DateFormat.YEAR
    ),
}


@flow(name="us_bea", log_prints=True)
def us_bea_flow(
    materialize_to_prod: bool = True,
    update_metadata: bool = True,
    force_run: bool = False,
) -> None:
    """Download the BEA economic accounts, rebuild all six tables, materialize.

    BEA benchmark revisions rewrite history, so each run is a full replace
    (``dump_mode="overwrite"``) rather than an incremental append. The source
    poll short-circuits the run when BEA has not published a newer month, which
    makes a scheduled run a cheap no-op between releases.

    Args:
        materialize_to_prod: Continue past the dev materialization to write the
            prod staging bucket and run dbt against ``target="prod"``. Set False
            to exercise only the dev half — required for a safe test run, since
            the default writes production.
        update_metadata: After a successful prod materialization, register table
            coverage and commit the source update. Has no effect when
            ``materialize_to_prod`` is False.
        force_run: Materialize even when the source poll reports no new month.
    """
    # pyrefly: ignore [unused-coroutine]
    rename_flow_run_dataset_table(
        prefix="Dump: ", dataset_id=DATASET_ID, table_id="us_bea"
    )

    work_dir = tempfile.mkdtemp(prefix="us_bea_")
    try:
        result = clean_bea(work_dir=work_dir)
        max_ym = result["max_year_month"]

        # Skip the run when BEA has not published a newer month (unless forced).
        has_new_data = poll_source_for_update_task(
            dataset_id=DATASET_ID,
            table_id=POLL_TABLE,
            source_max_date=max_ym,
            env="prod",
            date_format="%Y-%m",
            compare_against="coverage",
        )
        if not has_new_data and not force_run:
            return

        # Commit the source Update up front: if the flow fails mid-way, the
        # source metadata still records that new data had been published.
        commit_source_update_task(
            dataset_id=DATASET_ID,
            table_id=POLL_TABLE,
            source_max_date=max_ym,
            env="prod",
            date_format="%Y-%m",
            update_metadata=update_metadata,
            materialize_after_dump=materialize_to_prod,
        )

        tables = constants.ALL_TABLES.value

        # Dev: upload staging + materialize/test.
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

        # Prod: upload staging + materialize/test.
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
    finally:
        # Covers both early returns (no new data, dev-only) and any exception.
        shutil.rmtree(work_dir, ignore_errors=True)


# BEA refreshes NIPA monthly (personal income near month-end; benchmark and GDP
# revisions land irregularly). Poll across a few late-month days at 16:00 BRT;
# the source-poll guard no-ops until a new month actually appears.
# pyrefly: ignore [missing-attribute]
us_bea_flow.deploy_schedules = [
    {"cron": "0 16 25,26,27,28 * *", "timezone": "America/Sao_Paulo"}
]
# The clean step streams the ~50M-row county family through pandas/arrow in
# 500k-row flushes; give the worker headroom.
# pyrefly: ignore [missing-attribute]
us_bea_flow.job_variables = {"memory": "8Gi"}
