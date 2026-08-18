"""
Flows for us_treasury_usaspending — Prefect 3.

USAspending.gov Award Data Archive: every federal contract and financial
assistance transaction, one zip per fiscal year per award family, rebuilt
monthly by the source.

**Only the current fiscal year is refreshed.** Closed fiscal years are frozen
once loaded — the archive still republishes them, but their content no longer
changes materially, and re-downloading ~50 GB every month to detect that would
be waste. The current year's partition is replaced wholesale on each run, so
late corrections to open-year records are picked up.

Staging upload therefore uses ``dump_mode="append"``, not ``"overwrite"``:
overwrite drops the whole staging table, which would delete the frozen
historical partitions this flow does not re-upload. Each fiscal year writes a
single deterministically-named parquet object, so re-uploading the current year
replaces exactly that object.

**Cost note.** The dbt models are ``materialized="table"``, so every run
rebuilds the full ~250M-row tables from staging. That is correct and simple but
not cheap at monthly cadence. If the bill matters more than the simplicity, the
models can move to an incremental ``insert_overwrite`` strategy keyed on
``fiscal_year``; the staging layout above already supports it.

Deploy: `.github/scripts/deploy_flows.py` auto-discovers `us_treasury_usaspending_flow`;
the dev pool ignores the schedule, the prod pool activates it.
"""

import shutil
import tempfile
from datetime import date

from prefect import flow

from pipelines.datasets.us_treasury_usaspending.constants import constants
from pipelines.datasets.us_treasury_usaspending.tasks import (
    get_latest_stamp,
    refresh_fiscal_year,
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

# Tables the flow refreshes. `dicionario` is reference metadata built from the
# DATA Act element dictionary at onboarding and does not change with the
# monthly archive, so the flow leaves it alone.
REFRESHED_TABLES = list(constants.AWARD_FAMILIES.value.values())

# Both transaction tables refresh monthly, so both carry the BD Pro rolling
# window: the most recent 6 months of action_date are pro-only, everything
# older is free. register_table_materialization_task recomputes
# free_end = source_end - free_lag on every run, rewrites both DateTimeRanges,
# and re-issues the BigQuery Row Access Policies, so the window slides forward
# on its own.
#
# part_bdpro requires BOTH a free (is_closed=False) and a pro (is_closed=True)
# Coverage to already exist on the table, or assert_coverage_topology raises
# before anything is written.
_COVERAGE = {
    table: PartBdpro(
        date_column=DateOnly(col=constants.COVERAGE_DATE_COLUMN.value),
        date_format=DateFormat.YEAR_MD,
        free_lag=FreeLag(unit="months", value=6),
    )
    for table in REFRESHED_TABLES
}


def current_fiscal_year(today: date | None = None) -> int:
    """US federal fiscal year of `today` — FY N runs Oct 1 N-1 to Sep 30 N."""
    today = today or date.today()
    return today.year + 1 if today.month >= 10 else today.year


@flow(name="us_treasury_usaspending", log_prints=True)
def us_treasury_usaspending_flow(
    materialize_to_prod: bool = True,
    update_metadata: bool = True,
    force_run: bool = False,
    fiscal_year: int | None = None,
) -> None:
    """Refresh the current fiscal year of USAspending award transactions.

    Args:
        materialize_to_prod: Continue past the dev materialization to write the
            prod staging bucket and run dbt against ``target="prod"``. Set False
            to exercise only the dev half — required for a safe test run, since
            the default writes production and applies the BD Pro paywall.
        update_metadata: After a successful prod materialization, register table
            coverage and commit the source update. Has no effect when
            ``materialize_to_prod`` is False.
        force_run: Materialize even when the source poll reports no new archive
            build.
        fiscal_year: Fiscal year to refresh. Defaults to the current one; pass
            an explicit year to backfill or repair a closed year.
    """
    target_fy = fiscal_year or current_fiscal_year()

    # pyrefly: ignore [unused-coroutine]
    rename_flow_run_dataset_table(
        prefix="Dump: ", dataset_id=DATASET_ID, table_id=f"FY{target_fy}"
    )

    work_dir = tempfile.mkdtemp(prefix="us_treasury_usaspending_")
    try:
        # The archive's publication stamp is the poll signal. It is a build
        # date rather than a coverage date, which is the honest reading here:
        # each build is a full snapshot current as of that day, and the source
        # exposes no cheaper coverage marker than downloading the files.
        stamp_iso = get_latest_stamp(fiscal_year=target_fy)

        has_new_data = poll_source_for_update_task(
            dataset_id=DATASET_ID,
            table_id=REFRESHED_TABLES[0],
            source_max_date=stamp_iso,
            env="prod",
            date_format="%Y-%m-%d",
            compare_against="table_update",
        )
        if not has_new_data and not force_run:
            return

        result = refresh_fiscal_year(
            work_dir=work_dir, fiscal_year=target_fy, stamp_iso=stamp_iso
        )
        print(f"row counts: {result['row_counts']}")

        # Dev: upload staging for every table, materialize every table, and
        # only then test. Tests read sibling models (the dictionary-coverage
        # test references the dicionario model), so interleaving run and test
        # per table fails on the first table in a clean environment.
        for table in REFRESHED_TABLES:
            upload_to_gcs(
                data_path=result[table],
                dataset_id=DATASET_ID,
                table_id=table,
                bucket_name="basedosdados-dev",
                dump_mode="append",
                source_format="parquet",
            )
            run_dbt(
                dataset_id=DATASET_ID,
                table_id=table,
                dbt_command="run",
                target="dev",
            )
        for table in REFRESHED_TABLES:
            run_dbt(
                dataset_id=DATASET_ID,
                table_id=table,
                dbt_command="test",
                target="dev",
            )

        if not materialize_to_prod:
            return

        for table in REFRESHED_TABLES:
            upload_to_gcs(
                data_path=result[table],
                dataset_id=DATASET_ID,
                table_id=table,
                bucket_name="basedosdados",
                dump_mode="append",
                source_format="parquet",
            )
            run_dbt(
                dataset_id=DATASET_ID,
                table_id=table,
                dbt_command="run",
                target="prod",
            )
        for table in REFRESHED_TABLES:
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
            # Last, and only after prod succeeded.
            commit_source_update_task(
                dataset_id=DATASET_ID,
                table_id=REFRESHED_TABLES[0],
                source_max_date=stamp_iso,
                env="prod",
                date_format="%Y-%m-%d",
                update_metadata=update_metadata,
                materialize_after_dump=materialize_to_prod,
            )
    finally:
        # Covers both early returns (no new build, dev-only) and any exception.
        # A process worker reuses its filesystem and the archives are gigabytes.
        shutil.rmtree(work_dir, ignore_errors=True)


# The archive is rebuilt monthly, in the first half of the month. Poll across a
# few days at 16:00 BRT; the source-poll guard no-ops until a new build lands.
# pyrefly: ignore [missing-attribute]
us_treasury_usaspending_flow.deploy_schedules = [
    {"cron": "0 16 8,9,10,11,12 * *", "timezone": "America/Sao_Paulo"}
]
# The clean step streams CSV in batches, but a 297-column arrow batch plus the
# parquet writer still wants headroom, and the archives are several GB on disk.
# pyrefly: ignore [missing-attribute]
us_treasury_usaspending_flow.job_variables = {"memory": "16Gi"}
