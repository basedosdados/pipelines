"""
Flows for us_treasury_usaspending — Prefect 3.

USAspending.gov Award Data Archive: every federal contract and financial
assistance transaction, one zip per fiscal year per award family, rebuilt
monthly by the source.

**Only the open fiscal year is refreshed** — plus, from October through
December, the year that just closed, which still takes late reports. Older
years are frozen once loaded: the archive still republishes them, but their
content no longer changes materially, and re-downloading ~50 GB every month to
detect that would be waste. A refreshed year's partition is replaced wholesale,
so late corrections are picked up.

Staging upload therefore uses ``dump_mode="append"``, not ``"overwrite"``:
overwrite drops the whole staging table, which would delete the frozen
historical partitions this flow does not re-upload. Because each parquet is
named after its archive, and the archive name carries the build stamp, the flow
deletes a refreshed year's staging objects before uploading it; otherwise each
new build would add a second copy of the year.

**Cost note.** The dbt models are ``materialized="table"``, so every run
rebuilds the full ~250M-row tables from staging. That is correct and simple but
not cheap at monthly cadence. If the bill matters more than the simplicity, the
models can move to an incremental ``insert_overwrite`` strategy keyed on
``fiscal_year``; the staging layout above already supports it.

Deploy: `.github/workflows/scripts/deploy_flows.py` auto-discovers `us_treasury_usaspending_flow`;
the dev pool ignores the schedule, the prod pool activates it.
"""

import shutil
import tempfile
from datetime import date

from prefect.schedules import Cron

from pipelines.datasets.us_treasury_usaspending.constants import constants
from pipelines.datasets.us_treasury_usaspending.tasks import (
    clear_staging_fiscal_year,
    get_archived_fiscal_years,
    get_latest_stamp,
    refresh_fiscal_year,
)
from pipelines.utils.flow import flow
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


def default_fiscal_years(today: date | None = None) -> list[int]:
    """Fiscal years a scheduled run should refresh, oldest first.

    The current one always. From October through December also the one that
    just closed: agencies keep reporting and correcting its last months for
    weeks after September 30, and the new year is not in the archive until the
    next monthly build anyway.
    """
    today = today or date.today()
    fy = current_fiscal_year(today)
    return [fy - 1, fy] if today.month >= 10 else [fy]


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
        fiscal_year: Fiscal year to refresh. Defaults to
            :func:`default_fiscal_years`, minus any year the archive does not
            list yet; pass an explicit year to backfill or repair a closed one.
    """
    target_fys = get_archived_fiscal_years(
        [fiscal_year] if fiscal_year else default_fiscal_years()
    )

    rename_flow_run_dataset_table(
        prefix="Dump: ",
        dataset_id=DATASET_ID,
        table_id="+".join(f"FY{fy}" for fy in target_fys),
    )

    work_dir = tempfile.mkdtemp(prefix="us_treasury_usaspending_")
    try:
        # The archive's publication stamp is the poll signal. It is a build
        # date rather than a coverage date, which is the honest reading here:
        # each build is a full snapshot current as of that day, and the source
        # exposes no cheaper coverage marker than downloading the files.
        stamp_iso = get_latest_stamp(fiscal_year=target_fys[-1])

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

        # Every year cleans into the same partitioned output directory, so the
        # last result's table paths cover all of them.
        result: dict = {}
        row_counts: dict = {}
        for fy in target_fys:
            result = refresh_fiscal_year(
                work_dir=work_dir, fiscal_year=fy, stamp_iso=stamp_iso
            )
            row_counts.update(result["row_counts"])
        print(f"row counts: {row_counts}")

        # The dev materialization is the pre-arm validation path, not part of a
        # production run: it rebuilds and re-tests every table in
        # basedosdados-dev, which nothing downstream reads, while prod runs the
        # same models and the same tests seconds later. On tables this size that
        # duplication is a large share of the daily BigQuery byte quota.
        if not materialize_to_prod:
            # Upload staging for every table, materialize every table, and only
            # then test. Tests read sibling models (the dictionary-coverage test
            # references the dicionario model), so interleaving run and test per
            # table fails on the first table in a clean environment.
            for table in REFRESHED_TABLES:
                for fy in target_fys:
                    clear_staging_fiscal_year(
                        bucket_name="basedosdados-dev",
                        table_id=table,
                        fiscal_year=fy,
                    )
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
            return

        for table in REFRESHED_TABLES:
            for fy in target_fys:
                clear_staging_fiscal_year(
                    bucket_name="basedosdados", table_id=table, fiscal_year=fy
                )
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
# few days; the source-poll guard no-ops until a new build lands.
#
# 17:20 BRT, not 16:00. The hour normally follows the source's publication time,
# but this source stamps builds with a date and no time, so the hour is free —
# and hour 16 is full: every five-minute slot from :00 to :55 is taken, so no
# minute there can be both unused and five minutes clear of a neighbour. Hour 17
# has nothing in it. Minute 0 is the worst instant to pick regardless, with 15+
# pipelines already on it across the day.
us_treasury_usaspending_flow.deploy_schedules = [
    Cron("20 17 8,9,10,11,12 * *", timezone="America/Sao_Paulo")
]
# The clean step streams CSV in batches, but a 297-column arrow batch plus the
# parquet writer still wants headroom, and the archives are several GB on disk.
# The work pool's template has no `memory` variable, so that key alone is
# dropped and the pod gets the 4Gi default; `memory_limit` is what applies.
us_treasury_usaspending_flow.job_variables = {
    "memory": "16Gi",
    "memory_limit": "16Gi",
    "memory_request": "4Gi",
}
