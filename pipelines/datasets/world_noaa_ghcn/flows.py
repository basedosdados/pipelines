"""
Flows for world_noaa_ghcn — Prefect 3.

NOAA GHCN-Daily station observations. NCEI rewrites the current year's archive
daily and reconstructs the **whole** archive weekly, so this runs weekly and
rebuilds a trailing window of year-partitions rather than the full 3.19 billion
rows.

Two properties of the source shape this flow:

* **File modification times cannot detect change.** The weekly reconstruction
  re-stamps every one of the 264 ``by_year`` files with the same timestamp, so
  an mtime diff marks everything as changed on every run. The run is instead
  guarded by the source's max coverage date, read from the data itself.
* **The archive is partitioned by year and only recent years move.** Uploading
  with ``dump_mode="append"`` replaces just the blobs for the years in the
  window and leaves the other ~260 untouched; dbt then rebuilds the table from
  all of them. ``dump_mode="overwrite"`` would delete the staging table and
  every blob under it, destroying the whole backfill — it must never be used
  here.

A trailing window cannot follow the weekly reconstruction of *old* years, so
``full_refresh`` rebuilds all 264 partitions. That is a multi-hour run and is
meant to be triggered deliberately, not scheduled.

Deploy: `.github/scripts/deploy_flows.py` auto-discovers `world_noaa_ghcn_flow`;
the dev pool ignores the schedule, the prod pool activates it.
"""

import shutil
import tempfile

from prefect import flow

from pipelines.datasets.world_noaa_ghcn.constants import (
    ALL_TABLES,
    DATASET_ID,
)
from pipelines.datasets.world_noaa_ghcn.tasks import (
    download_and_clean,
    log_source_version,
    source_max_date,
)
from pipelines.datasets.world_noaa_ghcn.utils import refresh_years
from pipelines.utils.metadata.domain import (
    AllFree,
    DateFormat,
    DateOnly,
    FreeLag,
    PartBdpro,
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

# BD Pro rolling window.
#
# Data Basis paywalls the most recent window of any table that refreshes
# monthly or more often; everything older stays free. This dataset refreshes
# weekly, so `observation` -- the high-frequency table -- carries the window:
# the most recent 6 months are pro-only, the other ~113 years are free.
#
# The window rolls on its own. Every run recomputes free_end = source_end -
# free_lag, rewrites both DateTimeRanges, and re-issues the BigQuery Row Access
# Policies. There is nothing weekly to do by hand and the dbt model is
# untouched -- the paywall lives in the Row Access Policies, not in SQL.
#
# part_bdpro requires BOTH a free (is_closed=False) and a pro (is_closed=True)
# Coverage to already exist on the table, or assert_coverage_topology raises
# before anything is written. `code/set_bdpro_coverage.py` creates the pro
# Coverage; it has been run against staging and prod.
#
# `station_element_inventory` is annual in granularity and stays fully free, as
# lower-frequency tables in the same dataset do. `station` and `dicionario`
# have no date column, so they take no coverage spec at all.
_COVERAGE = {
    "observation": PartBdpro(
        date_column=DateOnly(col="date"),
        date_format=DateFormat.YEAR_MD,
        free_lag=FreeLag(unit="months", value=6),
    ),
    "station_element_inventory": AllFree(
        date_column=YearOnly(col="last_year"), date_format=DateFormat.YEAR
    ),
}


@flow(name="world_noaa_ghcn", log_prints=True)
def world_noaa_ghcn_flow(
    materialize_to_prod: bool = True,
    update_metadata: bool = True,
    force_run: bool = False,
    full_refresh: bool = False,
) -> None:
    """Refresh GHCN-Daily observations and station metadata.

    Rebuilds a trailing window of year-partitions (or every one of them when
    ``full_refresh`` is set), uploads them alongside the three small metadata
    tables, and materializes all four models.

    Args:
        materialize_to_prod: Continue past the dev materialization to write the
            prod staging bucket and run dbt against ``target="prod"``. Set False
            to exercise only the dev half — required for a safe test run, since
            the default writes production.
        update_metadata: After a successful prod materialization, register table
            coverage and commit the source update. No effect when
            ``materialize_to_prod`` is False.
        force_run: Materialize even when NCEI has published nothing newer.
        full_refresh: Rebuild all 264 year-partitions instead of the trailing
            window. Several hours; use when NCEI reprocesses historical data.
    """
    # pyrefly: ignore [unused-coroutine]
    rename_flow_run_dataset_table(
        prefix="Dump: ", dataset_id=DATASET_ID, table_id="observation"
    )

    work_dir = tempfile.mkdtemp(prefix="world_noaa_ghcn_")
    try:
        log_source_version()
        years = refresh_years(full=full_refresh)
        print(
            f"rebuilding {len(years)} year-partition(s): {years[0]}-{years[-1]}"
        )

        paths = download_and_clean(work_dir=work_dir, years=years)
        max_date = source_max_date(work_dir=work_dir, years=years)
        print(f"source max coverage date: {max_date}")

        # Skip when NCEI has published nothing newer. The download above is one
        # ~180 MB archive per window year, which is the cost of learning the
        # real coverage date: the version string moves whenever NCEI touches
        # the archive, so it cannot tell a new observation day from a rerun.
        has_new_data = poll_source_for_update_task(
            dataset_id=DATASET_ID,
            table_id="observation",
            source_max_date=max_date,
            env="prod",
            date_format="%Y-%m-%d",
            compare_against="coverage",
        )
        if not has_new_data and not force_run:
            print("no new data at the source; nothing to materialize")
            return

        commit_source_update_task(
            dataset_id=DATASET_ID,
            table_id="observation",
            source_max_date=max_date,
            env="prod",
            date_format="%Y-%m-%d",
            update_metadata=update_metadata,
            materialize_after_dump=materialize_to_prod,
        )

        bucket = (
            "basedosdados-dev" if not materialize_to_prod else "basedosdados"
        )
        target = "dev" if not materialize_to_prod else "prod"

        # dump_mode is "append" so the ~260 year-partitions outside the window
        # survive; "overwrite" deletes the staging table and every blob under
        # it. Upload every table first, then run, then test: the tests read
        # sibling models, so interleaving would test a table before its
        # siblings were rebuilt.
        for table in ALL_TABLES:
            upload_to_gcs(
                data_path=paths[table],
                dataset_id=DATASET_ID,
                table_id=table,
                bucket_name=bucket,
                dump_mode="append",
                source_format="parquet",
            )
        for table in ALL_TABLES:
            run_dbt(
                dataset_id=DATASET_ID,
                table_id=table,
                dbt_command="run",
                target=target,
            )
        for table in ALL_TABLES:
            run_dbt(
                dataset_id=DATASET_ID,
                table_id=table,
                dbt_command="test",
                target=target,
            )

        if materialize_to_prod and update_metadata:
            for table, coverage in _COVERAGE.items():
                register_table_materialization_task(
                    dataset_id=DATASET_ID,
                    table_id=table,
                    coverage=coverage,
                    env="prod",
                    bq_project="basedosdados",
                )
    finally:
        # Covers both early returns and any exception. A window run downloads a
        # few hundred MB and writes ~250 MB of parquet; a full refresh is 14 GB.
        shutil.rmtree(work_dir, ignore_errors=True)


# NCEI rewrites the current year daily and reconstructs the archive weekly.
# Weekly matches the reconstruction cadence and keeps the cost of rebuilding a
# 3.19bn-row table proportionate; the source-poll guard makes a run with no new
# observation day a cheap no-op. Tuesday 09:38 BRT is an otherwise-free slot.
# pyrefly: ignore [missing-attribute]
world_noaa_ghcn_flow.deploy_schedules = [
    {"cron": "38 9 * * 2", "timezone": "America/Sao_Paulo"}
]
# Sized to the clean step: one year-partition is read into arrow whole, and the
# largest is ~37M rows. `memory` alone is silently ignored by the work pool's
# job template, which defaults to 4Gi — `memory_limit` is the one that applies.
# pyrefly: ignore [missing-attribute]
world_noaa_ghcn_flow.job_variables = {
    "memory": "12Gi",
    "memory_limit": "12Gi",
    "memory_request": "4Gi",
}
