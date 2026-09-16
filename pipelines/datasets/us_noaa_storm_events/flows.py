"""
Flows for us_noaa_storm_events — Prefect 3.

NOAA NCEI Storm Events Database. NCEI publishes one gzipped CSV per (family, year)
and **restates whole years in bulk**: the file name carries a ``c<YYYYMMDD>``
creation token, and its own README says a change in that token marks either new
data or a correction to an earlier year. When this pipeline was built, 71 of the
77 ``details`` files carried the same ``c20260323`` token and all 74 older
``locations`` files carried ``c20260707`` — two sweeps that rewrote nearly the
whole corpus at once.

So each run re-downloads and re-cleans **every** year and rewrites every year
partition. An append-only refresh keyed on the new month would miss every
correction and, worse, accumulate duplicates of the years that were rewritten.
Rebuilding everything is affordable here — 363 MB of gzipped CSV, 3.9M rows — and
it makes the restatement problem structurally impossible rather than inferred
from state this pipeline has nowhere to keep.

The BigQuery cost is unchanged either way: the dbt models are
``materialized="table"``, so they rebuild in full from the staging external table
on every run regardless of how many partitions moved.

``dump_mode="append"`` is load-bearing: ``"overwrite"`` calls
``tb.delete(mode="all")``, which drops the **production** table, and fires from
the dev half of the flow too. With ``"append"`` the upload ends in
``Storage.upload(if_exists="replace")``, which replaces each partition blob at its
own path — the same end state, without the delete.

Deploy: `.github/scripts/deploy_flows.py` auto-discovers `us_noaa_storm_events_flow`;
the dev pool ignores the schedule, the prod pool activates it.
"""

import shutil
import tempfile

from prefect import flow

from pipelines.datasets.us_noaa_storm_events.constants import constants
from pipelines.datasets.us_noaa_storm_events.tasks import (
    clean_corpus,
    download_corpus,
    probe_source,
)
from pipelines.utils.metadata.domain import (
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

DATASET_ID = constants.DATASET_ID.value
EVENT = constants.EVENT.value

# Coverage spec per table.
#
# All three data tables refresh monthly, so all three carry the BD Pro rolling
# window: the most recent slice is pro-only, everything older is free. Each run
# recomputes free_end = source_end - free_lag, rewrites both DateTimeRanges and
# re-issues the BigQuery Row Access Policies, so the window slides on its own.
#
# part_bdpro requires BOTH a free (is_closed=False) and a pro (is_closed=True)
# Coverage to already exist on the table, or assert_coverage_topology raises
# before anything is written.
#
# The date column differs by table, and the choice is driven by nulls: the Row
# Access Policy grants allUsers `<date_col> <= free_end`, and a NULL fails that
# comparison, so any row with no date would be paywalled forever.
#
# * event uses begin_datetime, which is non-null on all 2,041,816 rows — a true
#   6-month rolling window.
# * fatality has fatality_datetime, but it is NULL on 12 rows where the source
#   reports no day. Those 12 deaths would never become free, so the window keys
#   on `year` instead, which is the non-null partition column.
# * event_location has no date column at all beyond `year`.
#
# The two granularities differ slightly at the boundary — event's free window
# ends mid-month while the other two end at a year boundary — which can leave a
# recent fatality free while its event is not. That direction is harmless: it
# never exposes a row the pro window is meant to cover.
#
# `dicionario` has no date column, so it takes no coverage spec at all.
_FREE_LAG = FreeLag(unit="months", value=6)

_COVERAGE = {
    "event": PartBdpro(
        date_column=DateOnly(col="begin_datetime"),
        date_format=DateFormat.YEAR_MD,
        free_lag=_FREE_LAG,
    ),
    "fatality": PartBdpro(
        date_column=YearOnly(col="year"),
        date_format=DateFormat.YEAR,
        free_lag=_FREE_LAG,
    ),
    "event_location": PartBdpro(
        date_column=YearOnly(col="year"),
        date_format=DateFormat.YEAR,
        free_lag=_FREE_LAG,
    ),
}


@flow(name="us_noaa_storm_events", log_prints=True)
def us_noaa_storm_events_flow(
    materialize_to_prod: bool = True,
    update_metadata: bool = True,
    force_run: bool = False,
) -> None:
    """Refresh the NOAA Storm Events Database from the bulk CSV directory.

    Args:
        materialize_to_prod: Continue past the dev materialization to write the
            prod staging bucket and run dbt against ``target="prod"``. Set False
            to exercise only the dev half — required for a safe test run, since
            the default writes production.
        update_metadata: After a successful prod materialization, register table
            coverage and commit the source update. Has no effect when
            ``materialize_to_prod`` is False.
        force_run: Materialize even when the source poll reports nothing new.
            Needed for a release that only restates earlier years without adding
            a month, which leaves the max coverage date unmoved.
    """
    # pyrefly: ignore [unused-coroutine]
    rename_flow_run_dataset_table(
        prefix="Dump: ", dataset_id=DATASET_ID, table_id=EVENT
    )

    work_dir = tempfile.mkdtemp(prefix="us_noaa_storm_events_")
    try:
        probe = probe_source(work_dir=work_dir)
        max_date = probe["max_date"]
        print(f"source years {probe['years'][0]}-{probe['years'][-1]}")
        print(f"source max coverage date: {max_date}")

        # Skip the run when NCEI has not published a newer month. A scheduled run
        # is then a cheap no-op — note Prefect still reports COMPLETED, so read
        # the logs rather than the state to tell the two apart.
        #
        # This guard keys on coverage, so a release that only *corrects* earlier
        # years without adding a month does not trip it. NCEI adds the new month
        # to the current year on essentially every release, so a correction-only
        # release is rare, and the next month's run rebuilds everything anyway;
        # force_run covers the case where it needs picking up sooner.
        has_new_data = poll_source_for_update_task(
            dataset_id=DATASET_ID,
            table_id=EVENT,
            source_max_date=max_date,
            env="prod",
            date_format="%Y-%m-%d",
            compare_against="coverage",
        )
        if not has_new_data and not force_run:
            print("Não há novas atualizações na fonte original")
            return

        input_dir = download_corpus(work_dir=work_dir, probe=probe)
        result = clean_corpus(
            work_dir=work_dir, input_dir=input_dir, probe=probe
        )
        print(f"row counts: {result['row_counts']}")

        commit_source_update_task(
            dataset_id=DATASET_ID,
            table_id=EVENT,
            source_max_date=max_date,
            env="prod",
            date_format="%Y-%m-%d",
            update_metadata=update_metadata,
            materialize_after_dump=materialize_to_prod,
        )

        tables = constants.ALL_TABLES.value

        # Build EVERY table before testing ANY of them, in both environments.
        # event's custom_dictionary_coverage test reads dicionario via ref(), and
        # fatality and event_location both have a relationships test against
        # event. Interleaved run/test per table, those tests would run before the
        # sibling they reference exists and fail with "Not found: Table ...".
        # A re-run hides it, because a stale sibling from an earlier build
        # survives — so it only bites in a clean environment, which is prod.
        if not materialize_to_prod:
            for table in tables:
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
            for table in tables:
                run_dbt(
                    dataset_id=DATASET_ID,
                    table_id=table,
                    dbt_command="test",
                    target="dev",
                )
            return

        for table in tables:
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
    finally:
        # Covers both early returns (no new data, dev-only) and any exception.
        # The corpus is 363 MB of gzipped CSV plus ~390 MB of parquet.
        shutil.rmtree(work_dir, ignore_errors=True)


# NCEI refreshes the current year's files monthly, but on no fixed day — the
# creation tokens observed over 2026 fall on the 1st, 19th, 23rd, 25th, 27th and
# 28th. So poll daily and let the source-poll guard no-op; the probe step reads
# the directory listing plus one ~12 MB file, so a no-op run is cheap.
# The minute is chosen, not defaulted: hour 8 already holds 0 (twice), 15 and 30,
# so 52 keeps well clear. Defaulting to :00 piles every pipeline onto the same
# instant, where they compete for BigQuery slots and trip the daily quota together.
# pyrefly: ignore [missing-attribute]
us_noaa_storm_events_flow.deploy_schedules = [
    {"cron": "52 8 * * *", "timezone": "America/Sao_Paulo"}
]
# The clean step holds one year of rows in memory at a time — 2011 is the largest
# at ~76k events with their narratives — but the 363 MB of gzipped CSV and ~390 MB
# of parquet share the pod's disk.
# pyrefly: ignore [missing-attribute]
us_noaa_storm_events_flow.job_variables = {"memory": "4Gi"}
