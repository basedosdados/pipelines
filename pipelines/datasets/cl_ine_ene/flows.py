"""Flows for cl_ine_ene — Prefect 3.

INE Chile's Encuesta Nacional de Empleo publishes one CSV per monthly moving
quarter, roughly a month after the quarter's last month. Each file is a distinct
period, so a run ingests only the periods that are new and appends them as fresh
``ano=/mes=`` partitions — unlike us_bls_cpi, whose source reships its whole
history every month.

That makes one failure mode invisible: INE periodically **rewrites the entire
back-series**, as it did when expansion factors were recalibrated on the Censo
2017 population projections. A poll that only looks at the newest quarter cannot
see it. So every run also re-reads the oldest period and compares its row count
and weight total against what is already published; a mismatch stops the run and
asks for a deliberate ``full_refresh``, rather than quietly leaving 16 years of
superseded weights in the table.

Deploy: `.github/scripts/deploy_flows.py` auto-discovers `cl_ine_ene_flow`; the
dev pool ignores the schedule, the prod pool activates it.
"""

import shutil
import tempfile

from prefect import flow

from pipelines.datasets.cl_ine_ene.constants import constants
from pipelines.datasets.cl_ine_ene.tasks import (
    anchor_fingerprint,
    download_and_clean,
    last_ingested_period,
    probe_source_max_period,
)
from pipelines.utils.metadata.domain import (
    DateFormat,
    FreeLag,
    PartBdpro,
    YearMonth,
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
TABLE_ID = constants.TABLE_ID.value

# The ENE refreshes monthly, so `microdato` carries the BD Pro rolling window:
# the most recent 6 moving quarters are pro-only, everything older is free. Each
# run recomputes free_end = source_end - free_lag, rewrites both DateTimeRanges
# and re-issues the BigQuery Row Access Policies, so the window slides on its own.
#
# part_bdpro requires BOTH a free (is_closed=False) and a pro (is_closed=True)
# Coverage to already exist on the table, or assert_coverage_topology raises
# before anything is written.
#
# `dicionario` is a literal dbt model with no date column, so it takes no
# coverage spec and is never uploaded — only built.
_COVERAGE = {
    TABLE_ID: PartBdpro(
        date_column=YearMonth(year="ano", month="mes"),
        date_format=DateFormat.YEAR_MONTH,
        free_lag=FreeLag(unit="months", value=6),
    ),
}


@flow(name="cl_ine_ene", log_prints=True)
def cl_ine_ene_flow(
    materialize_to_prod: bool = True,
    update_metadata: bool = True,
    force_run: bool = False,
    full_refresh: bool = False,
    expected_anchor_rows: int = 106579,
    expected_anchor_weight: float = 16995191.002,
) -> None:
    """Ingest every moving quarter INE has published since the last run.

    Args:
        materialize_to_prod: Continue past the dev materialization to write the
            prod staging bucket and run dbt against ``target="prod"``. Set False
            to exercise only the dev half — required for a safe test run, since
            the default writes production.
        update_metadata: After a successful prod materialization, register table
            coverage and commit the source update. No effect when
            ``materialize_to_prod`` is False.
        force_run: Ingest even when the source poll reports no new quarter.
        full_refresh: Re-download and rebuild every period from 2010-02, and skip
            the recalibration check. This is what to re-run with when the check
            trips.
        expected_anchor_rows: Row count of the anchor period as published. A
            change means INE rewrote the back-series.
        expected_anchor_weight: Sum of ``fact_cal`` over the anchor period as
            published, rounded to 3 decimals. Changes on a recalibration even
            when the row count does not.
    """
    # pyrefly: ignore [unused-coroutine]
    rename_flow_run_dataset_table(
        prefix="Dump: ", dataset_id=DATASET_ID, table_id=TABLE_ID
    )

    work_dir = tempfile.mkdtemp(prefix="cl_ine_ene_")
    try:
        source_max = probe_source_max_period()
        print(f"source publishes up to {source_max}")

        has_new_data = poll_source_for_update_task(
            dataset_id=DATASET_ID,
            table_id=TABLE_ID,
            source_max_date=source_max,
            env="prod",
            date_format="%Y-%m",
            compare_against="coverage",
        )

        # Guard against a silent rewrite of the whole back-series. This runs BEFORE
        # the no-new-data return: a recalibration republishes the history without
        # adding a quarter, so the poll reports nothing new and that is precisely
        # the case the canary exists to catch. Skipped only on a full refresh,
        # which is the remedy rather than the thing being guarded.
        if not full_refresh:
            fingerprint = anchor_fingerprint(work_dir=work_dir)
            drifted = (
                fingerprint["rows"] != expected_anchor_rows
                or abs(fingerprint["weight_total"] - expected_anchor_weight)
                > 0.01
            )
            if drifted:
                raise RuntimeError(
                    f"The anchor period {fingerprint['period']} no longer matches what is "
                    f"published: {fingerprint['rows']} rows / weight {fingerprint['weight_total']} "
                    f"versus {expected_anchor_rows} / {expected_anchor_weight}. INE has "
                    "rewritten the back-series — most likely a recalibration of the expansion "
                    "factors. Re-run with full_refresh=True and update expected_anchor_rows / "
                    "expected_anchor_weight to the new values; ingesting only the newest "
                    "quarter would leave superseded weights in every older partition."
                )

        if not has_new_data and not force_run and not full_refresh:
            return

        series_start = (
            f"{constants.FIRST_PERIOD.value[0]}-"
            f"{constants.FIRST_PERIOD.value[1]:02d}"
        )
        if full_refresh:
            first = series_start
        else:
            # Resume from the quarter after the newest one already ingested, not
            # from the source's newest. The poll only reports THAT the source is
            # ahead, never by how much, so starting at source_max would silently
            # skip every period that appeared while the flow was paused or failing.
            bq_project = (
                "basedosdados" if materialize_to_prod else "basedosdados-dev"
            )
            last = last_ingested_period(bq_project=bq_project)
            if last is None:
                first = series_start
            else:
                year, month = (int(part) for part in last.split("-"))
                year, month = (
                    (year + 1, 1) if month == 12 else (year, month + 1)
                )
                first = f"{year}-{month:02d}"
            if first > source_max:
                print(
                    f"nothing to ingest: already hold {last}, source has {source_max}"
                )
                return
        result = download_and_clean(
            work_dir=work_dir, first=first, last=source_max
        )
        print(
            f"ingesting {result['periods']} period(s) from {first} to {source_max}"
        )

        # Keyed on what is actually being ingested, not on how the run was asked
        # for: any run starting at the series start rewrites every partition and
        # must replace the staging table. Keying it on `full_refresh` alone would
        # append all 197 periods on top of a populated table — duplicating it —
        # whenever the resume point could not be determined.
        dump_mode = "overwrite" if first == series_start else "append"

        commit_source_update_task(
            dataset_id=DATASET_ID,
            table_id=TABLE_ID,
            source_max_date=source_max,
            env="prod",
            date_format="%Y-%m",
            update_metadata=update_metadata,
            materialize_after_dump=materialize_to_prod,
        )

        # The dev materialization is the pre-arm validation path, not part of a
        # production run: prod rebuilds the same models seconds later.
        if not materialize_to_prod:
            upload_to_gcs(
                data_path=result["data_path"],
                dataset_id=DATASET_ID,
                table_id=TABLE_ID,
                bucket_name="basedosdados-dev",
                dump_mode=dump_mode,
                source_format="parquet",
            )
            # Build every table first, then test: the dicionario coverage test
            # reads microdato, so an interleaved run/test would test against a
            # sibling that does not exist yet in a clean environment.
            for table in constants.DBT_TABLES.value:
                run_dbt(
                    dataset_id=DATASET_ID,
                    table_id=table,
                    dbt_command="run",
                    target="dev",
                )
            for table in constants.DBT_TABLES.value:
                run_dbt(
                    dataset_id=DATASET_ID,
                    table_id=table,
                    dbt_command="test",
                    target="dev",
                )
            return

        upload_to_gcs(
            data_path=result["data_path"],
            dataset_id=DATASET_ID,
            table_id=TABLE_ID,
            bucket_name="basedosdados",
            dump_mode=dump_mode,
            source_format="parquet",
        )
        for table in constants.DBT_TABLES.value:
            run_dbt(
                dataset_id=DATASET_ID,
                table_id=table,
                dbt_command="run",
                target="prod",
            )
        for table in constants.DBT_TABLES.value:
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
        # Covers both early returns and any exception. A full refresh downloads
        # ~6.5 GB, which a process worker would otherwise keep on disk.
        shutil.rmtree(work_dir, ignore_errors=True)


# INE publishes a moving quarter about a month after its last month, on a date
# set by the official calendar (late in the month). Poll across a few end-of-month
# days at 16:52 BRT; the source poll no-ops until a new quarter appears.
# pyrefly: ignore [missing-attribute]
cl_ine_ene_flow.deploy_schedules = [
    {"cron": "52 16 27,28,29,30 * *", "timezone": "America/Sao_Paulo"}
]
# An incremental run holds one ~98k-row period in pandas; a full refresh holds one
# period at a time but downloads 6.5 GB. `memory` alone is NOT a variable of the
# work pool's job template and is dropped silently, capping the pod at the 4Gi
# default — memory_limit is the one that is actually applied.
# pyrefly: ignore [missing-attribute]
cl_ine_ene_flow.job_variables = {
    "memory_limit": "8Gi",
    "memory_request": "2Gi",
}
