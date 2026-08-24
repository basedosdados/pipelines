"""
Flows for us_sec_edgar — Prefect 3.

SEC EDGAR Financial Statement Data Sets. The SEC publishes one ZIP per calendar
quarter, roughly five weeks after quarter end, and never rewrites an earlier
one — so each run **appends one partition** (``dump_mode="append"``) rather than
replacing the history the way us_bls_cpi does.

Deploy: `.github/scripts/deploy_flows.py` auto-discovers `us_sec_edgar_flow`;
the dev pool ignores the schedule, the prod pool activates it.
"""

import shutil
import tempfile

from prefect import flow

from pipelines.datasets.us_sec_edgar.constants import constants
from pipelines.datasets.us_sec_edgar.tasks import (
    build_dicionario_task,
    download_and_clean,
    resolve_latest_quarter,
)
from pipelines.utils.metadata.domain import (
    DateFormat,
    FreeLag,
    PartBdpro,
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

# The four data tables refresh together and carry the BD Pro rolling window:
# the two most recent quarters are pro-only, everything older is free. With a
# quarter read as DATE(year, quarter * 3, 1), a six-month lag lands free_end on
# the last month of the quarter before last, so the window is exactly two
# quarters wide and slides forward on every release.
#
# part_bdpro requires BOTH a free (is_closed=False) and a pro (is_closed=True)
# Coverage to already exist on the table, or assert_coverage_topology raises
# before anything is written. Both were registered during onboarding.
#
# `dicionario` has no date column, so it takes no coverage spec at all.
_COVERAGE = {
    table: PartBdpro(
        date_column=YearQuarter(year="year", quarter="quarter"),
        date_format=DateFormat.YEAR_MONTH,
        free_lag=FreeLag(unit="months", value=6),
    )
    for table in constants.SOURCE_FILES.value.values()
}

# Every table uploads with dump_mode="append", including dicionario.
#
# dicionario IS a full rebuild each run — it is the union of the published table
# and the new quarter — but it must NOT use dump_mode="overwrite" to get that.
# The overwrite branch of _upload_to_gcs calls `tb.delete(mode="all")`, and
# mode="all" drops the MATERIALIZED PRODUCTION table, not just the staging
# external table. bd.Table resolves its BigQuery projects from the pod config
# rather than from bucket_name, so the dev half of the loop deletes the prod
# table too; a run with materialize_to_prod=False then never rebuilds it. That
# is exactly how basedosdados.us_sec_edgar.dicionario was lost on 2026-08-19,
# and the same defect previously bit us_fed_fred.
#
# append gives identical semantics here without any delete: build_dicionario
# writes a single fixed dicionario/data.parquet, and the append branch ends in
# st.upload(..., if_exists="replace"), which replaces that one blob wholesale.


@flow(name="us_sec_edgar", log_prints=True)
def us_sec_edgar_flow(
    materialize_to_prod: bool = True,
    update_metadata: bool = True,
    force_run: bool = False,
    year: int | None = None,
    quarter: int | None = None,
) -> None:
    """Append the newest quarterly Financial Statement Data Set and materialize.

    The source poll short-circuits the run when the SEC has not published a new
    quarter, which makes a scheduled run a cheap no-op between releases.

    Args:
        materialize_to_prod: Continue past the dev materialization to write the
            prod staging bucket and run dbt against ``target="prod"``. Set False
            to exercise only the dev half — required for a safe test run, since
            the default writes production.
        update_metadata: After a successful prod materialization, register table
            coverage, which also re-issues the BD Pro Row Access Policies. Has no
            effect when ``materialize_to_prod`` is False.
        force_run: Materialize even when the source poll reports no new quarter.
        year: Release year to ingest. Defaults to the newest the SEC serves.
        quarter: Release quarter to ingest, 1-4. Use with ``year`` to backfill a
            specific quarter; ``force_run`` is then usually wanted too, since the
            poll only looks at the newest.
    """
    # pyrefly: ignore [unused-coroutine]
    rename_flow_run_dataset_table(
        prefix="Dump: ", dataset_id=DATASET_ID, table_id="numeric_fact"
    )

    # Both or neither: `year=2020, quarter=None` silently ingesting the latest
    # quarter into production is not a backfill anyone asked for.
    if (year is None) != (quarter is None):
        raise ValueError("pass both year and quarter, or neither")
    if year is None or quarter is None:
        latest = resolve_latest_quarter()
        year, quarter = int(latest["year"]), int(latest["quarter"])
    if not 1 <= quarter <= 4:
        raise ValueError(f"quarter must be 1-4, got {quarter}")
    max_date = f"{year}-{quarter * 3:02d}"

    # Skip the run when the SEC has not published a newer quarter. Compared
    # against the table's coverage, which is a competência like max_date — not
    # against Table.Update, which is a wall clock.
    has_new_data = poll_source_for_update_task(
        dataset_id=DATASET_ID,
        table_id="numeric_fact",
        source_max_date=max_date,
        env="prod",
        date_format="%Y-%m",
        compare_against="coverage",
    )
    if not has_new_data and not force_run:
        return

    # Committed here, right after the poll confirms, rather than at the end:
    # poll_source_for_update never reads the source Update (it compares against
    # Coverage), so an early commit cannot strand a later run, and if this flow
    # dies mid-way the source metadata still records that the SEC published.
    #
    # Only when the poll actually saw something newer. A forced run and an
    # explicit backfill of an older quarter both reach here with
    # has_new_data=False, and committing then would move
    # RawDataSource.Update.latest *backwards*.
    if has_new_data:
        commit_source_update_task(
            dataset_id=DATASET_ID,
            table_id="numeric_fact",
            source_max_date=max_date,
            env="prod",
            date_format="%Y-%m",
            update_metadata=update_metadata,
            materialize_after_dump=materialize_to_prod,
        )

    work_dir = tempfile.mkdtemp(prefix="us_sec_edgar_")
    try:
        result = download_and_clean(
            work_dir=work_dir, year=year, quarter=quarter
        )
        tables = constants.TABLES.value

        for environment, bucket, dbt_target in (
            ("dev", "basedosdados-dev", "dev"),
            ("prod", "basedosdados", "prod"),
        ):
            if environment == "prod" and not materialize_to_prod:
                return

            # Rebuilt per environment so the union starts from that project's
            # own published table rather than the other one's copy of it.
            result["dicionario"] = build_dicionario_task(
                work_dir=work_dir,
                observed=result["observed"],
                billing_project_id=bucket,
            )

            # Build every table before testing any of them. The tests are
            # cross-table — numeric_fact and presentation have relationships
            # onto submission, and three tables check custom_dictionary_coverage
            # against dicionario — so testing inside this loop would run a
            # table's tests before its sibling exists.
            for table in tables:
                upload_to_gcs(
                    data_path=result[table],
                    dataset_id=DATASET_ID,
                    table_id=table,
                    bucket_name=bucket,
                    dump_mode="append",
                    source_format="parquet",
                )
                run_dbt(
                    dataset_id=DATASET_ID,
                    table_id=table,
                    dbt_command="run",
                    target=dbt_target,
                )
            for table in tables:
                run_dbt(
                    dataset_id=DATASET_ID,
                    table_id=table,
                    dbt_command="test",
                    target=dbt_target,
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
        # Covers both early returns and any exception. One quarter is ~700 MB
        # of TSV plus its parquet; a process worker reuses its filesystem.
        shutil.rmtree(work_dir, ignore_errors=True)


# The SEC posts a quarter roughly five weeks after it ends: 2026Q1 landed
# 2026-04-09 and 2026Q2 on 2026-07-06. Poll across the first half of the month
# after each quarter's release window; the source-poll guard no-ops until a new
# quarter actually appears.
# pyrefly: ignore [missing-attribute]
us_sec_edgar_flow.deploy_schedules = [
    {
        "cron": "55 16 5,8,11,14,17,20 1,4,7,10 *",
        "timezone": "America/Sao_Paulo",
    }
]
# num.txt alone is ~600 MB of TSV, held as an arrow table while it is written.
# pyrefly: ignore [missing-attribute]
us_sec_edgar_flow.job_variables = {"memory": "8Gi"}
