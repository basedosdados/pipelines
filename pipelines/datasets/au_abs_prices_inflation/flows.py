"""
Flows for au_abs_prices_inflation — Prefect 3.

The dataset spans the whole ABS "Price indexes and inflation" topic, and each
ABS release has its own publication cadence, its own landing page and its own
fact table. So there is **one flow per release**, each polling its own source
and owning its own table: a release that fails to parse cannot block the
others, and the crons spread naturally across the quarter.

Every ABS release ships the full history in its time-series spreadsheets, so
each run is a **full replace** (``dump_mode="overwrite"``), not an incremental
append. The source poll short-circuits a run until ABS publishes a newer
period, which makes a scheduled run a cheap no-op between releases.

Deploy: ``.github/scripts/deploy_flows.py`` auto-discovers every flow defined
here; the dev pool ignores the schedules, the prod pool activates them.
"""

import shutil
import tempfile

from prefect import flow

from pipelines.datasets.au_abs_prices_inflation.constants import constants
from pipelines.datasets.au_abs_prices_inflation.tasks import (
    clean_cpi,
    clean_price_release,
    download_cpi,
    download_price_release,
)
from pipelines.utils.metadata.domain import (
    AllFree,
    DateFormat,
    FreeLag,
    PartBdpro,
    YearMonth,
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
TABLE_ID = constants.TABLE_ID.value

# Every ABS release republishes its full history, so each run is a full
# replace. That is `append`, not `overwrite`, despite the names.
#
# `dump_mode="overwrite"` calls `tb.delete(mode="all")`, which drops the
# MATERIALIZED PRODUCTION table, not just the staging external table -- and
# `bd.Table` resolves its projects from the worker's config.toml rather than
# from `bucket_name`, so it fires from the dev half too. A dev-only validation
# run (`materialize_to_prod=False`) would delete the prod table and then return
# before rebuilding it, reporting every task Completed. Confirmed on
# us_sec_edgar, 2026-08-19.
#
# `append` ends in `st.upload(..., if_exists="replace")`, which replaces each
# blob wholesale. Since every partition writes one `data.parquet` under
# `year=YYYY/` and each release ships the whole history, the file names are
# identical run to run and the semantics are the same -- without the delete.
_DUMP_MODE = "append"

# ---------------------------------------------------------------------------
# Consumer Price Index (former catalogue 6401.0) — monthly release
# ---------------------------------------------------------------------------
# Coverage spec per table.
#
# `cpi_monthly` is the high-frequency table, so it carries the BD Pro rolling
# window: the most recent 6 months are pro-only, everything older is free.
# Each run recomputes free_end = source_end - free_lag, rewrites both
# DateTimeRanges, and re-issues the BigQuery Row Access Policies, so the window
# slides forward on its own. part_bdpro requires BOTH a free (is_closed=False)
# and a pro (is_closed=True) Coverage to already exist on the table, or
# assert_coverage_topology raises before anything is written.
#
# `cpi_quarterly` refreshes only quarterly (less than monthly), so it stays
# fully free.
_CPI_COVERAGE = {
    "cpi_quarterly": AllFree(
        date_column=YearQuarter(year="year", quarter="quarter"),
        date_format=DateFormat.YEAR_MONTH,
    ),
    "cpi_monthly": PartBdpro(
        date_column=YearMonth(year="year", month="month"),
        date_format=DateFormat.YEAR_MONTH,
        free_lag=FreeLag(unit="months", value=6),
    ),
}


@flow(name="au_abs_prices_inflation_cpi", log_prints=True)
def au_abs_prices_inflation_cpi_flow(
    materialize_to_prod: bool = True,
    update_metadata: bool = True,
    force_run: bool = False,
) -> None:
    """Download the current ABS CPI release, rebuild both tables, materialize them.

    ABS ships the full history on every release, so each run is a full replace
    (``dump_mode="overwrite"``). The source poll short-circuits the run when ABS
    has not published a new month, making a scheduled run a cheap no-op between
    releases.

    Args:
        materialize_to_prod: Continue past the dev materialization to write the
            prod staging bucket and run dbt against ``target="prod"``. Set False
            to exercise only the dev half — required for a safe test run, since
            the default writes production.
        update_metadata: After a successful prod materialization, register table
            coverage and commit the source update. No effect when
            ``materialize_to_prod`` is False.
        force_run: Materialize even when the source poll reports no new month.
    """
    # pyrefly: ignore [unused-coroutine]
    rename_flow_run_dataset_table(
        prefix="Dump: ", dataset_id=DATASET_ID, table_id="cpi"
    )

    work_dir = tempfile.mkdtemp(prefix="au_abs_prices_inflation_cpi_")
    try:
        input_dir = download_cpi(work_dir=work_dir)
        result = clean_cpi(work_dir=work_dir, input_dir=input_dir)
        max_ym = result["max_year_month"]

        # Skip the run when ABS has not published a newer month (unless forced).
        has_new_data = poll_source_for_update_task(
            dataset_id=DATASET_ID,
            table_id="cpi_monthly",
            source_max_date=max_ym,
            env="prod",
            date_format="%Y-%m",
            compare_against="coverage",
        )
        if not has_new_data and not force_run:
            return

        # Comita o Update da fonte já aqui, antes de baixar/materializar: se o
        # flow falhar no meio, o metadado da fonte ainda reflete que havia dado
        # novo publicado, mesmo que a tabela não tenha sido atualizada.
        commit_source_update_task(
            dataset_id=DATASET_ID,
            table_id="cpi_monthly",
            source_max_date=max_ym,
            env="prod",
            date_format="%Y-%m",
            update_metadata=update_metadata,
            materialize_after_dump=materialize_to_prod,
        )

        tables = [TABLE_ID[f] for f in constants.SOURCE_TABLES.value]

        # The dev materialization is the pre-arm validation path, not part of a
        # production run: it rebuilds and re-tests every table in
        # basedosdados-dev, which nothing downstream reads. Running it on an
        # armed run doubled the BigQuery bytes billed for no signal — prod
        # runs the same models and the same tests seconds later.
        if not materialize_to_prod:
            # Dev: upload staging + materialize/test.
            for table in tables:
                upload_to_gcs(
                    data_path=result[table],
                    dataset_id=DATASET_ID,
                    table_id=table,
                    bucket_name="basedosdados-dev",
                    dump_mode=_DUMP_MODE,
                    source_format="parquet",
                )
                run_dbt(
                    dataset_id=DATASET_ID,
                    table_id=table,
                    dbt_command="run/test",
                    target="dev",
                )
            return

        # Prod: upload staging + materialize/test.
        for table in tables:
            upload_to_gcs(
                data_path=result[table],
                dataset_id=DATASET_ID,
                table_id=table,
                bucket_name="basedosdados",
                dump_mode=_DUMP_MODE,
                source_format="parquet",
            )
            run_dbt(
                dataset_id=DATASET_ID,
                table_id=table,
                dbt_command="run/test",
                target="prod",
            )

        if update_metadata:
            for table, coverage in _CPI_COVERAGE.items():
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


# ABS publishes the monthly CPI in the last week of each month (moving to the
# 4th Wednesday from Feb 2027). Poll across the last week at 16:00 BRT; the
# source-poll guard no-ops until a new month lands.
# pyrefly: ignore [missing-attribute]
au_abs_prices_inflation_cpi_flow.deploy_schedules = [
    {"cron": "15 16 22,23,24,25,26,27,28 * *", "timezone": "America/Sao_Paulo"}
]

# ---------------------------------------------------------------------------
# The other five ABS releases, one flow each
# ---------------------------------------------------------------------------
# Every one of these is quarterly, so every table stays AllFree: the BD Pro
# rolling window applies only to tables refreshed monthly or more often. That
# also means none of them issues Row Access Policies.
#
# The poll compares a "%Y-%m" string against the registered coverage, and the
# coverage for a YearQuarter column is stored as MAX(DATE(year, quarter * 3, 1))
# formatted the same way -- so `clean_all` reports the release's latest period
# as a year-MONTH. Polling at a coarser granularity than the coverage is stored
# at compares mismatched clocks, and the guard then either never fires or fires
# on every run.
_RELEASE_COVERAGE = AllFree(
    date_column=YearQuarter(year="year", quarter="quarter"),
    date_format=DateFormat.YEAR_MONTH,
)

# Poll windows follow each release's published calendar, read off its landing
# page. The minutes are deliberately distinct and none is 0: same-instant runs
# compete for BigQuery slots, and if the daily quota trips they all fail
# together. The source poll no-ops until a new quarter lands, so a generous
# window costs only a download.
_RELEASE_SCHEDULE = {
    # Released mid-February, May, August and November (18 Nov 2026, 17 Feb 2027).
    "wage_price_index": "17 14 16,17,18,19,20,21,22 2,5,8,11 *",
    # Released end of January, April, July and October (30 Oct 2026, 29 Jan 2027).
    "producer_price_index": "27 14 27,28,29,30,31 1,4,7,10 *",
    # Released a day before Producer (29 Oct 2026, 28 Jan 2027).
    "international_trade_price_index": "32 14 26,27,28,29,30,31 1,4,7,10 *",
    # Released in the first week of February, May, August and November.
    "living_cost_index": "37 14 1,2,3,4,5,6,7,8 2,5,8,11 *",
    # Released in the first fortnight of March, June, September and December.
    "dwelling_value": "42 14 1,2,3,4,5,6,7,8,9,10,11,12 3,6,9,12 *",
}


def _build_release_flow(release: str):
    """Build one ABS release's flow.

    A factory rather than five copies of the same body. Prefect's deploy script
    picks up any module-level ``Flow`` whose function is defined in this file,
    and the inner function is, so a factory deploys exactly like a top-level
    flow -- the same pattern ``pipelines/crawler/ibge_inflacao`` uses.
    """

    @flow(name=f"au_abs_prices_inflation_{release}", log_prints=True)
    def _release_flow(
        materialize_to_prod: bool = True,
        update_metadata: bool = True,
        force_run: bool = False,
    ) -> None:
        """Download the current ABS release, rebuild its table, materialize it.

        ABS ships the full history on every release, so each run is a full
        replace. The source poll short-circuits the run when ABS has not
        published a new quarter, making a scheduled run a cheap no-op between
        releases.

        Args:
            materialize_to_prod: Continue past the dev materialization to write
                the prod staging bucket and run dbt against ``target="prod"``.
                Set False to exercise only the dev half -- required for a safe
                test run, since the default writes production.
            update_metadata: After a successful prod materialization, register
                table coverage and commit the source update. No effect when
                ``materialize_to_prod`` is False.
            force_run: Materialize even when the poll reports no new quarter.
        """
        # pyrefly: ignore [unused-coroutine]
        rename_flow_run_dataset_table(
            prefix="Dump: ", dataset_id=DATASET_ID, table_id=release
        )

        work_dir = tempfile.mkdtemp(
            prefix=f"au_abs_prices_inflation_{release}_"
        )
        try:
            input_dir = download_price_release(
                release=release, work_dir=work_dir
            )
            result = clean_price_release(
                release=release, work_dir=work_dir, input_dir=input_dir
            )
            max_ym = result[f"{release}__max_year_month"]

            has_new_data = poll_source_for_update_task(
                dataset_id=DATASET_ID,
                table_id=release,
                source_max_date=max_ym,
                env="prod",
                date_format="%Y-%m",
                compare_against="coverage",
            )
            if not has_new_data and not force_run:
                return

            # Commit the source Update before materializing: if the flow dies
            # midway, the source metadata still records that ABS had published
            # a new quarter, even though the table was not refreshed.
            commit_source_update_task(
                dataset_id=DATASET_ID,
                table_id=release,
                source_max_date=max_ym,
                env="prod",
                date_format="%Y-%m",
                update_metadata=update_metadata,
                materialize_after_dump=materialize_to_prod,
            )

            # The dev materialization is the pre-arm validation path, not part
            # of a production run: it rebuilds and re-tests the table in
            # basedosdados-dev, which nothing downstream reads. An armed run
            # skips it -- prod runs the same model and tests seconds later.
            bucket = (
                "basedosdados" if materialize_to_prod else "basedosdados-dev"
            )
            target = "prod" if materialize_to_prod else "dev"
            upload_to_gcs(
                data_path=result[release],
                dataset_id=DATASET_ID,
                table_id=release,
                bucket_name=bucket,
                dump_mode=_DUMP_MODE,
                source_format="parquet",
            )
            run_dbt(
                dataset_id=DATASET_ID,
                table_id=release,
                dbt_command="run/test",
                target=target,
            )
            if not materialize_to_prod:
                return

            if update_metadata:
                register_table_materialization_task(
                    dataset_id=DATASET_ID,
                    table_id=release,
                    coverage=_RELEASE_COVERAGE,
                    env="prod",
                    bq_project="basedosdados",
                )
        finally:
            # Covers early returns (no new data, dev-only) and any exception.
            shutil.rmtree(work_dir, ignore_errors=True)

    # pyrefly: ignore [missing-attribute]
    _release_flow.deploy_schedules = [
        {"cron": _RELEASE_SCHEDULE[release], "timezone": "America/Sao_Paulo"}
    ]
    return _release_flow


wage_price_index_flow = _build_release_flow("wage_price_index")
producer_price_index_flow = _build_release_flow("producer_price_index")
international_trade_price_index_flow = _build_release_flow(
    "international_trade_price_index"
)
living_cost_index_flow = _build_release_flow("living_cost_index")
dwelling_value_flow = _build_release_flow("dwelling_value")
