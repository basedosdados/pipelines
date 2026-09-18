"""
Flows for us_eia_electricity — Prefect 3.

U.S. Energy Information Administration Forms EIA-860 (annual plant and generator
inventory) and EIA-923 (monthly generation, fuel consumption and fuel receipts).

**EIA republishes a report year rather than extending it.** A given year of
EIA-923 appears first as a within-year monthly file, then the following spring as
an *early release*, then as one or more *final revisions*; EIA-860 does the same
on an annual clock. Each publication supersedes the last. An append-only refresh
would double every month a revision restates, which is exactly the failure the
brief for this dataset warned about — so each run re-downloads every year and
rewrites every year partition from the newest file EIA serves. Rebuilding
everything makes the supersession problem structurally impossible instead of
inferred from state this pipeline has nowhere to keep, and it keeps the
``dicionario`` — which has no year partition — computed over the whole record.

The two forms are polled separately because they move on different clocks: 923
gains a month roughly monthly, 860 gains a year once a year. Either being newer
than what is registered is enough to run.

``dump_mode="append"`` is load-bearing: ``"overwrite"`` calls
``tb.delete(mode="all")``, which drops the **production** table, and fires from
the dev half of the flow too. With ``"append"`` the upload ends in
``Storage.upload(if_exists="replace")``, which replaces each partition blob at
its own path — the same end state, without the delete.

Deploy: `.github/scripts/deploy_flows.py` auto-discovers `us_eia_electricity_flow`; the dev
pool ignores the schedule, the prod pool activates it.
"""

import shutil
import tempfile

from prefect import flow

from pipelines.datasets.us_eia_electricity.constants import constants
from pipelines.datasets.us_eia_electricity.tasks import (
    clean_corpus,
    download_corpus,
    probe_source,
)
from pipelines.utils.metadata.domain import (
    AllFree,
    DateFormat,
    FreeLag,
    PartBdpro,
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

# The table each form is polled through, and the poll's date granularity.
POLL_TABLE = {
    "eia860": constants.GENERATOR.value,
    "eia923": constants.GENERATION_FUEL.value,
}

# Coverage spec per table.
#
# The house rule paywalls the most recent window of any table that refreshes
# monthly or more often, and leaves lower-frequency tables in the same dataset
# free. That splits this dataset exactly along the two forms:
#
# * EIA-923's generation_fuel and fuel_receipts_costs gain a month on roughly a
#   monthly clock, so both carry the BD Pro rolling window. Each run recomputes
#   free_end = source_end - free_lag, rewrites both DateTimeRanges and re-issues
#   the BigQuery Row Access Policies, so the window slides on its own and the
#   dbt models are untouched.
# * EIA-860's plant and generator are annual, so both stay fully free.
#
# part_bdpro requires BOTH a free (is_closed=False) and a pro (is_closed=True)
# Coverage to already exist on the table, or assert_coverage_topology raises
# before anything is written. Both are registered at onboarding.
#
# The Row Access Policy grants allUsers `<date_col> <= free_end`, and a NULL
# fails that comparison, so a row with no date would be paywalled forever. Both
# pro tables key on (year, month), and both were measured non-null on every row:
# generation_fuel's month comes from the melt itself, and every one of
# fuel_receipts_costs' 766,262 deliveries carries a report month.
#
# `dicionario` has no date column, so it takes no coverage spec at all.
_FREE_LAG = FreeLag(unit="months", value=6)

_COVERAGE = {
    constants.PLANT.value: AllFree(
        date_column=YearOnly(col="year"), date_format=DateFormat.YEAR
    ),
    constants.GENERATOR.value: AllFree(
        date_column=YearOnly(col="year"), date_format=DateFormat.YEAR
    ),
    constants.GENERATION_FUEL.value: PartBdpro(
        date_column=YearMonth(year="year", month="month"),
        date_format=DateFormat.YEAR_MONTH,
        free_lag=_FREE_LAG,
    ),
    constants.FUEL_RECEIPTS_COSTS.value: PartBdpro(
        date_column=YearMonth(year="year", month="month"),
        date_format=DateFormat.YEAR_MONTH,
        free_lag=_FREE_LAG,
    ),
}


@flow(name="us_eia_electricity", log_prints=True)
def us_eia_electricity_flow(
    materialize_to_prod: bool = True,
    update_metadata: bool = True,
    force_run: bool = False,
) -> None:
    """Refresh EIA Forms 860 and 923 from the published survey ZIPs.

    Args:
        materialize_to_prod: Continue past the dev materialization to write the
            prod staging bucket and run dbt against ``target="prod"``. Set False
            to exercise only the dev half — required for a safe test run, since
            the default writes production.
        update_metadata: After a successful prod materialization, register table
            coverage and commit the source updates. Has no effect when
            ``materialize_to_prod`` is False.
        force_run: Materialize even when both source polls report nothing new.
            Needed for a release that only restates earlier years without adding
            a period, which leaves the max coverage date unmoved.
    """
    # pyrefly: ignore [unused-coroutine]
    rename_flow_run_dataset_table(
        prefix="Dump: ",
        dataset_id=DATASET_ID,
        table_id=constants.GENERATION_FUEL.value,
    )

    work_dir = tempfile.mkdtemp(prefix="us_eia_electricity_")
    try:
        probe = probe_source(work_dir=work_dir)
        for form in ("eia860", "eia923"):
            print(
                f"{form}: report years {probe['years'][form][0]}-{probe['years'][form][-1]}, "
                f"max coverage date {probe['max_date'][form]}"
            )

        # Poll each form through its own table. A scheduled run is a cheap no-op
        # when neither has moved — note Prefect still reports COMPLETED, so read
        # the logs rather than the state to tell a no-op from an ingest.
        #
        # Both guards key on coverage, so a revision that only *restates* an
        # earlier period without adding a new one does not trip them. That is the
        # normal shape of an EIA final revision, and it is precisely what the
        # rebuild-everything design exists to absorb — so it is picked up by the
        # next run that does add a period, or immediately with force_run.
        has_new_data = False
        for form in ("eia860", "eia923"):
            if poll_source_for_update_task(
                dataset_id=DATASET_ID,
                table_id=POLL_TABLE[form],
                source_max_date=probe["max_date"][form],
                env="prod",
                date_format="%Y-%m-%d",
                compare_against="coverage",
            ):
                has_new_data = True
        if not has_new_data and not force_run:
            print("Não há novas atualizações na fonte original")
            return

        input_dir = download_corpus(work_dir=work_dir, probe=probe)
        result = clean_corpus(work_dir=work_dir, input_dir=input_dir)
        print(f"row counts: {result['row_counts']}")

        for form in ("eia860", "eia923"):
            commit_source_update_task(
                dataset_id=DATASET_ID,
                table_id=POLL_TABLE[form],
                source_max_date=probe["max_date"][form],
                env="prod",
                date_format="%Y-%m-%d",
                update_metadata=update_metadata,
                materialize_after_dump=materialize_to_prod,
            )

        tables = constants.ALL_TABLES.value

        # Build EVERY table before testing ANY of them, in both environments.
        # Each data table's custom_dictionary_coverage test reads dicionario via
        # ref(). Interleaved run/test per table, that test would run before
        # dicionario exists and fail with "Not found: Table ...". A re-run hides
        # it, because a stale dicionario from an earlier build survives — so it
        # only bites in a clean environment, which is prod.
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
        # The corpus is ~660 MB of source ZIPs plus the cleaned parquet.
        shutil.rmtree(work_dir, ignore_errors=True)


# EIA-923 gains a month around the end of each month and EIA-860 gains a year in
# the northern summer, but neither lands on a fixed day: the 2026 EIA-923 files
# carry publication dates of 21 July, 30 June and so on. So poll daily and let
# the source-poll guard no-op. The probe reads two landing pages and one ~20 MB
# ZIP, so a no-op run costs well under a minute.
# The minute is chosen, not defaulted: hour 9 is quieter than 8, and :17 keeps
# clear of every cron already in this repo. Defaulting to :00 piles every
# pipeline onto the same instant, where they compete for BigQuery slots and trip
# the daily quota together.
# pyrefly: ignore [missing-attribute]
us_eia_electricity_flow.deploy_schedules = [
    {"cron": "17 9 * * *", "timezone": "America/Sao_Paulo"}
]
# `memory` alone is NOT a variable of the basedosdados work pool's job template
# and is silently dropped, leaving the pod on the template default of 4Gi. The
# key the pod actually gets is `memory_limit`. The clean step parses one 25 MB
# Excel workbook at a time through openpyxl, which is the peak; 8Gi leaves room
# for the largest EIA-860 generator workbook plus the melt.
# pyrefly: ignore [missing-attribute]
us_eia_electricity_flow.job_variables = {
    "memory_limit": "8Gi",
    "memory_request": "2Gi",
}
