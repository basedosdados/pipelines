"""
Flows for us_nsf_ncses — Prefect 3.

Two NCSES surveys, both annual, refreshed by one flow:

* **HERD** — the Higher Education Research and Development public use files,
  released around September for the prior fiscal year.
* **SED** — the Survey of Earned Doctorates published data tables, released
  around August for the prior academic year.

One flow rather than two, for a reason that is easy to get wrong: every table's
`custom_dictionary_coverage` test reads ``ref('us_nsf_ncses__dicionario')``, and
the dictionary is written by the HERD half. Split across two flows, the SED
flow's tests would depend on a staging table it does not own.

Each release restates its own history — HERD retro-imputes prior years, an SED
cycle republishes its whole series — so a run rebuilds every partition rather
than appending the newest one. ``dump_mode="append"`` is deliberate: it ends in
``st.upload(..., if_exists="replace")``, which replaces each blob by name, while
``"overwrite"`` calls ``tb.delete(mode="all")`` and drops the materialized
*production* table even from a dev-only run.

Deploy: `.github/scripts/deploy_flows.py` auto-discovers `us_nsf_ncses_flow`;
the dev pool ignores the schedule, the prod pool activates it.
"""

import shutil
import tempfile

from prefect import flow
from prefect.utilities.asyncutils import run_coro_as_sync

from pipelines.datasets.us_nsf_ncses.constants import constants
from pipelines.datasets.us_nsf_ncses.tasks import (
    check_source,
    clean_all,
    download_all,
)
from pipelines.utils.metadata.domain import AllFree, DateFormat, YearOnly
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
DATE_FORMAT = constants.DATE_FORMAT.value

# Coverage spec per table. Both surveys are annual, so nothing here is behind
# the BD Pro rolling window, which applies to tables refreshed monthly or more
# often. `dicionario` has no date column and so takes no spec at all.
_COVERAGE = {
    "herd_institution": AllFree(
        date_column=YearOnly(col="year"), date_format=DateFormat.YEAR
    ),
    "herd_expenditure": AllFree(
        date_column=YearOnly(col="year"), date_format=DateFormat.YEAR
    ),
    "herd_personnel": AllFree(
        date_column=YearOnly(col="year"), date_format=DateFormat.YEAR
    ),
    "herd_survey_item": AllFree(
        date_column=YearOnly(col="year"), date_format=DateFormat.YEAR
    ),
    "sed_estimate": AllFree(
        date_column=YearOnly(col="reference_year"),
        date_format=DateFormat.YEAR,
    ),
    "sed_data_table": AllFree(
        date_column=YearOnly(col="reference_year"),
        date_format=DateFormat.YEAR,
    ),
}


@flow(name="us_nsf_ncses", log_prints=True)
def us_nsf_ncses_flow(
    materialize_to_prod: bool = True,
    update_metadata: bool = True,
    force_run: bool = False,
) -> None:
    """Refresh both NCSES surveys and materialize all seven tables.

    The two source polls short-circuit the run when neither survey has published
    a newer year, which makes a scheduled run a cheap no-op between releases.
    Either survey being new is enough: both are rebuilt together because the
    dictionary they share is written by the HERD half.

    Args:
        materialize_to_prod: Continue past the dev materialization to write the
            prod staging bucket and run dbt against ``target="prod"``. Set False
            to exercise only the dev half — required for a safe test run, since
            the default writes production.
        update_metadata: Write metadata to the prod backend — the source
            Update, committed as soon as a poll reports a new year, and the
            table coverage, registered after a successful prod materialization.
            Set False for a test run: the metadata tasks are pinned
            ``env="prod"`` regardless of which pool the run is on.
        force_run: Materialize even when neither source poll reports a new year.
    """
    run_coro_as_sync(
        rename_flow_run_dataset_table(
            prefix="Dump: ", dataset_id=DATASET_ID, table_id="ncses"
        )
    )

    source = check_source()
    herd_year = str(source["herd_year"])
    sed_year = str(source["sed_year"])

    herd_is_new = poll_source_for_update_task(
        dataset_id=DATASET_ID,
        table_id=constants.HERD_POLL_TABLE.value,
        source_max_date=herd_year,
        env="prod",
        date_format=DATE_FORMAT,
        compare_against="coverage",
    )
    sed_is_new = poll_source_for_update_task(
        dataset_id=DATASET_ID,
        table_id=constants.SED_POLL_TABLE.value,
        source_max_date=sed_year,
        env="prod",
        date_format=DATE_FORMAT,
        compare_against="coverage",
    )
    if not (herd_is_new or sed_is_new) and not force_run:
        print("neither HERD nor SED has published a newer year; nothing to do")
        return

    # Commit the source Update here, not after materialization.
    # `commit_source_update_task` records what NCSES *published*, which is true
    # the moment the poll confirms it and stays true if a later step fails; the
    # poll compares against `Coverage` (`compare_against="coverage"`), never
    # against this record, so an early write cannot stall the next run. Only the
    # survey whose poll actually reported a new year is committed — `force_run`
    # re-materializes without claiming the source published anything.
    # `materialize_to_prod` gates it too: these tasks are pinned `env="prod"`
    # whichever pool the run is on, so a dev-only validation run must not touch
    # production metadata.
    if update_metadata and materialize_to_prod:
        for is_new, poll_table, year in (
            (herd_is_new, constants.HERD_POLL_TABLE.value, herd_year),
            (sed_is_new, constants.SED_POLL_TABLE.value, sed_year),
        ):
            if is_new:
                commit_source_update_task(
                    dataset_id=DATASET_ID,
                    table_id=poll_table,
                    source_max_date=year,
                    env="prod",
                    date_format=DATE_FORMAT,
                )

    work_dir = tempfile.mkdtemp(prefix="us_nsf_ncses_")
    try:
        input_dir = download_all(work_dir=work_dir, source=source)
        produced = clean_all(
            work_dir=work_dir, input_dir=input_dir, source=source
        )

        tables = constants.ALL_TABLES.value

        # The dev materialization is the pre-arm validation path, not part of a
        # production run: it rebuilds and re-tests every table in
        # basedosdados-dev, which nothing downstream reads.
        bucket = "basedosdados" if materialize_to_prod else "basedosdados-dev"
        target = "prod" if materialize_to_prod else "dev"

        # Upload and build every table before testing any of them: the
        # dictionary-coverage and relationships tests read sibling models, so an
        # interleaved run/test fails on a table its sibling has not built yet.
        for table in tables:
            upload_to_gcs(
                data_path=produced[table],
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
        # Covers both early returns and any exception. The k8s work pool gives
        # each run a fresh pod, but a process worker reuses its filesystem, and
        # the HERD download is ~320 MB.
        shutil.rmtree(work_dir, ignore_errors=True)


# SED releases its cycle around late August, HERD its public use files around
# September. Poll weekly across August to December at 07:34 BRT; the source
# polls no-op until a new year actually appears. The minute is deliberately not
# :00 — a dozen pipelines firing on the same instant compete for BigQuery slots
# and fail together when the daily quota trips.
# pyrefly: ignore [missing-attribute]
us_nsf_ncses_flow.deploy_schedules = [
    {"cron": "34 7 5,12,19,26 8,9,10,11,12 *", "timezone": "America/Sao_Paulo"}
]
# The HERD clean holds one fiscal year of rows at a time, peaking well under a
# gigabyte, but the downloaded ZIPs and the parquet share the pod's disk.
# `memory` alone is silently dropped: the work pool's job template only knows
# `memory_limit` and `memory_request`, and defaults to 4Gi for anything else.
# pyrefly: ignore [missing-attribute]
us_nsf_ncses_flow.job_variables = {
    "memory_limit": "8Gi",
    "memory_request": "2Gi",
}
