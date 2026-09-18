"""Flows for ``us_osha_enforcement`` — Prefect 3.

OSHA enforcement data from the DOL Enforcement Data Catalog: inspections, the
violations cited, the penalties assessed, and the incidents and injuries
investigated. Eleven tables, 38.8M rows, 1972 onward.

The source reissues every file daily and amends prior records in place, because
a penalty is contested and revised for years after the citation. So this is a
**partition refresh**, not an append: each run rebuilds a set of partition
years and rewrites one Parquet object per partition, leaving the untouched
years' objects where they are. Which years is decided by
:func:`~pipelines.datasets.us_osha_enforcement.utils.plan_refresh` — a trailing
window plus any older year OSHA has actually touched, read from
``inspection.case_mod_date``.

Weekly, not daily: the files total 6 GB and the data does not move fast enough
to justify seven downloads a week.

Deploy: ``.github/scripts/deploy_flows.py`` discovers ``us_osha_enforcement_flow``.
The dev pool strips the schedule; the prod pool activates it, paused.
"""

from __future__ import annotations

import shutil
import tempfile

from prefect import flow

from pipelines.datasets.us_osha_enforcement.constants import constants
from pipelines.datasets.us_osha_enforcement.tasks import (
    clean_osha,
    download_osha,
    plan_osha_refresh,
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

#: Coverage spec per table.
#:
#: This dataset refreshes weekly, so the house rule applies: the most recent
#: window of every table is BD Pro and everything older stays free. Six months,
#: which on a year-granularity table resolves to "the current year is pro".
#:
#: `register_table_materialization_task` recomputes free_end = source_end -
#: free_lag on every run, rewrites both DateTimeRanges and re-issues the
#: BigQuery Row Access Policies, so the window rolls forward by itself. The dbt
#: model is untouched — the paywall is enforced by the policies, not by SQL.
#:
#: part_bdpro requires BOTH a free (is_closed=False) and a pro (is_closed=True)
#: Coverage to already exist on the table, or assert_coverage_topology raises
#: before anything is written. They are created at onboarding by
#: models/us_osha_enforcement/code/register_metadata.py.
#:
#: `violation_event` reports its partition `year`, not its own `event_date`,
#: unlike the other three tables that have a real date column. 100 of its
#: 11.7M rows carry an event dated in the future — up to 2026-12-01, which is
#: impossible for an event — and read_max_date takes the max, so those rows
#: alone would push free_end three months earlier and paywall data that should
#: be free. The partition year comes from the parent inspection's open_date,
#: which has no future rows at all.
#:
#: `dicionario` has no date column and takes no coverage spec.
_FREE_LAG = FreeLag(unit="months", value=6)

_COVERAGE = {
    "inspection": PartBdpro(
        date_column=DateOnly(col="open_date"),
        date_format=DateFormat.YEAR_MD,
        free_lag=_FREE_LAG,
    ),
    "violation": PartBdpro(
        date_column=DateOnly(col="issuance_date"),
        date_format=DateFormat.YEAR_MD,
        free_lag=_FREE_LAG,
    ),
    "violation_event": PartBdpro(
        date_column=YearOnly(col="year"),
        date_format=DateFormat.YEAR,
        free_lag=_FREE_LAG,
    ),
    "violation_text": PartBdpro(
        date_column=YearOnly(col="year"),
        date_format=DateFormat.YEAR,
        free_lag=_FREE_LAG,
    ),
    "related_activity": PartBdpro(
        date_column=YearOnly(col="year"),
        date_format=DateFormat.YEAR,
        free_lag=_FREE_LAG,
    ),
    "emphasis_code": PartBdpro(
        date_column=YearOnly(col="year"),
        date_format=DateFormat.YEAR,
        free_lag=_FREE_LAG,
    ),
    "optional_code_info": PartBdpro(
        date_column=YearOnly(col="year"),
        date_format=DateFormat.YEAR,
        free_lag=_FREE_LAG,
    ),
    "accident": PartBdpro(
        date_column=DateOnly(col="event_date"),
        date_format=DateFormat.YEAR_MD,
        free_lag=_FREE_LAG,
    ),
    "accident_injury": PartBdpro(
        date_column=YearOnly(col="year"),
        date_format=DateFormat.YEAR,
        free_lag=_FREE_LAG,
    ),
    "accident_narrative": PartBdpro(
        date_column=YearOnly(col="year"),
        date_format=DateFormat.YEAR,
        free_lag=_FREE_LAG,
    ),
}


@flow(name="us_osha_enforcement", log_prints=True)
def us_osha_enforcement_flow(
    materialize_to_prod: bool = True,
    update_metadata: bool = True,
    force_run: bool = False,
    trailing_years: int = 8,
    modified_days: int = 400,
) -> None:
    """Refresh the OSHA enforcement tables from the DOL bulk files.

    Args:
        materialize_to_prod: Continue past the dev materialization to write the
            prod staging bucket and run dbt against ``target="prod"``. Set
            False for a safe validation run — the default writes production.
        update_metadata: After a successful prod materialization, register
            table coverage. No effect when ``materialize_to_prod`` is False.
        force_run: Materialize even when the poll reports nothing new.
        trailing_years: Partition years always rebuilt, counting back from the
            newest inspection year.
        modified_days: Also rebuild any older year holding an inspection whose
            ``case_mod_date`` falls within this many days.
    """
    # pyrefly: ignore [unused-coroutine]
    rename_flow_run_dataset_table(
        prefix="Dump: ", dataset_id=DATASET_ID, table_id="inspection"
    )

    work_dir = tempfile.mkdtemp(prefix="us_osha_enforcement_")
    try:
        input_dir = download_osha(work_dir=work_dir)
        plan = plan_osha_refresh(
            input_dir=input_dir,
            trailing_years=trailing_years,
            modified_days=modified_days,
        )

        # The poll compares a full date against a YEAR_MD coverage. Passing a
        # year here would silently turn a weekly pipeline into an annual one.
        has_new_data = poll_source_for_update_task(
            dataset_id=DATASET_ID,
            table_id="inspection",
            source_max_date=plan["max_date"],
            env="prod",
            date_format="%Y-%m-%d",
            compare_against="coverage",
        )
        if not has_new_data and not force_run:
            return

        commit_source_update_task(
            dataset_id=DATASET_ID,
            table_id="inspection",
            source_max_date=plan["max_date"],
            env="prod",
            date_format="%Y-%m-%d",
            update_metadata=update_metadata,
            materialize_after_dump=materialize_to_prod,
        )

        paths = clean_osha(
            input_dir=input_dir, work_dir=work_dir, years=plan["years"]
        )
        tables = constants.TABLES.value

        # Upload and build EVERY table before testing ANY of them. The
        # referential tests read sibling models — violation.inspection_id
        # against inspection, accident_narrative.accident_id against accident —
        # and interleaving run/test per table would test the first model before
        # its parent exists. That failure hides in a re-run where a stale
        # sibling survives, and only bites in a clean environment.
        bucket = "basedosdados" if materialize_to_prod else "basedosdados-dev"
        target = "prod" if materialize_to_prod else "dev"

        for table in tables:
            upload_to_gcs(
                data_path=paths[table],
                dataset_id=DATASET_ID,
                table_id=table,
                bucket_name=bucket,
                dump_mode="append",
                source_format="parquet",
            )
        for table in tables:
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
        shutil.rmtree(work_dir, ignore_errors=True)


# Sunday 04:47 America/Sao_Paulo. OSHA reissues the files daily; weekly is
# ample for the way this data is used, and 6 GB of downloads a day is not.
# The minute is deliberately not 0 — a dozen pipelines already fire at :00 and
# compete for BigQuery slots.
# pyrefly: ignore [missing-attribute]
us_osha_enforcement_flow.deploy_schedules = [
    {"cron": "47 4 * * 0", "timezone": "America/Sao_Paulo"}
]

# `memory` alone is silently dropped: it is not a key of the work pool's job
# template, so a flow that sets only it runs on the pool default of 4Gi
# whatever number it names. The clean step holds the citation-text and
# narrative reassembly maps in memory.
# pyrefly: ignore [missing-attribute]
us_osha_enforcement_flow.job_variables = {
    "memory_limit": "12Gi",
    "memory_request": "4Gi",
}
