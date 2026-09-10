"""
Flows for us_nih_reporter — Prefect 3.

NIH RePORTER ExPORTER bulk files. NIH does **not** refresh these weekly, whatever
the RePORTER database behind them does. Its own FAQ says the consolidated project
and abstract files are created once at the close of each fiscal year, that the
**three prior fiscal years are restated at the same time**, and that the
publication and link files are updated then too. The published dates bear that
out: the FY2025 project file carried 2026-07-09, FY2024 carried 2025-07-07, and
FY2008 through FY2023 all carried the same 2025-06-16 — one sweep that rewrote
sixteen fiscal years at once. FY1985-FY2005 have not moved since 2010.

The two all-fiscal-years files are different: patents and clinical studies both
carried 2026-09-07 when this was built, a day old, and together weigh 16 MB.

Hence **two flows**, because one cadence cannot serve both. ``us_nih_reporter``
rebuilds the annual corpus; ``us_nih_reporter_links`` rebuilds the two link
tables weekly. Folding them together would drag a 3.6 GB rebuild along behind a
16 MB refresh every week.

Each flow rebuilds its **whole** corpus rather than a trailing window:

* A window sized to the newest fiscal year would miss every restatement sweep,
  which is most of what actually changes here.
* The dicionario's temporal coverage is computed from the project partitions. A
  windowed run would publish a register claiming each code exists only within
  the window.
* Rebuilding is affordable at this cadence — 3.6 GB of archives, 2.95M project
  rows, a handful of times a year — and it makes the restatement problem
  structurally impossible rather than inferred from state this pipeline has
  nowhere to keep.

The poll is what keeps that affordable. ``probe_source`` costs two header
requests per file and downloads nothing, so a run that finds nothing new is a
few hundred kilobytes. It compares the newest ``Last-Modified`` across the
flow's own families against ``Table.Update.latest`` — the signal here is a
publication timestamp, not a coverage date, which is why ``compare_against`` is
``"table_update"`` and not the usual ``"coverage"``: fiscal-year coverage does
not move when NIH restates FY2019.

Every table is ``AllFree``. The BD Pro rolling window covers tables refreshed
monthly or more often; the annual tables are not, and the two link tables carry
no date column for a window to slide along, so no Row Access Policies are issued
and none of the paywall machinery runs.

``dump_mode="append"`` is load-bearing: ``"overwrite"`` calls
``tb.delete(mode="all")``, which drops the **production** table, and fires from
the dev half of the flow too. With ``"append"`` the upload ends in
``Storage.upload(if_exists="replace")``, which replaces each partition blob at
its own path — the same end state, without the delete.

Deploy: `.github/scripts/deploy_flows.py` auto-discovers both flows; the dev pool
ignores the schedules, the prod pool activates them.
"""

import shutil
import tempfile
from collections.abc import Mapping

from prefect import flow

from pipelines.datasets.us_nih_reporter.constants import constants
from pipelines.datasets.us_nih_reporter.tasks import (
    clean_corpus,
    download_corpus,
    probe_source,
)
from pipelines.utils.metadata.domain import (
    AllFree,
    CoverageSpec,
    DateFormat,
    NonHistorical,
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

# Coverage spec per table.
#
# The four year-partitioned tables key their coverage on `year`, which is
# non-null on every row by construction. patent_link and clinical_study_link
# have no date column at all — the source ships them as one snapshot covering
# every fiscal year — so they take NonHistorical, which derives a single
# coverage from the BigQuery table's last_modified. dicionario is a register
# with no coverage of its own and is absent here.
_YEARLY = AllFree(
    date_column=YearOnly(col="year"), date_format=DateFormat.YEAR
)

_ANNUAL_COVERAGE = {
    "project": _YEARLY,
    "project_abstract": _YEARLY,
    "publication": _YEARLY,
    "publication_link": _YEARLY,
}

_LINK_COVERAGE = {
    "patent_link": NonHistorical(),
    "clinical_study_link": NonHistorical(),
}


def _materialize(
    families: list[str],
    tables: list[str],
    coverage: Mapping[str, CoverageSpec],
    anchor_table: str,
    materialize_to_prod: bool,
    update_metadata: bool,
    force_run: bool,
) -> None:
    """Poll, rebuild, upload and materialize one family group.

    Shared by the two flows: they differ only in which families they probe,
    which tables they build and which table anchors the source poll.

    Args:
        families: ExPORTER families to probe and download, as keys of
            ``constants.FAMILIES``.
        tables: Clean tables this group builds, uploads and tests, in build
            order — every one is built before any is tested.
        coverage: Clean table -> its ``CoverageSpec``, registered after a
            successful prod materialization. A table absent from this mapping
            gets no coverage registered, which is how ``dicionario`` is handled.
        anchor_table: Table whose raw data source the poll and the source-update
            commit are read from and written to.
        materialize_to_prod: Write the prod staging bucket and run dbt against
            ``target="prod"``. False exercises only the dev half.
        update_metadata: Register table coverage and commit the source update
            after a successful prod materialization. No effect when
            ``materialize_to_prod`` is False.
        force_run: Rebuild even when the source poll reports nothing new.

    Returns:
        None.
    """
    # pyrefly: ignore [unused-coroutine]
    rename_flow_run_dataset_table(
        prefix="Dump: ", dataset_id=DATASET_ID, table_id=anchor_table
    )

    work_dir = tempfile.mkdtemp(prefix=f"{DATASET_ID}_")
    try:
        probe = probe_source(families=families)
        print(
            f"{len(probe['listing'])} ExPORTER files published for {families}"
        )
        max_date = probe["max_date"]
        print(f"newest source publication date: {max_date}")

        # Skip the run when nothing has been republished since the last
        # materialization. A scheduled run is then a cheap no-op — note Prefect
        # still reports COMPLETED, so read the logs rather than the state to
        # tell a no-op apart from a real refresh.
        has_new_data = poll_source_for_update_task(
            dataset_id=DATASET_ID,
            table_id=anchor_table,
            source_max_date=max_date,
            env="prod",
            date_format="%Y-%m-%d",
            compare_against="table_update",
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
            table_id=anchor_table,
            source_max_date=max_date,
            env="prod",
            date_format="%Y-%m-%d",
            update_metadata=update_metadata,
            materialize_after_dump=materialize_to_prod,
        )

        # Build EVERY table before testing ANY of them, in both environments.
        # project's custom_dictionary_coverage test reads dicionario via ref().
        # Interleaved run/test per table, that test would run before dicionario
        # exists and fail with "Not found: Table ... us_nih_reporter.dicionario".
        # A re-run hides it, because a stale dicionario from an earlier build
        # survives — so it only bites in a clean environment, which is prod.
        bucket = "basedosdados" if materialize_to_prod else "basedosdados-dev"
        target = "prod" if materialize_to_prod else "dev"
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
            for table, spec in coverage.items():
                register_table_materialization_task(
                    dataset_id=DATASET_ID,
                    table_id=table,
                    coverage=spec,
                    env="prod",
                    bq_project="basedosdados",
                )
    finally:
        # Covers both early returns (no new data) and any exception. The annual
        # corpus is 3.6 GB of archives plus several more of extracted CSV and
        # parquet, which must not be left behind on the worker.
        shutil.rmtree(work_dir, ignore_errors=True)


@flow(name="us_nih_reporter", log_prints=True)
def us_nih_reporter_flow(
    materialize_to_prod: bool = True,
    update_metadata: bool = True,
    force_run: bool = False,
) -> None:
    """Rebuild the annual ExPORTER corpus: projects, abstracts and publications.

    Args:
        materialize_to_prod: Continue past the dev materialization to write the
            prod staging bucket and run dbt against ``target="prod"``. Set False
            to exercise only the dev half — required for a safe test run, since
            the default writes production.
        update_metadata: After a successful prod materialization, register table
            coverage and commit the source update. Has no effect when
            ``materialize_to_prod`` is False.
        force_run: Materialize even when the source poll reports nothing new.
    """
    _materialize(
        families=constants.ANNUAL_FAMILIES.value,
        tables=constants.ANNUAL_TABLES.value,
        coverage=_ANNUAL_COVERAGE,
        anchor_table=constants.PROJECT.value,
        materialize_to_prod=materialize_to_prod,
        update_metadata=update_metadata,
        force_run=force_run,
    )


@flow(name="us_nih_reporter_links", log_prints=True)
def us_nih_reporter_links_flow(
    materialize_to_prod: bool = True,
    update_metadata: bool = True,
    force_run: bool = False,
) -> None:
    """Rebuild the two all-fiscal-years link tables: patents and clinical studies.

    Args:
        materialize_to_prod: Continue past the dev materialization to write the
            prod staging bucket and run dbt against ``target="prod"``. Set False
            to exercise only the dev half — required for a safe test run, since
            the default writes production.
        update_metadata: After a successful prod materialization, register table
            coverage and commit the source update. Has no effect when
            ``materialize_to_prod`` is False.
        force_run: Materialize even when the source poll reports nothing new.
    """
    _materialize(
        families=constants.LINK_FAMILIES.value,
        tables=constants.LINK_TABLES.value,
        coverage=_LINK_COVERAGE,
        anchor_table=constants.PATENT_LINK.value,
        materialize_to_prod=materialize_to_prod,
        update_metadata=update_metadata,
        force_run=force_run,
    )


# The consolidated annual release lands at the end of January and the restatement
# sweeps have fallen in June and July; NIH names no fixed day for either. So poll
# three times a month and let the source-poll guard no-op — a no-op run is two
# header requests per file and no download at all.
# The minute is chosen, not defaulted: hour 7 already holds 0, 23, 37, 38, 45 and
# 51, so 12 keeps clear of all of them. Defaulting to :00 piles every pipeline
# onto the same instant, where they compete for BigQuery slots and trip the daily
# quota together.
# pyrefly: ignore [missing-attribute]
us_nih_reporter_flow.deploy_schedules = [
    {"cron": "12 7 6,16,26 * *", "timezone": "America/Sao_Paulo"}
]
# The clean step holds one file's rows in memory at a time, plus the FY1985-FY1999
# funding supplement (840,226 rows) for the whole project pass; a full local
# rebuild peaked at 0.95 GB resident. `memory_limit` is
# what the pod actually gets — a bare `memory` key is not in the work pool's job
# template and is dropped silently, leaving the default 4Gi however large a number
# it names.
# pyrefly: ignore [missing-attribute]
us_nih_reporter_flow.job_variables = {
    "memory_limit": "8Gi",
    "memory_request": "3Gi",
}

# Patents and clinical studies are rewritten roughly weekly. Poll every Tuesday;
# hour 7 minute 12 is already this dataset's slot on the monthly flow, and the
# two only collide three times a year.
# pyrefly: ignore [missing-attribute]
us_nih_reporter_links_flow.deploy_schedules = [
    {"cron": "17 7 * * 2", "timezone": "America/Sao_Paulo"}
]
# 16 MB of CSV, 132k rows across the two tables.
# pyrefly: ignore [missing-attribute]
us_nih_reporter_links_flow.job_variables = {
    "memory_limit": "2Gi",
    "memory_request": "1Gi",
}
