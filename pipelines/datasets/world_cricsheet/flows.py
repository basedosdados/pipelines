"""
Flows for world_cricsheet — Prefect 3.

Cricsheet global cricket. The full-history bundle ``all_csv2.zip`` ships the
entire history on every (near-daily) release, so each run is a **full replace**
(``dump_mode="overwrite"``), not an incremental append — this sidesteps the
duplicate-on-append problem the overlapping recent windows would otherwise cause.
A single flow downloads once, rebuilds all four tables, and materializes them.
Because each run re-ingests the whole 11.4M-row history, it is scheduled
**weekly** (not daily).

Deploy: ``.github/scripts/deploy_flows.py`` auto-discovers ``world_cricsheet_flow``;
the dev pool ignores the schedule, the prod pool activates it (paused until armed).
"""

import shutil
import tempfile

from prefect import flow

from pipelines.datasets.world_cricsheet.constants import constants
from pipelines.datasets.world_cricsheet.tasks import (
    clean_cricsheet,
    download_cricsheet,
)
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

DATASET_ID = constants.DATASET_ID.value
POLL_TABLE = constants.POLL_TABLE.value
BUNDLE_URL = constants.BUNDLE_RAW_SOURCE_URL.value

# Coverage spec per table.
#
# deliveries and matches refresh near-daily, so they carry the BD Pro rolling
# window on their start_date DATE column: the most recent 6 months are pro-only,
# everything older is free. Each run recomputes free_end = source_end - free_lag,
# rewrites both DateTimeRanges, and re-issues the BigQuery Row Access Policies, so
# the window slides forward on its own. part_bdpro requires BOTH a free
# (is_closed=False) and a pro (is_closed=True) Coverage to already exist on the
# table, or assert_coverage_topology raises before anything is written.
#
# match_players has only an annual `year` column (no fine date), so it stays
# fully free. people is a non-temporal dimension and takes no coverage spec at
# all (it is uploaded/materialized, but not registered here).
_COVERAGE = {
    "deliveries": PartBdpro(
        date_column=DateOnly(col="start_date"),
        date_format=DateFormat.YEAR_MD,
        free_lag=FreeLag(unit="months", value=6),
    ),
    "matches": PartBdpro(
        date_column=DateOnly(col="start_date"),
        date_format=DateFormat.YEAR_MD,
        free_lag=FreeLag(unit="months", value=6),
    ),
    "match_players": AllFree(
        date_column=YearOnly(col="year"), date_format=DateFormat.YEAR
    ),
}


@flow(name="world_cricsheet", log_prints=True)
def world_cricsheet_flow(
    materialize_to_prod: bool = True,
    update_metadata: bool = True,
    force_run: bool = False,
) -> None:
    """Download the Cricsheet bundle, rebuild all four tables, materialize them.

    Cricsheet ships the full history on every (near-daily) release, so each run
    is a full replace (``dump_mode="overwrite"``) rather than an incremental
    append. The source poll short-circuits the run when Cricsheet has not
    published a newer match date, making a scheduled run a cheap no-op between
    releases.

    Args:
        materialize_to_prod: Continue past the dev materialization to write the
            prod staging bucket and run dbt against ``target="prod"``. Set False
            to exercise only the dev half — required for a safe test run, since
            the default writes production.
        update_metadata: After a successful prod materialization, register table
            coverage (rolling BD Pro window for deliveries/matches) and commit the
            source update. Has no effect when ``materialize_to_prod`` is False.
        force_run: Materialize even when the source poll reports no new match date.
    """
    # pyrefly: ignore [unused-coroutine]
    rename_flow_run_dataset_table(
        prefix="Dump: ", dataset_id=DATASET_ID, table_id="cricsheet"
    )

    work_dir = tempfile.mkdtemp(prefix="world_cricsheet_")
    try:
        input_dir = download_cricsheet(work_dir=work_dir)
        result = clean_cricsheet(work_dir=work_dir, input_dir=input_dir)
        max_start = result["max_start_date"]

        # Skip the run when Cricsheet has not published a newer match date
        # (unless forced). The polled table has 3 raw sources linked, so name the
        # bundle explicitly by URL or the client raises on the ambiguity.
        has_new_data = poll_source_for_update_task(
            dataset_id=DATASET_ID,
            table_id=POLL_TABLE,
            source_max_date=max_start,
            env="prod",
            date_format="%Y-%m-%d",
            raw_source_url=BUNDLE_URL,
            compare_against="coverage",
        )
        if not has_new_data and not force_run:
            return

        # Comita o Update da fonte já aqui, antes de baixar/materializar: se o
        # flow falhar no meio, o metadado da fonte ainda reflete que havia dado
        # novo publicado, mesmo que a tabela não tenha sido atualizada.
        commit_source_update_task(
            dataset_id=DATASET_ID,
            table_id=POLL_TABLE,
            source_max_date=max_start,
            env="prod",
            date_format="%Y-%m-%d",
            raw_source_url=BUNDLE_URL,
            update_metadata=update_metadata,
            materialize_after_dump=materialize_to_prod,
        )

        tables = constants.ALL_TABLES.value

        # Dev: upload + build every table first, then test every table. The
        # cross-table relationships test (match_players.person_id -> people)
        # is pulled in when testing either table, so all four tables must be
        # materialized before any test runs — a per-table run+test loop fails
        # on a fresh target when the referenced table does not exist yet.
        for table in tables:
            upload_to_gcs(
                data_path=result[table],
                dataset_id=DATASET_ID,
                table_id=table,
                bucket_name="basedosdados-dev",
                dump_mode="overwrite",
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

        if not materialize_to_prod:
            return

        # Prod: same two-phase pattern — build all tables, then test all.
        for table in tables:
            upload_to_gcs(
                data_path=result[table],
                dataset_id=DATASET_ID,
                table_id=table,
                bucket_name="basedosdados",
                dump_mode="overwrite",
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
        # A k8s work pool gives each run a fresh pod, but a process/local worker
        # reuses its filesystem — the extracted bundle is several GB.
        shutil.rmtree(work_dir, ignore_errors=True)


# Cricsheet republishes near-daily as matches finish, but each run is a full
# overwrite rebuild of the whole 11.4M-row dataset (the ~114 MB bundle ships the
# entire history), so a daily cadence would re-ingest everything every day. Run
# WEEKLY instead — Monday 06:00 BRT — which cuts the cost ~4x while keeping
# freshness fine. The source-poll guard still no-ops between real releases, and
# the full-replace dump means overlapping windows never duplicate.
# pyrefly: ignore [missing-attribute]
world_cricsheet_flow.deploy_schedules = [
    {"cron": "15 6 * * 1", "timezone": "America/Sao_Paulo"}
]
# The deliveries build streams 11.4M rows and the bundle extracts to several GB;
# give the worker headroom.
# pyrefly: ignore [missing-attribute]
world_cricsheet_flow.job_variables = {"memory": "12Gi"}
