"""
Flows for fr_meteofrance — Prefect 3.

Météo-France publishes on two different cadences, so this dataset has two flows:

* ``fr_meteofrance_synop_flow`` (daily) rewrites only the current year's
  ``synop_<year>.csv.gz``, so it downloads one file and replaces one ``annee=``
  partition. ``dump_mode="append"`` overwrites that partition object and leaves
  the other thirty untouched — ``"overwrite"`` would drop the whole staging
  table, and drops the prod table even from a dev run.
* ``fr_meteofrance_climatologie_flow`` (monthly) reissues the climatological
  sheets, so it rebuilds the normals and both station registers. It also
  re-downloads the full SYNOP history, because ``station_synop`` carries each
  station's first and last observation year, which one year cannot give — that
  doubles as a monthly full rebuild of ``synop``.

Deploy: `.github/scripts/deploy_flows.py` auto-discovers both flows; the dev pool
ignores the schedules, the prod pool activates them.
"""

import shutil
import tempfile

from prefect import flow

from pipelines.datasets.fr_meteofrance.constants import constants
from pipelines.datasets.fr_meteofrance.tasks import (
    clean_normales_task,
    clean_synop_task,
    download_fiches_task,
    download_synop,
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

# Coverage spec per table.
#
# `synop` is the high-frequency table — it refreshes daily — so it carries the
# BD Pro rolling window: the most recent 6 months are pro-only, everything older
# is free. Each run recomputes free_end = source_end - free_lag, rewrites both
# DateTimeRanges and re-issues the BigQuery Row Access Policies, so the window
# slides forward on its own.
#
# part_bdpro requires BOTH a free (is_closed=False) and a pro (is_closed=True)
# Coverage to already exist on the table, or assert_coverage_topology raises
# before anything is written.
#
# `normale_climatologique` is reissued monthly but its content is a fixed
# 1991-2020 statistic: the republished sheet restates the same reference period
# rather than advancing it. A rolling window over a series that does not move
# would paywall the tail of 2020 forever, so it stays fully free, keyed on the
# reference period's end year.
#
# The two station registers and `dicionario` have no meaningful date column and
# take no coverage spec at all — their registered ranges are set once, by hand.
_COVERAGE = {
    "synop": PartBdpro(
        date_column=DateOnly(col="date"),
        date_format=DateFormat.YEAR_MD,
        free_lag=FreeLag(unit="months", value=6),
    ),
    "normale_climatologique": AllFree(
        date_column=YearOnly(col="annee_fin_reference"),
        date_format=DateFormat.YEAR,
    ),
}


def _materialize(tables: dict, bucket: str, target: str) -> None:
    """Upload each table's parquet, then build every model, then test every model.

    Run-then-test is split into two passes on purpose: `synop` and
    `normale_climatologique` carry `custom_dictionary_coverage` against
    `dicionario`, and `synop` a `custom_relationships` against `station_synop`.
    Interleaved, the first table's test would run before its sibling exists and
    fail on a clean environment.
    """
    for table, path in tables.items():
        upload_to_gcs(
            data_path=path,
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


@flow(name="fr_meteofrance_synop", log_prints=True)
def fr_meteofrance_synop_flow(
    materialize_to_prod: bool = True,
    update_metadata: bool = True,
    force_run: bool = False,
) -> None:
    """Refresh the current year of SYNOP observations.

    Args:
        materialize_to_prod: Continue past the dev materialization to write the
            prod staging bucket and run dbt against ``target="prod"``. Set False
            to exercise only the dev half — required for a safe test run, since
            the default writes production.
        update_metadata: After a successful prod materialization, register table
            coverage and commit the source update. Has no effect when
            ``materialize_to_prod`` is False.
        force_run: Materialize even when the source poll reports no new day.
    """
    # pyrefly: ignore [unused-coroutine]
    rename_flow_run_dataset_table(
        prefix="Dump: ", dataset_id=DATASET_ID, table_id="synop"
    )

    work_dir = tempfile.mkdtemp(prefix="fr_meteofrance_synop_")
    try:
        input_dir = download_synop(work_dir=work_dir, full_history=False)
        result = clean_synop_task(work_dir=work_dir, input_dir=input_dir)
        max_date = result["max_date"]

        has_new_data = poll_source_for_update_task(
            dataset_id=DATASET_ID,
            table_id="synop",
            source_max_date=max_date,
            env="prod",
            date_format="%Y-%m-%d",
            compare_against="coverage",
        )
        if not has_new_data and not force_run:
            return

        commit_source_update_task(
            dataset_id=DATASET_ID,
            table_id="synop",
            source_max_date=max_date,
            env="prod",
            date_format="%Y-%m-%d",
            update_metadata=update_metadata,
            materialize_after_dump=materialize_to_prod,
        )

        tables = {"synop": result["synop"]}

        if not materialize_to_prod:
            _materialize(tables, bucket="basedosdados-dev", target="dev")
            return

        _materialize(tables, bucket="basedosdados", target="prod")

        if update_metadata:
            register_table_materialization_task(
                dataset_id=DATASET_ID,
                table_id="synop",
                coverage=_COVERAGE["synop"],
                env="prod",
                bq_project="basedosdados",
            )
    finally:
        # Covers both early returns and any exception. The k8s work pool gives
        # each run a fresh pod, but a process worker reuses its filesystem.
        shutil.rmtree(work_dir, ignore_errors=True)


@flow(name="fr_meteofrance_climatologie", log_prints=True)
def fr_meteofrance_climatologie_flow(
    materialize_to_prod: bool = True,
    update_metadata: bool = True,
    force_run: bool = False,
) -> None:
    """Rebuild the normals, both station registers, the dictionary and all of SYNOP.

    Météo-France reissues the climatological sheets monthly. This flow also
    re-downloads the full SYNOP history, because ``station_synop`` carries each
    station's first and last observation year — which doubles as a monthly full
    rebuild of ``synop`` and repairs any partition a daily run left stale.

    Args:
        materialize_to_prod: Continue past the dev materialization to write the
            prod staging bucket and run dbt against ``target="prod"``. Set False
            to exercise only the dev half.
        update_metadata: After a successful prod materialization, register table
            coverage and commit the source update.
        force_run: Materialize even when the source poll reports no new edition.
    """
    # pyrefly: ignore [unused-coroutine]
    rename_flow_run_dataset_table(
        prefix="Dump: ",
        dataset_id=DATASET_ID,
        table_id="normale_climatologique",
    )

    work_dir = tempfile.mkdtemp(prefix="fr_meteofrance_clim_")
    try:
        fiche_dir = download_fiches_task(work_dir=work_dir)
        normals = clean_normales_task(work_dir=work_dir, fiche_dir=fiche_dir)
        max_date = normals["max_date"]

        has_new_data = poll_source_for_update_task(
            dataset_id=DATASET_ID,
            table_id="normale_climatologique",
            source_max_date=max_date,
            env="prod",
            date_format="%Y-%m-%d",
            compare_against="coverage",
        )
        if not has_new_data and not force_run:
            return

        commit_source_update_task(
            dataset_id=DATASET_ID,
            table_id="normale_climatologique",
            source_max_date=max_date,
            env="prod",
            date_format="%Y-%m-%d",
            update_metadata=update_metadata,
            materialize_after_dump=materialize_to_prod,
        )

        input_dir = download_synop(work_dir=work_dir, full_history=True)
        synop = clean_synop_task(
            work_dir=work_dir, input_dir=input_dir, with_stations=True
        )

        # dicionario first: the dictionary-coverage tests on synop and
        # normale_climatologique read it, and station_synop is read by synop's
        # relationships test.
        tables = {
            "dicionario": normals["dicionario"],
            "station_synop": synop["station_synop"],
            "station_climatologique": normals["station_climatologique"],
            "normale_climatologique": normals["normale_climatologique"],
            "synop": synop["synop"],
        }

        if not materialize_to_prod:
            _materialize(tables, bucket="basedosdados-dev", target="dev")
            return

        _materialize(tables, bucket="basedosdados", target="prod")

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
        shutil.rmtree(work_dir, ignore_errors=True)


# Météo-France refreshes the current year's SYNOP file through the day; 07:23 BRT
# is after the overnight sync and on a minute nothing else in the repo uses.
# pyrefly: ignore [missing-attribute]
fr_meteofrance_synop_flow.deploy_schedules = [
    {"cron": "23 7 * * *", "timezone": "America/Sao_Paulo"}
]
# One year of SYNOP is ~350k rows in pandas; modest.
# pyrefly: ignore [missing-attribute]
fr_meteofrance_synop_flow.job_variables = {"memory": "4Gi"}

# The sheets are reissued monthly, early in the month. Poll across a few days at
# 07:37 BRT; the source-poll guard no-ops until a new edition actually appears.
# pyrefly: ignore [missing-attribute]
fr_meteofrance_climatologie_flow.deploy_schedules = [
    {"cron": "37 7 6,7,8,9 * *", "timezone": "America/Sao_Paulo"}
]
# Cleans 31 years of SYNOP plus 1,576 sheets; give the worker headroom.
# pyrefly: ignore [missing-attribute]
fr_meteofrance_climatologie_flow.job_variables = {"memory": "8Gi"}
