"""Flow for br_mps_beneficios — Prefect 3.

INSS benefícios concedidos (flow of new grants) and mantidos (stock in payment),
by município and month. Unlike a source that reships its whole history each
release, INSS publishes one file per competência, so each run is an
**incremental append** of the new month rather than a full replace: a month of
mantidos ATIVOS is 12 GB uncompressed, and rebuilding the series would be ~700
GB of input.

The two tables advance independently — concedido reached 2026-08 while mantido
was still at 2026-01 — so each is polled on its own and only the ones with a new
competência are rebuilt. ``dicionario_especie`` is static but is rebuilt every
run: both data tables carry a ``relationships`` test against it, so in a clean
environment the sibling has to exist before either is tested.

Deploy: ``.github/workflows/scripts/deploy_flows.py`` auto-discovers
``br_mps_beneficios_flow``; the dev pool strips the schedule, the prod pool
activates it (paused until armed in Django admin).
"""

from __future__ import annotations

import shutil
import tempfile

from prefect.schedules import Cron

from pipelines.datasets.br_mps_beneficios.constants import constants
from pipelines.datasets.br_mps_beneficios.tasks import (
    build_dicionario,
    latest_source_competencia,
    previous_competencia_shape,
    refresh_month,
)
from pipelines.utils.flow import flow
from pipelines.utils.metadata.domain import (
    AllFree,
    DateFormat,
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
CONCEDIDO = constants.TABLE_CONCEDIDO.value
MANTIDO = constants.TABLE_MANTIDO.value
DICIONARIO = constants.TABLE_DICIONARIO.value

# Coverage spec per table.
#
# Both data tables refresh monthly, and the house rule is that a table
# refreshed monthly or more often paywalls its most recent window to BD Pro.
# They are registered AllFree here deliberately, and switching them is a
# separate, deliberate step, because:
#
#   * `part_bdpro` requires BOTH a free (is_closed=False) and a pro
#     (is_closed=True) Coverage to already exist on the table, or
#     `assert_coverage_topology` raises before anything is written — and only
#     the free one exists today;
#   * creating the pro Coverage sets `Table.contains_closed_data`, which puts a
#     Pro badge on a dataset that is already published and entirely free, so it
#     is visibly wrong until the Row Access Policies exist.
#
# To switch: create the pro Coverage on each data table
# (`create_update_coverage(table_id=…, area_id=<br>, is_closed=True, env="prod")`,
# and set is_closed on its DateTimeRange too), then change these two entries to
# `PartBdpro(date_column=YearMonth(...), date_format=DateFormat.YEAR_MONTH,
# free_lag=FreeLag(unit="months", value=6))`. The window then rolls by itself.
#
# `dicionario_especie` has no date column, so it takes no coverage spec at all.
_COVERAGE = {
    CONCEDIDO: AllFree(
        date_column=YearMonth(year="ano", month="mes"),
        date_format=DateFormat.YEAR_MONTH,
    ),
    MANTIDO: AllFree(
        date_column=YearMonth(year="ano", month="mes"),
        date_format=DateFormat.YEAR_MONTH,
    ),
}


def _competencia_to_date(competencia: int) -> str:
    """YYYYMM -> YYYY-MM, the coverage date the metadata tasks compare against."""
    ano, mes = divmod(competencia, 100)
    return f"{ano:04d}-{mes:02d}"


@flow(name="br_mps_beneficios", log_prints=True)
def br_mps_beneficios_flow(
    materialize_to_prod: bool = True,
    update_metadata: bool = True,
    force_run: bool = False,
) -> None:
    """Refresh whichever of the two tables has a new competência published.

    Args:
        materialize_to_prod: Continue past the dev materialization to write the
            prod staging bucket and run dbt against ``target="prod"``. Set False
            to exercise only the dev half — required for a safe test run, since
            the default writes production.
        update_metadata: After a successful prod materialization, register table
            coverage. Has no effect when ``materialize_to_prod`` is False.
        force_run: Rebuild even when the poll reports no new competência. The
            reissue guard still applies, so forcing a run on a republished month
            stages nothing.
    """
    # pyrefly: ignore [unused-coroutine]
    rename_flow_run_dataset_table(
        prefix="Dump: ", dataset_id=DATASET_ID, table_id="beneficios"
    )

    work_dir = tempfile.mkdtemp(prefix="br_mps_beneficios_")
    bq_project = "basedosdados" if materialize_to_prod else "basedosdados-dev"
    try:
        staged: dict[str, dict] = {}
        for table_id in (CONCEDIDO, MANTIDO):
            competencia = latest_source_competencia(table_id=table_id)
            source_date = _competencia_to_date(competencia)

            has_new = poll_source_for_update_task(
                dataset_id=DATASET_ID,
                table_id=table_id,
                source_max_date=source_date,
                env="prod",
                date_format="%Y-%m",
                compare_against="coverage",
            )
            if not has_new and not force_run:
                print(
                    f"{table_id}: source still at {source_date}, nothing to do"
                )
                continue

            previous = previous_competencia_shape(
                table_id=table_id, bq_project=bq_project
            )
            result = refresh_month(
                table_id=table_id,
                competencia=competencia,
                work_dir=work_dir,
                previous=previous,
            )
            if result["status"] != "ok":
                # A reissued or empty month must not advance the source Update:
                # the publisher has not actually released this competência.
                print(f"{table_id}: {result['status']} — not materialising")
                continue

            staged[table_id] = result | {"source_date": source_date}

        if not staged:
            print("no table had a genuinely new competência")
            return

        # The source Update is committed right after the poll, before the
        # materialization, so a mid-flow failure still records that the source
        # had published — and only for the tables that really advanced.
        for table_id, result in staged.items():
            commit_source_update_task(
                dataset_id=DATASET_ID,
                table_id=table_id,
                source_max_date=result["source_date"],
                env="prod",
                date_format="%Y-%m",
                update_metadata=update_metadata,
                materialize_after_dump=materialize_to_prod,
            )

        dicionario_path = build_dicionario(work_dir=work_dir)
        bucket = "basedosdados" if materialize_to_prod else "basedosdados-dev"
        target = "prod" if materialize_to_prod else "dev"

        # Build every table, THEN test every table. Both data tables have a
        # `relationships` test against ref('..._dicionario_especie'), which reads
        # the sibling model: interleaved, the first table's test runs before the
        # dictionary is built and fails on a clean environment.
        upload_to_gcs(
            data_path=dicionario_path,
            dataset_id=DATASET_ID,
            table_id=DICIONARIO,
            bucket_name=bucket,
            dump_mode="overwrite",
            source_format="parquet",
        )
        for table_id, result in staged.items():
            # append, not overwrite: the run stages one new competência as its
            # own parquet inside the year partition and must not disturb the
            # months already there.
            upload_to_gcs(
                data_path=result["path"],
                dataset_id=DATASET_ID,
                table_id=table_id,
                bucket_name=bucket,
                dump_mode="append",
                source_format="parquet",
            )

        for table_id in (DICIONARIO, *staged):
            run_dbt(
                dataset_id=DATASET_ID,
                table_id=table_id,
                dbt_command="run",
                target=target,
            )
        for table_id in (DICIONARIO, *staged):
            run_dbt(
                dataset_id=DATASET_ID,
                table_id=table_id,
                dbt_command="test",
                target=target,
            )

        if materialize_to_prod and update_metadata:
            for table_id in staged:
                register_table_materialization_task(
                    dataset_id=DATASET_ID,
                    table_id=table_id,
                    coverage=_COVERAGE[table_id],
                    env="prod",
                    bq_project="basedosdados",
                )
    finally:
        # Covers the early returns and any exception. A mantidos archive is
        # ~950 MB and the k8s pool gives a fresh pod per run, but a process
        # worker reuses its filesystem.
        shutil.rmtree(work_dir, ignore_errors=True)


# INSS publishes on no fixed day. Concedido for month M has appeared around
# M+1, mantidos far later and irregularly, so poll across several days at a
# minute nobody else uses (16:57 BRT). The poll guard makes a run with nothing
# new a cheap no-op, and the reissue guard makes a run on a republished month
# a no-op too.
br_mps_beneficios_flow.deploy_schedules = [
    Cron("57 16 8,12,16,20,24,28 * *", timezone="America/Sao_Paulo")
]
# Aggregating a month of mantidos streams 12 GB out of the zip and holds the
# running frame in pandas; measured RSS peaked at 1.1 GB. `memory_limit` is the
# key the work pool's job template actually exposes — setting only `memory`
# is dropped silently and leaves the pod on the 4Gi default.
br_mps_beneficios_flow.job_variables = {
    "memory_limit": "8Gi",
    "memory_request": "2Gi",
}
