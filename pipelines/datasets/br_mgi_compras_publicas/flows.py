"""
Flows for br_mgi_compras_publicas — Prefect 3.

Brazilian federal procurement from the Compras.gov.br open-data API. Two flows,
split by how the underlying law behaves rather than by convenience:

* **daily** — the Lei 14.133/2021 modules and the SIASG contract registry, which
  are live and revised continuously. Items are revised in place for a median of
  78 days after inclusion, so each run re-fetches a trailing window rather than
  only what is new; the dbt models collapse each key to its latest state.
* **weekly** — the registries (orgao, UASG, suppliers, catalogues) and the
  dicionario. These are snapshots stamped with an extraction date, and they move
  slowly enough that a daily rebuild would be waste.

The Lei 8.666 legado tables are **not** refreshed: that procurement regime has
ended and its 2025 tail is 7,562 rows. They are a closed archive, backfilled
once. Adding them to a schedule would spend hours re-reading a frozen dataset.

Deploy: `.github/scripts/deploy_flows.py` discovers the flow objects below.
"""

from __future__ import annotations

import datetime as dt

from prefect import flow

from pipelines.datasets.br_mgi_compras_publicas.constants import constants
from pipelines.datasets.br_mgi_compras_publicas.tasks import (
    clear_staging_partitions,
    rebuild_dicionario,
    refresh_table,
)
from pipelines.utils.metadata.domain import (
    AllFree,
    DateFormat,
    DateOnly,
    FreeLag,
    PartBdpro,
)
from pipelines.utils.metadata.tasks import (
    register_table_materialization_task,
)
from pipelines.utils.tasks import (
    rename_flow_run_dataset_table,
    run_dbt,
    upload_to_gcs,
)

DATASET_ID = constants.DATASET_ID.value

#: Revision window. Measured on the source: 98.9% of contratacao_item revisions
#: land within 180 days of inclusion and the median is 78 days, so a shorter
#: window would leave stale rows behind that no later run would ever revisit.
REVISION_WINDOW_DAYS = 180

DAILY_TABLES = (
    "contratacao",
    "contratacao_item",
    "contratacao_item_resultado",
    "ata_registro_preco",
    "ata_registro_preco_item",
    "contrato",
    "contrato_item",
)
WEEKLY_TABLES = (
    "orgao",
    "unidade_administrativa",
    "fornecedor",
    "catalogo_material",
    "catalogo_servico",
)
#: Not harvested -- derived from the other tables' chunks, so it is rebuilt
#: after them rather than fetched. It has no TableSpec, and asking
#: refresh_table for it raises.
DERIVED_TABLES = ("dicionario",)

#: Tables refreshed daily paywall their most recent window to BD Pro; the
#: slow-moving registries stay fully open. `register_table_materialization_task`
#: rolls the window forward on every run and re-issues the Row Access Policies.
#: Column the BD Pro window is measured on, per daily table.
#:
#: It must be a real date, not `ano`. A year column cannot express a six-month
#: boundary: every row of 2026 reads as 2026-01-01, so
#: `DATE(ano,1,1) <= today - 6 months` releases the whole current year for
#: free. A date column makes the window exactly "newer than six months ago",
#: to the day, and it rolls forward on every run.
#:
#: For the contratacao tables the choice is forced -- each has one publication
#: date. For atas and contratos it is not: `data_vigencia_inicial` looks
#: forward, so a contract recorded today to start in 2028 would sit behind the
#: paywall for two years while a contract signed in 2023 starting next month
#: would be paid. Keying on when the row was *recorded* paywalls what is
#: actually new and cannot be sidestepped by future-dating.
BDPRO_DATE_COLUMN = {
    "contratacao": "data_publicacao_pncp",
    "contratacao_item": "data_inclusao_pncp",
    "contratacao_item_resultado": "data_resultado_pncp",
    "ata_registro_preco": "data_hora_inclusao",
    "ata_registro_preco_item": "data_hora_inclusao",
    "contrato": "data_hora_inclusao",
    "contrato_item": "data_hora_inclusao",
}

COVERAGE = {
    t: PartBdpro(
        date_column=DateOnly(col=BDPRO_DATE_COLUMN[t]),
        date_format=DateFormat.YEAR_MD,
        free_lag=FreeLag(unit="months", value=6),
    )
    for t in DAILY_TABLES
} | {
    # The registries are keyed on a full extraction date, not a year, and the
    # dicionario has no date column at all so it takes no coverage spec.
    t: AllFree(
        date_column=DateOnly(col="data_extracao"),
        date_format=DateFormat.YEAR_MD,
    )
    for t in WEEKLY_TABLES
}


def _run(
    tables: tuple[str, ...],
    output_dir: str,
    since: dt.date | None,
    materialize_to_prod: bool,
    update_metadata: bool,
    derived: tuple[str, ...] = (),
) -> None:
    """Harvest, upload and build every table, then test them all.

    Every table is run before any table is tested, per environment. Cross-table
    tests -- the directory `relationships` checks and the dictionary coverage
    test -- read sibling models, so interleaving run and test per table fails on
    a clean environment where the sibling does not exist yet.
    """
    paths = {t: refresh_table(t, output_dir, since=since) for t in tables}
    for name in derived:
        paths[name] = rebuild_dicionario(
            output_dir, _after=list(paths.values())
        )
    tables = tables + derived

    for bucket, target, enabled in (
        ("basedosdados-dev", "dev", True),
        ("basedosdados", "prod", materialize_to_prod),
    ):
        if not enabled:
            continue
        for table in tables:
            # A trailing-window refresh must not use "overwrite": that drops the
            # whole staging prefix, so the table would be rebuilt from the
            # window alone and every earlier year would be lost. Clear just the
            # partitions being replaced, then append them back whole.
            path = paths[table]
            if since is not None:
                path = clear_staging_partitions(
                    table=table, data_path=path, bucket_name=bucket
                )
            upload_to_gcs(
                data_path=path,
                dataset_id=DATASET_ID,
                table_id=table,
                bucket_name=bucket,
                dump_mode="append" if since is not None else "overwrite",
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

    if update_metadata and materialize_to_prod:
        for table in tables:
            if table in COVERAGE:
                register_table_materialization_task(
                    dataset_id=DATASET_ID,
                    table_id=table,
                    coverage=COVERAGE[table],
                    env="prod",
                    bq_project="basedosdados",
                )


@flow(name="br_mgi_compras_publicas.diario")
def br_mgi_compras_publicas_diario_flow(
    materialize_to_prod: bool = True,
    update_metadata: bool = True,
    output_dir: str = "/tmp/br_mgi_compras_publicas",
    revision_window_days: int = REVISION_WINDOW_DAYS,
) -> None:
    """Refresh the Lei 14.133 modules and the contract registry."""
    # pyrefly: ignore [unused-coroutine]
    rename_flow_run_dataset_table(
        prefix="Dump: ", dataset_id=DATASET_ID, table_id="diario"
    )
    # Align to 1 January: the harvest writes whole `ano=` partitions, and the
    # upload replaces a partition wholesale. A window starting mid-year would
    # rewrite that year's partition with only its tail, dropping January to the
    # window start.
    window_start = dt.date.today() - dt.timedelta(days=revision_window_days)
    since = dt.date(window_start.year, 1, 1)
    _run(DAILY_TABLES, output_dir, since, materialize_to_prod, update_metadata)


@flow(name="br_mgi_compras_publicas.semanal")
def br_mgi_compras_publicas_semanal_flow(
    materialize_to_prod: bool = True,
    update_metadata: bool = True,
    output_dir: str = "/tmp/br_mgi_compras_publicas",
) -> None:
    """Re-snapshot the registries, catalogues and dicionario."""
    # pyrefly: ignore [unused-coroutine]
    rename_flow_run_dataset_table(
        prefix="Dump: ", dataset_id=DATASET_ID, table_id="semanal"
    )
    # Snapshots carry no date filter -- the whole register is re-read and
    # stamped with today's extraction date.
    _run(
        WEEKLY_TABLES,
        output_dir,
        None,
        materialize_to_prod,
        update_metadata,
        derived=DERIVED_TABLES,
    )


# Minute chosen off the hour and away from the slots already in use: piling
# every pipeline onto :00 makes them compete for BigQuery slots and fail
# together if the daily quota trips.
# pyrefly: ignore [missing-attribute]
br_mgi_compras_publicas_diario_flow.deploy_schedules = [
    {"cron": "37 5 * * *", "timezone": "America/Sao_Paulo"}
]
# pyrefly: ignore [missing-attribute]
br_mgi_compras_publicas_semanal_flow.deploy_schedules = [
    {"cron": "12 4 * * 0", "timezone": "America/Sao_Paulo"}
]
# pyrefly: ignore [missing-attribute]
br_mgi_compras_publicas_diario_flow.job_variables = {"memory": "8Gi"}
# pyrefly: ignore [missing-attribute]
br_mgi_compras_publicas_semanal_flow.job_variables = {"memory": "8Gi"}
