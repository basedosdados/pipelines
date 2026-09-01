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
from pipelines.datasets.br_mgi_compras_publicas.tasks import refresh_table
from pipelines.utils.metadata.domain import (
    AllFree,
    DateFormat,
    DateOnly,
    FreeLag,
    PartBdpro,
    YearOnly,
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
    "dicionario",
)

#: Tables refreshed daily paywall their most recent window to BD Pro; the
#: slow-moving registries stay fully open. `register_table_materialization_task`
#: rolls the window forward on every run and re-issues the Row Access Policies.
COVERAGE = {
    t: PartBdpro(
        date_column=YearOnly(col="ano"),
        date_format=DateFormat.YEAR,
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
    if t != "dicionario"
}


def _run(
    tables: tuple[str, ...],
    output_dir: str,
    since: dt.date | None,
    materialize_to_prod: bool,
    update_metadata: bool,
) -> None:
    """Harvest, upload and build every table, then test them all.

    Every table is run before any table is tested, per environment. Cross-table
    tests -- the directory `relationships` checks and the dictionary coverage
    test -- read sibling models, so interleaving run and test per table fails on
    a clean environment where the sibling does not exist yet.
    """
    paths = {t: refresh_table(t, output_dir, since=since) for t in tables}

    for bucket, target, enabled in (
        ("basedosdados-dev", "dev", True),
        ("basedosdados", "prod", materialize_to_prod),
    ):
        if not enabled:
            continue
        for table in tables:
            upload_to_gcs(
                data_path=paths[table],
                dataset_id=DATASET_ID,
                table_id=table,
                bucket_name=bucket,
                dump_mode="overwrite",
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
    rename_flow_run_dataset_table(
        prefix="Dump: ", dataset_id=DATASET_ID, table_id="diario"
    )
    since = dt.date.today() - dt.timedelta(days=revision_window_days)
    _run(DAILY_TABLES, output_dir, since, materialize_to_prod, update_metadata)


@flow(name="br_mgi_compras_publicas.semanal")
def br_mgi_compras_publicas_semanal_flow(
    materialize_to_prod: bool = True,
    update_metadata: bool = True,
    output_dir: str = "/tmp/br_mgi_compras_publicas",
) -> None:
    """Re-snapshot the registries, catalogues and dicionario."""
    rename_flow_run_dataset_table(
        prefix="Dump: ", dataset_id=DATASET_ID, table_id="semanal"
    )
    # Snapshots carry no date filter -- the whole register is re-read and
    # stamped with today's extraction date.
    _run(WEEKLY_TABLES, output_dir, None, materialize_to_prod, update_metadata)


# Minute chosen off the hour and away from the slots already in use: piling
# every pipeline onto :00 makes them compete for BigQuery slots and fail
# together if the daily quota trips.
br_mgi_compras_publicas_diario_flow.deploy_schedules = [
    {"cron": "37 5 * * *", "timezone": "America/Sao_Paulo"}
]
br_mgi_compras_publicas_semanal_flow.deploy_schedules = [
    {"cron": "12 4 * * 0", "timezone": "America/Sao_Paulo"}
]
br_mgi_compras_publicas_diario_flow.job_variables = {"memory": "8Gi"}
br_mgi_compras_publicas_semanal_flow.job_variables = {"memory": "8Gi"}
