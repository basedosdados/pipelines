"""Flows for br_senado_dados_abertos_administrativos — Prefect 3.

Senado Federal administrative open data (``adm.senado.gov.br``). The dataset has
two table shapes, both refreshed by one mechanism (see
``models/br_senado_dados_abertos_administrativos/PIPELINE_PLAN.md``):

- 30 **snapshot** tables (``data_extracao``): each run stacks a new dated
  snapshot — the source exposes only current state, so the time dimension is
  ours (CNPJ model, as ``au_ato_abr``).
- 10 **time-series** tables (``ano``): each run refreshes the last two years in
  place (current + prior, to catch late-arriving data); older years are stable.

Both are served by overwriting the **staging** external table with the current
window and letting the **incremental** dbt models (``insert_overwrite`` on the
partition column) replace exactly that partition in prod, so history accumulates
in prod, not staging. ``dicionario`` (a full-refresh table) and any table empty
for the window are skipped per run — see ``_run``.

Two module-level flows on mutually-exclusive days share one runner: the daily
flow skips the contratação fan-out (``sub_resources=False``); the weekly Monday
flow includes it. deploy discovers both flow objects.
"""

import datetime as dt
import shutil
import tempfile

from prefect import flow

from pipelines.datasets.br_senado_dados_abertos_administrativos import utils
from pipelines.datasets.br_senado_dados_abertos_administrativos.constants import (
    constants,
)
from pipelines.datasets.br_senado_dados_abertos_administrativos.tasks import (
    extract_and_clean,
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
    register_table_materialization_task,
)
from pipelines.utils.tasks import (
    rename_flow_run_dataset_table,
    run_dbt,
    upload_to_gcs,
)

DATASET_ID = constants.DATASET_ID.value

# Coverage per table shape (see PIPELINE_PLAN.md).
#   snapshots  -> PartBdpro 6-month rolling window on data_extracao
#   series     -> AllFree on ano
#   dicionario -> no coverage (no date column)
_PART_BDPRO = PartBdpro(
    date_column=DateOnly(col="data_extracao"),
    date_format=DateFormat.YEAR_MD,
    free_lag=FreeLag(unit="months", value=constants.FREE_LAG_MONTHS.value),
)
_ALL_FREE = AllFree(
    date_column=YearOnly(col="ano"),
    date_format=DateFormat.YEAR,
)
_COVERAGE = {
    **{t: _PART_BDPRO for t in utils.SNAPSHOTS},
    **{t: _ALL_FREE for t in utils.PARTITIONED},
}
# The table whose source Update anchors the dataset — any stable snapshot table.
_ANCHOR_TABLE = "senador"


def _run(
    sub_resources: bool,
    materialize_to_prod: bool,
    update_metadata: bool,
) -> None:
    """Extract the current window, upload staging, materialize, test, and
    (on prod) refresh coverage + the source update.

    Snapshot-stacking has no "did the source update?" question — we snapshot on
    schedule — so there is no poll gate: every scheduled run produces a snapshot.
    A prod run goes straight to prod (no redundant dev materialization, whose
    bytes buy no signal — prod runs the same models and tests seconds later).
    """
    # pyrefly: ignore [unused-coroutine]
    rename_flow_run_dataset_table(
        prefix="Dump: ", dataset_id=DATASET_ID, table_id=_ANCHOR_TABLE
    )

    bucket = "basedosdados" if materialize_to_prod else "basedosdados-dev"
    target = "prod" if materialize_to_prod else "dev"

    work_dir = tempfile.mkdtemp(prefix="br_senado_adm_")
    try:
        output_dir = f"{work_dir}/output"
        this_year = dt.date.today().year
        result = extract_and_clean(
            output_dir=output_dir,
            sub_resources=sub_resources,
            # Last two years: refresh the current year and also catch late-arriving
            # prior-year data (e.g. CEAPS reimbursements filed after year end).
            years=[this_year - 1, this_year],
        )
        counts = result["counts"]
        max_date = result["extracted_at"]

        # Upload only tables that actually produced rows for this window. A table
        # empty for the window writes no parquet (e.g. suprido_movimentacao has no
        # rows in some years), so uploading it would raise FileNotFoundError; with
        # insert_overwrite, skipping simply leaves its existing partitions in prod.
        # dicionario is excluded too: it is rebuilt from the supridos in this run,
        # so a windowed extract would shrink it — it is static reference data,
        # fully populated at onboarding, and no test depends on it (a full-history
        # refresh, if ever needed, is a separate manual run).
        static_skip = {"dicionario"}
        tables = [
            t
            for t in result["tables"]
            if counts.get(t, 0) > 0 and t not in static_skip
        ]
        empty = [t for t in result["tables"] if counts.get(t, 0) == 0]
        if empty:
            print(
                f"Skipping {len(empty)} table(s) empty for this window: {empty}"
            )
        print("Skipping dicionario (static reference; not refreshed per run)")

        # Upload staging (overwrite = the current window) + run every table, THEN
        # test every table. Cross-table tests (relationships, dictionary
        # coverage) read sibling models, so all siblings must exist before any
        # test runs — never interleave run/test per table.
        for table in tables:
            upload_to_gcs(
                data_path=f"{output_dir}/{table}",
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

        if not materialize_to_prod:
            return

        if update_metadata:
            for table in tables:
                coverage = _COVERAGE.get(table)
                if coverage is None:  # dicionario — no date column
                    continue
                register_table_materialization_task(
                    dataset_id=DATASET_ID,
                    table_id=table,
                    coverage=coverage,
                    env="prod",
                    bq_project="basedosdados",
                )
            # Source Update: its max coverage date is today's snapshot date.
            commit_source_update_task(
                dataset_id=DATASET_ID,
                table_id=_ANCHOR_TABLE,
                source_max_date=max_date,
                env="prod",
                date_format="%Y-%m-%d",
                update_metadata=update_metadata,
                materialize_after_dump=materialize_to_prod,
            )
    finally:
        shutil.rmtree(work_dir, ignore_errors=True)


@flow(name="br_senado_dados_abertos_administrativos_daily", log_prints=True)
def br_senado_dados_abertos_administrativos_daily_flow(
    materialize_to_prod: bool = True,
    update_metadata: bool = True,
    force_run: bool = False,
) -> None:
    """Daily refresh — parents, snapshots and last-two-years series, no fan-out.

    Args:
        materialize_to_prod: Continue past dev to write the prod staging bucket
            and run dbt ``target="prod"``. False exercises only the dev half —
            required for a safe test run, since the default writes production.
        update_metadata: After a prod materialization, refresh table coverage
            (BD Pro rolling window on snapshots) and commit the source update.
            No effect when ``materialize_to_prod`` is False.
        force_run: Accepted for parity with the repo's flow signature; this
            snapshot pipeline has no poll gate, so it does not change behaviour.
    """
    _run(
        sub_resources=False,
        materialize_to_prod=materialize_to_prod,
        update_metadata=update_metadata,
    )


@flow(name="br_senado_dados_abertos_administrativos_weekly", log_prints=True)
def br_senado_dados_abertos_administrativos_weekly_flow(
    materialize_to_prod: bool = True,
    update_metadata: bool = True,
    force_run: bool = False,
) -> None:
    """Weekly (Monday) refresh — the full extract, including the contratação
    children fan-out (item, garantia, pagamento(+documento_fiscal,+empenho),
    aditivo, ata_acionamento).

    Args as in the daily flow.
    """
    _run(
        sub_resources=True,
        materialize_to_prod=materialize_to_prod,
        update_metadata=update_metadata,
    )


# Mutually-exclusive days: daily every day except Monday, weekly on Monday. The
# clean step holds the current payroll year in memory (~0.5-1 GB); give headroom.
# pyrefly: ignore [missing-attribute]
br_senado_dados_abertos_administrativos_daily_flow.deploy_schedules = [
    {"cron": constants.DAILY_CRON.value, "timezone": "America/Sao_Paulo"}
]
# pyrefly: ignore [missing-attribute]
br_senado_dados_abertos_administrativos_daily_flow.job_variables = {
    "memory": "8Gi"
}
# pyrefly: ignore [missing-attribute]
br_senado_dados_abertos_administrativos_weekly_flow.deploy_schedules = [
    {"cron": constants.WEEKLY_CRON.value, "timezone": "America/Sao_Paulo"}
]
# pyrefly: ignore [missing-attribute]
br_senado_dados_abertos_administrativos_weekly_flow.job_variables = {
    "memory": "8Gi"
}
