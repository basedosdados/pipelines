"""
Flows for br_cgu_sancoes — Prefect 3.

CGU sanctions registries (CEIS, CNEP, CEPIM, Acordos de Leniência) from the
Portal da Transparência. Each registry is an on-demand live snapshot with no
historical archive, so every run fetches the latest available snapshot and does
a full replace (``dump_mode="overwrite"``), stamping ``data_extracao``. A single
flow downloads all registries once and rebuilds all six tables.

BD Pro rolling window: the high-value compliance tables (ceis, cnep,
acordos_leniencia) paywall their recent window keyed on the *sanction start*
date (``data_inicio_sancao`` / ``data_inicio_acordo``), so a sanction ages out
of Pro six months after it starts. The remaining tables (cepim,
acordos_leniencia_efeitos) stay fully free (coverage keyed on the snapshot date,
their only date column); ``dicionario`` has no date column and takes no spec.

Deploy: `.github/scripts/deploy_flows.py` auto-discovers `br_cgu_sancoes_flow`;
the dev pool ignores the schedule, the prod pool activates it.
"""

import shutil
import tempfile

from prefect import flow

from pipelines.datasets.br_cgu_sancoes.constants import constants
from pipelines.datasets.br_cgu_sancoes.tasks import (
    clean_sancoes,
    download_sancoes,
)
from pipelines.utils.metadata.domain import (
    AllFree,
    DateFormat,
    DateOnly,
    FreeLag,
    PartBdpro,
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
# ceis / cnep / acordos_leniencia are the high-frequency compliance tables and
# carry the BD Pro rolling window, keyed on the SANCTION START date — a sanction
# that started in the last 6 months is pro-only, everything older is free. Each
# run recomputes free_end = max(<start_date>) - 6 months, rewrites both
# DateTimeRanges and re-issues the BigQuery Row Access Policies, so the window
# slides on its own. The paywall columns are 0% null, so the standard RAP
# (<date_col> <= free_end) hides nothing unintentionally.
#
# part_bdpro requires BOTH a free (is_closed=False) and a pro (is_closed=True)
# Coverage to already exist on the table, or assert_coverage_topology raises
# before anything is written — these three are onboarded AllFree, so the pro
# Coverage must be created by hand before the first armed run.
#
# cepim / acordos_leniencia_efeitos have no content date; their only date column
# is the snapshot stamp, so they stay fully free keyed on data_extracao.
# `dicionario` has no date column and takes no coverage spec at all.
_PART_BDPRO_DATE = {
    "ceis": "data_inicio_sancao",
    "cnep": "data_inicio_sancao",
    "acordos_leniencia": "data_inicio_acordo",
}
_COVERAGE = {
    "ceis": PartBdpro(
        date_column=DateOnly(col="data_inicio_sancao"),
        date_format=DateFormat.YEAR_MD,
        free_lag=FreeLag(unit="months", value=6),
    ),
    "cnep": PartBdpro(
        date_column=DateOnly(col="data_inicio_sancao"),
        date_format=DateFormat.YEAR_MD,
        free_lag=FreeLag(unit="months", value=6),
    ),
    "acordos_leniencia": PartBdpro(
        date_column=DateOnly(col="data_inicio_acordo"),
        date_format=DateFormat.YEAR_MD,
        free_lag=FreeLag(unit="months", value=6),
    ),
    "cepim": AllFree(
        date_column=DateOnly(col="data_extracao"),
        date_format=DateFormat.YEAR_MD,
    ),
    "acordos_leniencia_efeitos": AllFree(
        date_column=DateOnly(col="data_extracao"),
        date_format=DateFormat.YEAR_MD,
    ),
}

# The table whose source update gates the run (poll guard). ceis is the primary,
# largest registry; the others are committed alongside it below.
_POLL_TABLE = "ceis"


@flow(name="br_cgu_sancoes", log_prints=True)
def br_cgu_sancoes_flow(
    materialize_to_prod: bool = True,
    update_metadata: bool = True,
    force_run: bool = False,
) -> None:
    """Download the CGU sanction snapshots, rebuild all six tables, materialize.

    The registries ship the full history on every snapshot, so each run is a full
    replace (``dump_mode="overwrite"``). Since only the current snapshot is
    retrievable (no archive), the source poll is a weak guard — the flow is
    scheduled weekly and each snapshot overwrites the last.

    Args:
        materialize_to_prod: Continue past the dev materialization to write the
            prod staging bucket and run dbt against ``target="prod"``. Set False
            to exercise only the dev half — required for a safe test run, since
            the default writes production.
        update_metadata: After a successful prod materialization, register table
            coverage (which applies the BD Pro Row Access Policies for the
            part_bdpro tables) and commit the source update. Has no effect when
            ``materialize_to_prod`` is False.
        force_run: Materialize even when the source poll reports no new snapshot.
    """
    # pyrefly: ignore [unused-coroutine]
    rename_flow_run_dataset_table(
        prefix="Dump: ", dataset_id=DATASET_ID, table_id="sancoes"
    )

    work_dir = tempfile.mkdtemp(prefix="br_cgu_sancoes_")
    try:
        input_dir = download_sancoes(work_dir=work_dir)
        result = clean_sancoes(work_dir=work_dir, input_dir=input_dir)
        snapshots = result["snapshots"]

        # Weak guard: a live-snapshot source exposes no publication competência,
        # so compare the extraction date against Table.Update.latest (a
        # wall-clock). A weekly run is therefore effectively always "new"; the
        # guard mainly prevents a same-day re-run from redoing work.
        has_new_data = poll_source_for_update_task(
            dataset_id=DATASET_ID,
            table_id=_POLL_TABLE,
            source_max_date=snapshots[_POLL_TABLE],
            env="prod",
            date_format="%Y-%m-%d",
            compare_against="table_update",
        )
        if not has_new_data and not force_run:
            return

        # Commit the source Update for each data table up front (before
        # materializing): if the flow fails mid-way, the source metadata still
        # reflects that a new snapshot was published. For a live-snapshot source
        # the "coverage date" is the extraction date.
        for table in constants.DATA_TABLES.value:
            commit_source_update_task(
                dataset_id=DATASET_ID,
                table_id=table,
                source_max_date=snapshots[table],
                env="prod",
                date_format="%Y-%m-%d",
                update_metadata=update_metadata,
                materialize_after_dump=materialize_to_prod,
            )

        tables = constants.ALL_TABLES.value

        # The dev materialization is the pre-arm validation path, not part of a
        # production run: it rebuilds and re-tests every table in
        # basedosdados-dev, which nothing downstream reads. Running it on an
        # armed run doubled the BigQuery bytes billed for no signal — prod
        # runs the same models and the same tests seconds later.
        if not materialize_to_prod:
            # Dev: upload staging + materialize/test.
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
                    dbt_command="run/test",
                    target="dev",
                )
            return

        # Prod: upload staging + materialize/test.
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
                dbt_command="run/test",
                target="prod",
            )

        if update_metadata:
            # dicionario has no date column, so it is not in _COVERAGE.
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
        shutil.rmtree(work_dir, ignore_errors=True)


# The registries refresh irregularly (a live on-demand snapshot). Run weekly,
# polling across a couple of days (Mon + Tue) at 08:00 BRT so a day where the
# on-demand generation fails still gets a retry; overwrite makes a second run
# idempotent.
# pyrefly: ignore [missing-attribute]
br_cgu_sancoes_flow.deploy_schedules = [
    {"cron": "30 8 * * 1,2", "timezone": "America/Sao_Paulo"}
]
