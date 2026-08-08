"""Flows for br_mf_divida_ativa — Prefect 3.

PGFN "Dados Abertos da Dívida Ativa da União": three quarterly tables (SIDA /
previdenciário / FGTS). Each release adds ONE new quarter as an immutable
snapshot, so the pipeline is **incremental append**, not full replace — only
quarters newer than the registered ``RawDataSource.Update`` boundary are
downloaded and appended to staging (partitioned by ano/trimestre), which keeps
each quarter's partition from being ingested twice. All three tables paywall
their most recent two quarters (``PartBdpro``, free_lag 6 months = 2 quarters);
the rolling window and its BigQuery Row Access Policies are re-applied on every
prod run by ``register_table_materialization_task``.

Deploy: `.github/scripts/deploy_flows.py` auto-discovers ``br_mf_divida_ativa_flow``
(defined at module level here); the dev pool ignores the schedule, the prod pool
activates it (paused until armed).
"""

import shutil
import tempfile

from prefect import flow

from pipelines.datasets.br_mf_divida_ativa.constants import constants
from pipelines.datasets.br_mf_divida_ativa.tasks import (
    clean_quarters_task,
    discover_new_quarters,
)
from pipelines.datasets.br_mf_divida_ativa.utils import TABLES
from pipelines.utils.metadata.domain import (
    DateFormat,
    FreeLag,
    PartBdpro,
    YearQuarter,
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
# The probe/anchor table: SIDA is published every quarter and carries the single
# raw data source that the poll/commit/boundary all read.
ANCHOR_TABLE = "nao_previdenciario"

# All three tables refresh quarterly, so all three paywall their most recent two
# quarters to BD Pro. free_lag = 6 months = 2 quarters: free ends at
# source_end - 6 months, pro spans the two quarters after that. The window rolls
# on its own each run. part_bdpro requires BOTH a free (is_closed=False) and a
# pro (is_closed=True) Coverage to already exist on the table (created at
# onboarding), or assert_coverage_topology raises before anything is written.
_COVERAGE = {
    table: PartBdpro(
        date_column=YearQuarter(year="ano", quarter="trimestre"),
        date_format=DateFormat.YEAR_MONTH,
        free_lag=FreeLag(unit="months", value=6),
    )
    for table in TABLES
}


@flow(name="br_mf_divida_ativa", log_prints=True)
def br_mf_divida_ativa_flow(
    materialize_to_prod: bool = True,
    update_metadata: bool = True,
    force_run: bool = False,
) -> None:
    """Ingest any newly published PGFN quarters and materialize the three tables.

    Only quarters newer than the registered source boundary are downloaded and
    appended, so a scheduled run is a cheap no-op between quarterly releases.

    Args:
        materialize_to_prod: Continue past the dev materialization to write the
            prod staging bucket and run dbt against ``target="prod"``. Set False
            to exercise only the dev half — required for a safe test run, since
            the default writes production.
        update_metadata: After a successful prod materialization, roll the
            coverage/Row-Access-Policy window and commit the source update. Has no
            effect when ``materialize_to_prod`` is False.
        force_run: When the source has nothing genuinely new, re-ingest the latest
            available quarter into DEV ONLY (overwriting dev staging so the dbt
            uniqueness test stays clean). Prod is never overwritten or
            re-appended by a forced run — only genuine new quarters ever reach
            prod. Use for a safe dev smoke test:
            ``{materialize_to_prod: False, update_metadata: False, force_run: True}``.
    """
    # pyrefly: ignore [unused-coroutine]
    rename_flow_run_dataset_table(
        prefix="Dump: ", dataset_id=DATASET_ID, table_id=ANCHOR_TABLE
    )

    work_dir = tempfile.mkdtemp(prefix="br_mf_divida_ativa_")
    try:
        disc = discover_new_quarters(DATASET_ID, ANCHOR_TABLE, env="prod")
        new_quarters = disc["quarters"]
        available = disc["available"]
        max_date = disc["max_date"]
        if available is None:
            return

        # Record a Poll on the source (audit: "when we last looked"). Non-gating —
        # the ingest decision is driven by discover_new_quarters, not this return.
        # pyrefly: ignore [unused-coroutine]
        poll_source_for_update_task(
            dataset_id=DATASET_ID,
            table_id=ANCHOR_TABLE,
            source_max_date=max_date,
            env="prod",
            date_format="%Y-%m-%d",
        )

        if new_quarters:
            quarters = new_quarters
            dump_mode = "append"
        elif force_run:
            # Forced dev smoke test: nothing genuinely new, so re-ingest the
            # latest quarter into DEV with overwrite (clean single-quarter dev
            # staging -> dbt uniqueness passes). Prod is skipped below.
            quarters = [available]
            dump_mode = "overwrite"
            print(
                f"force_run with no new quarters: DEV-only overwrite re-ingest "
                f"of {available} for testing; prod untouched."
            )
        else:
            print("No new quarters at source; nothing to do.")
            return

        result = clean_quarters_task(quarters=quarters, work_dir=work_dir)

        # Dev: upload staging + materialize/test.
        for table in TABLES:
            data_path = result.get(table)
            if not data_path:
                continue
            upload_to_gcs(
                data_path=data_path,
                dataset_id=DATASET_ID,
                table_id=table,
                bucket_name="basedosdados-dev",
                dump_mode=dump_mode,
                source_format="parquet",
            )
            run_dbt(
                dataset_id=DATASET_ID,
                table_id=table,
                dbt_command="run/test",
                target="dev",
            )

        if not materialize_to_prod:
            return

        # Prod: only genuine new quarters are ever appended — a forced smoke test
        # (dump_mode="overwrite") never reaches prod, so prod is never truncated.
        if not new_quarters:
            print("No genuine new quarters; skipping prod materialization.")
            return

        for table in TABLES:
            data_path = result.get(table)
            if not data_path:
                continue
            upload_to_gcs(
                data_path=data_path,
                dataset_id=DATASET_ID,
                table_id=table,
                bucket_name="basedosdados",
                dump_mode="append",
                source_format="parquet",
            )
            run_dbt(
                dataset_id=DATASET_ID,
                table_id=table,
                dbt_command="run/test",
                target="prod",
            )

        if update_metadata:
            # Rolls both DateTimeRanges and re-issues the Row Access Policies for
            # each table (part_bdpro), then advances the source Update to the
            # newest quarter — committed last, only after prod succeeded.
            for table in TABLES:
                register_table_materialization_task(
                    dataset_id=DATASET_ID,
                    table_id=table,
                    coverage=_COVERAGE[table],
                    env="prod",
                    bq_project="basedosdados",
                )
            commit_source_update_task(
                dataset_id=DATASET_ID,
                table_id=ANCHOR_TABLE,
                source_max_date=max_date,
                env="prod",
                date_format="%Y-%m-%d",
            )
    finally:
        # Covers early returns (no new data, dev-only) and any exception. A
        # process/local worker reuses its filesystem and the SIDA ZIPs are large.
        shutil.rmtree(work_dir, ignore_errors=True)


# PGFN republishes quarterly on no fixed day; poll a few days each month at 15:00
# BRT. The source-boundary check no-ops until a genuinely new quarter appears.
# pyrefly: ignore [missing-attribute]
br_mf_divida_ativa_flow.deploy_schedules = [
    {"cron": constants.SCHEDULE_CRON.value, "timezone": "America/Sao_Paulo"}
]
# The clean step streams the SIDA quarter in 400k-row chunks, so peak RAM is
# modest; give headroom for the pandas->arrow buffers and the GCS upload.
# pyrefly: ignore [missing-attribute]
br_mf_divida_ativa_flow.job_variables = {"memory": "8Gi"}
