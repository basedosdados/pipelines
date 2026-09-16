"""Flows for br_cgu_orcamento_publico — Prefect 3.

Orçamento e execução da despesa do Governo Federal (Portal da Transparência,
CGU): dotação inicial e autorizada, valor empenhado e valor realizado, por
órgão, unidade orçamentária, função, subfunção, programa, ação e natureza da
despesa, um exercício por arquivo desde 2014.

The portal restates closed exercises — the 2024 file gained rows and value
months after the year ended — so every run re-downloads the whole history and
replaces the table (``dump_mode="overwrite"``). The whole source is a handful of
megabytes, which is what makes a full rebuild the cheap option as well as the
correct one.

Deploy: ``.github/scripts/deploy_flows.py`` auto-discovers
``br_cgu_orcamento_publico_flow``; the dev pool ignores the schedule, the prod
pool activates it.
"""

import shutil
import tempfile

from prefect import flow

from pipelines.datasets.br_cgu_orcamento_publico.constants import constants
from pipelines.datasets.br_cgu_orcamento_publico.tasks import (
    clean_orcamento,
    download_orcamento,
    probe_source,
)
from pipelines.utils.metadata.domain import AllFree, DateFormat, YearOnly
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
TABLE_ID = constants.TABLE_ID.value

# Annual table, so no BD Pro rolling window: the paywall applies to tables
# refreshed monthly or more often. Coverage is the exercise year.
_COVERAGE = AllFree(
    date_column=YearOnly(col="ano_exercicio"), date_format=DateFormat.YEAR
)


@flow(name="br_cgu_orcamento_publico", log_prints=True)
def br_cgu_orcamento_publico_flow(
    materialize_to_prod: bool = True,
    update_metadata: bool = True,
    force_run: bool = False,
) -> None:
    """Rebuild br_cgu_orcamento_publico.orcamento from the Portal da Transparência.

    Args:
        materialize_to_prod: Continue past the dev materialization to write the
            prod staging bucket and run dbt against ``target="prod"``. Set False
            to exercise only the dev half — required for a safe test run, since
            the default writes production.
        update_metadata: After a successful prod materialization, register table
            coverage and commit the source update. No effect when
            ``materialize_to_prod`` is False.
        force_run: Materialize even when the portal has not regenerated the
            files since the last refresh. Needed for the first run, whose
            ``Table.Update.latest`` is newer than the source's ``Last-Modified``.
    """
    # pyrefly: ignore [unused-coroutine]
    rename_flow_run_dataset_table(
        prefix="Dump: ", dataset_id=DATASET_ID, table_id=TABLE_ID
    )

    work_dir = tempfile.mkdtemp(prefix="br_cgu_orcamento_publico_")
    try:
        # One HEAD, before spending the download budget. The CGU host is behind
        # an AWS WAF rate rule that CAPTCHA-blocks bursts, so the run asks the
        # cheapest possible question first and bails when there is nothing new.
        max_modified = probe_source()

        # The exercise year only moves once a year while the file contents move
        # continuously, so freshness is the ZIP's Last-Modified compared against
        # Table.Update.latest — a publication timestamp against a wall clock,
        # which is what compare_against="table_update" is for.
        has_new_data = poll_source_for_update_task(
            dataset_id=DATASET_ID,
            table_id=TABLE_ID,
            source_max_date=max_modified,
            env="prod",
            date_format="%Y-%m-%d",
            compare_against="table_update",
        )
        if not has_new_data and not force_run:
            return

        downloaded = download_orcamento(work_dir=work_dir)
        result = clean_orcamento(
            work_dir=work_dir,
            input_dir=downloaded["input_dir"],
            years=downloaded["years"],
        )
        print(
            f"cleaned {len(result['rows'])} exercises, "
            f"{result['total']:,} rows: {result['rows']}"
        )

        commit_source_update_task(
            dataset_id=DATASET_ID,
            table_id=TABLE_ID,
            source_max_date=max_modified,
            env="prod",
            date_format="%Y-%m-%d",
            update_metadata=update_metadata,
            materialize_after_dump=materialize_to_prod,
        )

        if not materialize_to_prod:
            upload_to_gcs(
                data_path=result["path"],
                dataset_id=DATASET_ID,
                table_id=TABLE_ID,
                bucket_name="basedosdados-dev",
                dump_mode="overwrite",
                source_format="csv",
            )
            run_dbt(
                dataset_id=DATASET_ID,
                table_id=TABLE_ID,
                dbt_command="run",
                target="dev",
            )
            run_dbt(
                dataset_id=DATASET_ID,
                table_id=TABLE_ID,
                dbt_command="test",
                target="dev",
            )
            return

        upload_to_gcs(
            data_path=result["path"],
            dataset_id=DATASET_ID,
            table_id=TABLE_ID,
            bucket_name="basedosdados",
            dump_mode="overwrite",
            source_format="csv",
        )
        run_dbt(
            dataset_id=DATASET_ID,
            table_id=TABLE_ID,
            dbt_command="run",
            target="prod",
        )
        run_dbt(
            dataset_id=DATASET_ID,
            table_id=TABLE_ID,
            dbt_command="test",
            target="prod",
        )

        if update_metadata:
            register_table_materialization_task(
                dataset_id=DATASET_ID,
                table_id=TABLE_ID,
                coverage=_COVERAGE,
                env="prod",
                bq_project="basedosdados",
            )
    finally:
        # Covers both early returns and any exception. A process worker reuses
        # its filesystem between runs, so the scratch directory has to go.
        shutil.rmtree(work_dir, ignore_errors=True)


# The portal regenerates every exercise file together, currently around 05:00
# BRT. Weekly is enough for an annual table whose closed years are restated
# now and then; the poll guard no-ops a run that finds nothing new.
# pyrefly: ignore [missing-attribute]
br_cgu_orcamento_publico_flow.deploy_schedules = [
    {"cron": "23 6 * * 4", "timezone": "America/Sao_Paulo"}
]
# ~290k CSV rows streamed a few thousand at a time; the 4Gi default is ample.
# `memory` alone is silently dropped by the work pool template — the effective
# key is `memory_limit`.
# pyrefly: ignore [missing-attribute]
br_cgu_orcamento_publico_flow.job_variables = {
    "memory_limit": "2Gi",
    "memory_request": "1Gi",
}
