"""
Flow compartilhado para br_tse_eleicoes — Prefect 3.
"""

from prefect import flow

from pipelines.datasets.br_tse_eleicoes.tasks import (
    flows_control,
    get_data_source_max_date,
    preparing_data,
)
from pipelines.utils.metadata.domain import (
    AllFree,
    DateFormat,
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


def _tse_flow(table_id: str, cron: str | None):
    @flow(
        name=f"br_tse_eleicoes__{table_id}",
        log_prints=True,
    )
    def _flow(
        dataset_id: str = "br_tse_eleicoes",
        table_id: str = table_id,
        year: int | str = 2026,
        materialize_after_dump: bool = True,
        dbt_alias: bool = True,
        update_metadata: bool = True,
        target: str = "prod",
        force_run: bool = False,
    ) -> None:
        # pyrefly: ignore [unused-coroutine]
        rename_flow_run_dataset_table(
            prefix="Dump: ", dataset_id=dataset_id, table_id=table_id
        )

        flow = flows_control(table_id=table_id, mode="prod", year=year)
        data_source_max_date = get_data_source_max_date(flow_class=flow)

        if not force_run:
            has_new_data = poll_source_for_update_task(
                dataset_id=dataset_id,
                table_id=table_id,
                source_max_date=data_source_max_date,
                env="prod",
                date_format="%Y-%m-%d",
                compare_against="table_update",
            )
            if not has_new_data:
                return

        # Comita o Update da fonte já aqui, antes de baixar/materializar: se o
        # flow falhar no meio, o metadado da fonte ainda reflete que havia dado
        # novo publicado, mesmo que a tabela não tenha sido atualizada.
        commit_source_update_task(
            dataset_id=dataset_id,
            table_id=table_id,
            source_max_date=data_source_max_date,
            env="prod",
            date_format="%Y-%m-%d",
            update_metadata=update_metadata,
            materialize_after_dump=materialize_after_dump,
        )

        ready_data_path = preparing_data(flow_class=flow)

        upload_to_gcs(
            data_path=ready_data_path,
            dataset_id=dataset_id,
            table_id=table_id,
            bucket_name="basedosdados-dev",
            dump_mode="append",
        )

        run_dbt(
            dataset_id=dataset_id,
            table_id=table_id,
            dbt_command="run/test",
            dbt_alias=dbt_alias,
            target="dev",
        )

        if not materialize_after_dump:
            return

        upload_to_gcs(
            data_path=ready_data_path,
            dataset_id=dataset_id,
            table_id=table_id,
            bucket_name="basedosdados",
            dump_mode="append",
        )

        run_dbt(
            dataset_id=dataset_id,
            table_id=table_id,
            dbt_command="run/test",
            dbt_alias=dbt_alias,
            target=target,
        )

        if update_metadata:
            # Legado lia data_eleicao (DATE) mas formatava p/ "%Y" → cobertura
            # year-only. O domínio refatorado proíbe DateOnly+YEAR (R4); a coluna
            # `ano` (partição canônica das tabelas TSE) dá a mesma granularidade
            # ano-only de forma R4-limpa. ⚠ oráculo in-pod: comparar só o ano.
            register_table_materialization_task(
                dataset_id=dataset_id,
                table_id=table_id,
                coverage=AllFree(
                    date_column=YearOnly(col="ano"),
                    date_format=DateFormat.YEAR,
                ),
                env="prod",
                bq_project="basedosdados",
            )

    # pyrefly: ignore [missing-attribute]
    _flow.deploy_schedules = (
        [{"cron": cron, "timezone": "America/Sao_Paulo"}] if cron else []
    )
    return _flow


# Schedules eram comentados no Prefect 0 — mantém sem cron por enquanto.
br_tse_eleicoes__candidatos = _tse_flow("candidatos", None)
br_tse_eleicoes__bens_candidato = _tse_flow("bens_candidato", None)
br_tse_eleicoes__despesas_candidato = _tse_flow("despesas_candidato", None)
br_tse_eleicoes__receitas_candidato = _tse_flow("receitas_candidato", None)
