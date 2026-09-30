"""
Flows for br_bndes_operacoes_contratadas — Prefect 3.

Wrapper @flow do crawler: expoe os parametros de run e o cron. A logica de
orquestracao (poll deferido) vive em pipelines/crawler/bndes/flows.py.

A exceção é a operacoes_pre_embarque, que já segue o desenho de diretório
único: o corpo fica no próprio flow, e o download e a limpeza, em
constants/utils/tasks deste diretório.
"""

from prefect.schedules import Cron

from pipelines.crawler.bndes.flows import (
    _run_operacoes,
    _run_operacoes_administracao_publica,
    _run_operacoes_exportacao_bens,
    _run_operacoes_exportacao_servicos,
)
from pipelines.datasets.br_bndes_operacoes_contratadas.tasks import (
    clean_table,
    download_table,
    get_source_last_modified,
)
from pipelines.utils.flow import flow
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


@flow(
    name="br_bndes_operacoes_contratadas__operacoes_indiretas_automaticas",
    log_prints=True,
    description=(
        "Dump da tabela operacoes_indiretas_automaticas "
        "do dataset br_bndes_operacoes_contratadas."
    ),
)
def br_bndes_operacoes_contratadas__operacoes_indiretas_automaticas(
    dataset_id: str = "br_bndes_operacoes_contratadas",
    table_id: str = "operacoes_indiretas_automaticas",
    materialize_after_dump: bool = True,
    update_metadata: bool = True,
    target: str = "prod",
    force_run: bool = False,
) -> None:
    _run_operacoes(
        dataset_id=dataset_id,
        table_id=table_id,
        materialize_after_dump=materialize_after_dump,
        update_metadata=update_metadata,
        target=target,
        force_run=force_run,
    )


br_bndes_operacoes_contratadas__operacoes_indiretas_automaticas.deploy_schedules = [
    Cron("0 6 * * 1", timezone="America/Sao_Paulo")
]


@flow(
    name="br_bndes_operacoes_contratadas__operacoes_nao_automaticas",
    log_prints=True,
    description=(
        "Dump da tabela operacoes_nao_automaticas "
        "do dataset br_bndes_operacoes_contratadas."
    ),
)
def br_bndes_operacoes_contratadas__operacoes_nao_automaticas(
    dataset_id: str = "br_bndes_operacoes_contratadas",
    table_id: str = "operacoes_nao_automaticas",
    materialize_after_dump: bool = True,
    update_metadata: bool = True,
    target: str = "prod",
    force_run: bool = False,
) -> None:
    _run_operacoes(
        dataset_id=dataset_id,
        table_id=table_id,
        materialize_after_dump=materialize_after_dump,
        update_metadata=update_metadata,
        target=target,
        force_run=force_run,
    )


br_bndes_operacoes_contratadas__operacoes_nao_automaticas.deploy_schedules = [
    Cron("0 6 * * 1", timezone="America/Sao_Paulo")
]


@flow(
    name="br_bndes_operacoes_contratadas__operacoes_administracao_publica",
    log_prints=True,
    description=(
        "Dump da tabela operacoes_administracao_publica "
        "do dataset br_bndes_operacoes_contratadas."
    ),
)
def br_bndes_operacoes_contratadas__operacoes_administracao_publica(
    dataset_id: str = "br_bndes_operacoes_contratadas",
    table_id: str = "operacoes_administracao_publica",
    materialize_after_dump: bool = True,
    update_metadata: bool = True,
    target: str = "prod",
    force_run: bool = False,
) -> None:
    _run_operacoes_administracao_publica(
        dataset_id=dataset_id,
        table_id=table_id,
        materialize_after_dump=materialize_after_dump,
        update_metadata=update_metadata,
        target=target,
        force_run=force_run,
    )


# cron semanal (segunda 06h BRT), igual a outra tabela; a fonte atualiza mensal e o poll
# deferido no-opa quando nao ha novidade. Ajuste se quiser outra janela.
br_bndes_operacoes_contratadas__operacoes_administracao_publica.deploy_schedules = [
    Cron("0 6 * * 1", timezone="America/Sao_Paulo")
]


@flow(
    name="br_bndes_operacoes_contratadas__operacoes_exportacao_bens",
    log_prints=True,
    description=(
        "Dump da tabela operacoes_exportacao_bens "
        "do dataset br_bndes_operacoes_contratadas."
    ),
)
def br_bndes_operacoes_contratadas__operacoes_exportacao_bens(
    dataset_id: str = "br_bndes_operacoes_contratadas",
    table_id: str = "operacoes_exportacao_bens",
    materialize_after_dump: bool = True,
    update_metadata: bool = True,
    force_run: bool = False,
) -> None:
    _run_operacoes_exportacao_bens(
        dataset_id=dataset_id,
        table_id=table_id,
        materialize_after_dump=materialize_after_dump,
        update_metadata=update_metadata,
        force_run=force_run,
    )


br_bndes_operacoes_contratadas__operacoes_exportacao_bens.deploy_schedules = [
    Cron("0 6 * * 1", timezone="America/Sao_Paulo")
]


@flow(
    name="br_bndes_operacoes_contratadas__operacoes_exportacao_servicos",
    log_prints=True,
    description=(
        "Dump da tabela operacoes_exportacao_servicos "
        "do dataset br_bndes_operacoes_contratadas."
    ),
)
def br_bndes_operacoes_contratadas__operacoes_exportacao_servicos(
    dataset_id: str = "br_bndes_operacoes_contratadas",
    table_id: str = "operacoes_exportacao_servicos",
    materialize_after_dump: bool = True,
    update_metadata: bool = True,
    target: str = "prod",
    force_run: bool = False,
) -> None:
    _run_operacoes_exportacao_servicos(
        dataset_id=dataset_id,
        table_id=table_id,
        materialize_after_dump=materialize_after_dump,
        update_metadata=update_metadata,
        target=target,
        force_run=force_run,
    )


# Sem cron, ao contrário das irmãs: a série da fonte termina em 2015-04-28 e
# não recebe operação nova desde então. A tabela entra como carga única, por
# disparo manual. Se a fonte voltar a publicar, basta acrescentar
# deploy_schedules aqui — o resto do flow já está pronto para o poll.


@flow(
    name="br_bndes_operacoes_contratadas__operacoes_pre_embarque",
    log_prints=True,
)
def br_bndes_operacoes_contratadas__operacoes_pre_embarque(
    dataset_id: str = "br_bndes_operacoes_contratadas",
    table_id: str = "operacoes_pre_embarque",
    materialize_after_dump: bool = True,
    update_metadata: bool = True,
    target: str = "prod",
    force_run: bool = False,
) -> None:
    """Atualiza a operacoes_pre_embarque quando o BNDES republica o recurso."""
    rename_flow_run_dataset_table(
        prefix="Dump: ", dataset_id=dataset_id, table_id=table_id
    )

    source_max_date = get_source_last_modified()

    if not force_run:
        has_new_data = poll_source_for_update_task(
            dataset_id=dataset_id,
            table_id=table_id,
            source_max_date=source_max_date,
            env="prod",
            date_format=DateFormat.YEAR_MD,
            compare_against="table_update",
        )
        if not has_new_data:
            print(f"Não há atualizações para a tabela {table_id}!")
            return

    commit_source_update_task(
        dataset_id=dataset_id,
        table_id=table_id,
        source_max_date=source_max_date,
        env="prod",
        date_format=DateFormat.YEAR_MD,
        update_metadata=update_metadata,
        materialize_after_dump=materialize_after_dump,
    )

    csv_path = download_table()
    filepath = clean_table(csv_path=csv_path)

    upload_to_gcs(
        data_path=filepath,
        dataset_id=dataset_id,
        table_id=table_id,
        bucket_name="basedosdados-dev",
        dump_mode="overwrite",
        source_format="parquet",
    )

    run_dbt(
        dataset_id=dataset_id,
        table_id=table_id,
        dbt_command="run/test",
        target="dev",
    )

    if not materialize_after_dump:
        return

    upload_to_gcs(
        data_path=filepath,
        dataset_id=dataset_id,
        table_id=table_id,
        bucket_name="basedosdados",
        dump_mode="overwrite",
        source_format="parquet",
    )

    run_dbt(
        dataset_id=dataset_id,
        table_id=table_id,
        dbt_command="run/test",
        target=target,
    )

    if update_metadata:
        register_table_materialization_task(
            dataset_id=dataset_id,
            table_id=table_id,
            coverage=AllFree(
                date_column=YearOnly(col="ano"), date_format=DateFormat.YEAR
            ),
            env="prod",
            bq_project="basedosdados",
        )


br_bndes_operacoes_contratadas__operacoes_pre_embarque.deploy_schedules = [
    Cron("25 2 * * 1", timezone="America/Sao_Paulo")
]
