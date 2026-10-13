"""
Flows de br_bndes_operacoes_contratadas — Prefect 3.

Um @flow por tabela, com os parâmetros de run e o cron. O corpo das cinco
tabelas antigas fica nas funções `_run_*` deste arquivo (a indiretas e a não
automáticas dividem `_run_operacoes`); o da operacoes_pre_embarque, no próprio
flow. Download e limpeza ficam em constants/utils/tasks deste diretório.

Poll deferido: poll_source_for_update_task detecta novidade (grava só o Poll);
commit_source_update_task grava o Update logo após essa confirmação, antes de
baixar e materializar. O poll nunca lê o RawDataSource.Update para decidir
(compara com o Table.Update), então adiantar esse valor não trava runs futuras
e mostra que havia dado novo publicado mesmo que o flow falhe no meio.

Duas datas, que não se confundem:
- Update da fonte (poll/commit): data de publicação, o last_modified do CKAN.
- Período da tabela (registro da materialização): data da operação, com dia
  (AllFree + DateOnly + DateFormat.YEAR_MD), porque nenhuma tabela tem coluna
  de mês. A exceção é a operacoes_exportacao_servicos, carga única, por ano.
"""

from prefect.schedules import Cron

from pipelines.datasets.br_bndes_operacoes_contratadas.tasks import (
    clean_and_partition,
    clean_and_partition_administracao_publica,
    clean_and_partition_exportacao_bens,
    clean_and_partition_exportacao_servicos,
    clean_table,
    download_administracao_publica_csv,
    download_exportacao_bens_csv,
    download_exportacao_servicos_csv,
    download_source_csv,
    download_table,
    get_source_last_modified_pre_embarque,
    get_source_max_date,
    get_source_max_date_administracao_publica,
    get_source_max_date_exportacao_bens,
    get_source_max_date_exportacao_servicos,
)
from pipelines.utils.flow import flow
from pipelines.utils.metadata.domain import (
    AllFree,
    DateFormat,
    DateOnly,
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


def _run_operacoes(
    dataset_id: str,
    table_id: str,
    materialize_after_dump: bool,
    update_metadata: bool,
    target: str,
    force_run: bool,
) -> None:
    """
    Orquestra o crawler: poll deferido -> download -> clean -> upload -> dbt.

    Args:
        dataset_id (str): ID do dataset no GCP/BigQuery.
        table_id (str): ID da tabela no GCP/BigQuery.
        materialize_after_dump (bool): se False, para apos o dbt em dev (nao toca prod).
        update_metadata (bool): se True, registra materializacao e commita o Update da fonte.
        target (str): target do dbt na etapa de prod.
        force_run (bool): ignora o early-return quando nao ha novidade.
    """

    rename_flow_run_dataset_table(
        prefix="Dump: ", dataset_id=dataset_id, table_id=table_id
    )

    source_max_date = get_source_max_date(table_id=table_id)

    has_new_data = poll_source_for_update_task(
        dataset_id=dataset_id,
        table_id=table_id,
        source_max_date=source_max_date,
        env="prod",
        date_format=DateFormat.YEAR_MD,
        compare_against="table_update",
    )

    if not has_new_data and not force_run:
        return

    # Comita o Update da fonte já aqui, antes de baixar/materializar: se o
    # flow falhar no meio, o metadado da fonte ainda reflete que havia dado
    # novo publicado, mesmo que a tabela não tenha sido atualizada.
    commit_source_update_task(
        dataset_id=dataset_id,
        table_id=table_id,
        source_max_date=source_max_date,
        env="prod",
        date_format=DateFormat.YEAR_MD,
        update_metadata=update_metadata,
        materialize_after_dump=materialize_after_dump,
    )

    csv_path = download_source_csv(table_id=table_id)

    output_dir = clean_and_partition(csv_path=csv_path, table_id=table_id)

    upload_to_gcs(
        data_path=output_dir,
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
        data_path=output_dir,
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
                date_column=DateOnly(col="data_contratacao"),
                date_format=DateFormat.YEAR_MD,
            ),
            env="prod",
            bq_project="basedosdados",
        )


def _run_operacoes_exportacao_bens(
    dataset_id: str,
    table_id: str,
    materialize_after_dump: bool,
    update_metadata: bool,
    force_run: bool,
) -> None:
    """
    Orquestra o crawler da 3a tabela: poll deferido -> download -> clean -> upload -> dbt.

    Mesma receita das irmas, trocando so as tasks especificas da tabela.

    Args:
        dataset_id (str): ID do dataset no GCP/BigQuery.
        table_id (str): ID da tabela no GCP/BigQuery.
        materialize_after_dump (bool): se False, para apos o dbt em dev (nao toca prod).
        update_metadata (bool): se True, registra materializacao e commita o Update da fonte.
        force_run (bool): ignora o early-return quando nao ha novidade.
    """
    rename_flow_run_dataset_table(
        prefix="Dump: ", dataset_id=dataset_id, table_id=table_id
    )

    source_max_date = get_source_max_date_exportacao_bens()

    has_new_data = poll_source_for_update_task(
        dataset_id=dataset_id,
        table_id=table_id,
        source_max_date=source_max_date,
        env="prod",
        date_format=DateFormat.YEAR_MD,
        compare_against="table_update",
    )

    if not has_new_data and not force_run:
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

    csv_path = download_exportacao_bens_csv()

    output_dir = clean_and_partition_exportacao_bens(csv_path=csv_path)

    upload_to_gcs(
        data_path=output_dir,
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
        data_path=output_dir,
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
        target="prod",
    )

    if update_metadata:
        register_table_materialization_task(
            dataset_id=dataset_id,
            table_id=table_id,
            coverage=AllFree(
                date_column=DateOnly(col="data_contratacao"),
                date_format=DateFormat.YEAR_MD,
            ),
            env="prod",
            bq_project="basedosdados",
        )


def _run_operacoes_exportacao_servicos(
    dataset_id: str,
    table_id: str,
    materialize_after_dump: bool,
    update_metadata: bool,
    target: str,
    force_run: bool,
) -> None:
    """
    Orquestra o crawler da 4a tabela: poll deferido -> download -> clean -> upload -> dbt.

    Mesma receita das irmas, trocando so as tasks especificas da tabela. A
    tabela e carga unica — a serie da fonte termina em 2015 e o flow nao tem
    cron —, entao na pratica so roda por disparo manual, com force_run.

    Args:
        dataset_id (str): ID do dataset no GCP/BigQuery.
        table_id (str): ID da tabela no GCP/BigQuery.
        materialize_after_dump (bool): se False, para apos o dbt em dev (nao toca prod).
        update_metadata (bool): se True, registra materializacao e commita o Update da fonte.
        target (str): target do dbt na etapa de prod.
        force_run (bool): ignora o early-return quando nao ha novidade.
    """
    rename_flow_run_dataset_table(
        prefix="Dump: ", dataset_id=dataset_id, table_id=table_id
    )

    source_max_date = get_source_max_date_exportacao_servicos()

    has_new_data = poll_source_for_update_task(
        dataset_id=dataset_id,
        table_id=table_id,
        source_max_date=source_max_date,
        env="prod",
        date_format=DateFormat.YEAR_MD,
        compare_against="table_update",
    )

    if not has_new_data and not force_run:
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

    csv_path = download_exportacao_servicos_csv()

    output_dir = clean_and_partition_exportacao_servicos(csv_path=csv_path)

    upload_to_gcs(
        data_path=output_dir,
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
        data_path=output_dir,
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


def _run_operacoes_administracao_publica(
    dataset_id: str,
    table_id: str,
    materialize_after_dump: bool,
    update_metadata: bool,
    target: str,
    force_run: bool,
) -> None:
    """
    Orquestra o crawler da 2a tabela: poll deferido -> download -> clean -> upload -> dbt.

    Mesma receita de _run_operacoes_indiretas_automaticas: poll deferido — o
    poll_source_for_update_task grava so o Poll ao detectar novidade; o
    commit_source_update_task grava o Update da fonte logo apos essa
    confirmacao, antes de baixar/materializar prod.

    Args:
        dataset_id (str): ID do dataset no GCP/BigQuery.
        table_id (str): ID da tabela no GCP/BigQuery.
        materialize_after_dump (bool): se False, para apos o dbt em dev (nao toca prod).
        update_metadata (bool): se True, registra materializacao e commita o Update da fonte.
        target (str): target do dbt na etapa de prod.
        force_run (bool): ignora o early-return quando nao ha novidade.
    """
    rename_flow_run_dataset_table(
        prefix="Dump: ", dataset_id=dataset_id, table_id=table_id
    )

    source_max_date = get_source_max_date_administracao_publica()

    has_new_data = poll_source_for_update_task(
        dataset_id=dataset_id,
        table_id=table_id,
        source_max_date=source_max_date,
        env="prod",
        date_format=DateFormat.YEAR_MD,
        compare_against="table_update",
    )

    if not has_new_data and not force_run:
        return

    # Comita o Update da fonte já aqui, antes de baixar/materializar: se o
    # flow falhar no meio, o metadado da fonte ainda reflete que havia dado
    # novo publicado, mesmo que a tabela não tenha sido atualizada.
    commit_source_update_task(
        dataset_id=dataset_id,
        table_id=table_id,
        source_max_date=source_max_date,
        env="prod",
        date_format=DateFormat.YEAR_MD,
        update_metadata=update_metadata,
        materialize_after_dump=materialize_after_dump,
    )

    csv_path = download_administracao_publica_csv()

    output_dir = clean_and_partition_administracao_publica(csv_path=csv_path)

    upload_to_gcs(
        data_path=output_dir,
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
        data_path=output_dir,
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
                date_column=DateOnly(col="data_nivel_atual"),
                date_format=DateFormat.YEAR_MD,
            ),
            env="prod",
            bq_project="basedosdados",
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
    force_run: bool = False,
) -> None:
    """Atualiza a operacoes_pre_embarque quando o BNDES republica o recurso."""
    rename_flow_run_dataset_table(
        prefix="Dump: ", dataset_id=dataset_id, table_id=table_id
    )

    source_max_date = get_source_last_modified_pre_embarque()

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
        target="prod",
    )

    if update_metadata:
        register_table_materialization_task(
            dataset_id=dataset_id,
            table_id=table_id,
            coverage=AllFree(
                date_column=DateOnly(col="data_contratacao"),
                date_format=DateFormat.YEAR_MD,
            ),
            env="prod",
            bq_project="basedosdados",
        )


br_bndes_operacoes_contratadas__operacoes_pre_embarque.deploy_schedules = [
    Cron("25 2 * * 1", timezone="America/Sao_Paulo")
]
