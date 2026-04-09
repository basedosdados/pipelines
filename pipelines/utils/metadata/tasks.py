"""
Tasks for metadata
"""

from datetime import datetime, timedelta

import basedosdados as bd
import pandas as pd
from prefect import task

from pipelines.constants import constants
from pipelines.utils.metadata.utils import (
    able_to_query_bigquery_metadata,
    check_if_values_are_accepted,
    create_update,
    extract_last_date_from_bq,
    get_api_last_update_date,
    get_api_most_recent_date,
    get_billing_project_id,
    get_coverage_parameters,
    get_id,
    get_table_status,
    get_url,
    update_data_source_poll,
    update_data_source_update_date,
    update_date_from_bq_metadata,
    update_row_access_policy,
)
from pipelines.utils.utils import get_redis_client, log


@task
def get_today_date():
    d = datetime.today()
    return d.strftime("%Y-%m-%d")


@task
def update_django_metadata(
    dataset_id: str,
    table_id: str,
    date_column_name: dict | None = None,
    date_format: str = "%Y-%m",
    coverage_type: str = "part_bdpro",
    time_delta: dict | None = None,
    prefect_mode: str = "prod",
    api_mode: str = "prod",
    bq_project: str = "basedosdados",
    historical_database: bool = True,
):
    """
          Updates temporal coverage Django metadata. Version 1.3.

    Args:
          dataset_id (str): O ID do conjunto de dados.
          table_id (str): O ID da tabela dentro do conjunto de dados.
          nome_coluna_data (dict): Um dicionário especificando os nomes das colunas usadas para extrair a cobertura temporal.
              Chaves válidas estão descritas no arquivo 'constants.py'.
          date_format (str): O formato da data a ser atualizado no Django.
          coverage_type (str): pode ser "part_bdpro", "all_bdpro" ou "all_free"
          time_delta (dict): dicionário com unidade temporal e valor do delta a ser aplicado caso 'part_bdpro'
          prefect_mode (str): colocar o materialization_mode do flow
          api_mode (str): pode ser 'prod ou 'staging'
          bq_project (str): projeto que será consultado para obter a cobertura temporal
          historical_database (bool): marcar como False para casos em que a base nao possua uma coluna que represente a data de cobertura

      Returns:
          -   None

          Raises:
              -   Exception: If the  coverage_type, time_delta or date_column_name is not supported.
              -   Exception: If try to update published table with non prod data

    """

    date_column_name = (
        {"year": "ano", "month": "mes"}
        if date_column_name is None
        else date_column_name
    )
    time_delta = {"months": 6} if time_delta is None else time_delta

    check_if_values_are_accepted(
        coverage_type=coverage_type,
        time_delta=time_delta,
        date_column_name=date_column_name,
    )

    billing_project_id = get_billing_project_id(mode=prefect_mode)

    backend = bd.Backend(graphql_url=get_url(api_mode))

    django_table_id = backend._get_table_id_from_name(
        gcp_dataset_id=dataset_id, gcp_table_id=table_id
    )

    if api_mode == "prod" and bq_project != "basedosdados":
        log(
            "WARNING: Production API Mode with Non-Production Project Selected"
        )

        status = get_table_status(table_id=django_table_id, backend=backend)
        if status != "under_review":
            raise ValueError(
                "The table status should be under_review to update metadata with non-production data"
            )

    last_date = extract_last_date_from_bq(
        dataset_id,
        table_id,
        date_format,
        date_column_name,
        billing_project_id,
        bq_project,
        historical_database,
    )

    free_parameters, bdpro_parameters = get_coverage_parameters(
        coverage_type,
        last_date,
        time_delta,
        django_table_id,
        date_format,
        historical_database,
        backend=backend,
    )

    if free_parameters is not None:
        create_update(
            query_class="allDatetimerange",
            query_parameters={"$coverage_Id: ID": free_parameters["coverage"]},
            mutation_class="CreateUpdateDateTimeRange",
            mutation_parameters=free_parameters,
            update=True,
            backend=backend,
        )

    if bdpro_parameters is not None:
        create_update(
            query_class="allDatetimerange",
            query_parameters={
                "$coverage_Id: ID": bdpro_parameters["coverage"]
            },
            mutation_class="CreateUpdateDateTimeRange",
            mutation_parameters=bdpro_parameters,
            update=True,
            backend=backend,
        )

    if coverage_type == "part_bdpro":
        update_row_access_policy(
            bq_project,
            dataset_id,
            table_id,
            billing_project_id,
            date_column_name,
            date_format,
            free_parameters,
        )

    if able_to_query_bigquery_metadata(billing_project_id, bq_project):
        _, update_id = get_id(
            query_class="allUpdate",
            query_parameters={"$table_Id: ID": django_table_id},
            backend=backend,
        )

        latest = update_date_from_bq_metadata(
            dataset_id=dataset_id,
            table_id=table_id,
            billing_project_id=billing_project_id,
            project_id=bq_project,
        )

        create_update(
            query_class="allUpdate",
            query_parameters={"$id: ID": update_id},
            mutation_class="CreateUpdateUpdate",
            mutation_parameters={
                "id": update_id,
                "latest": latest.isoformat(),
            },
            update=True,
            backend=backend,
        )


@task(
    max_retries=constants.TASK_MAX_RETRIES.value,
    retry_delay=timedelta(seconds=constants.TASK_RETRY_DELAY.value),
)
def check_if_data_is_outdated(
    dataset_id: str,
    table_id: str,
    data_source_max_date: datetime,
    date_type: str = "data_max_date",
    date_format: str = "%Y-%m-%d",
) -> bool:
    """Essa task checa se há necessidade de atualizar os dados no BQ

    Args:
        dataset_id e table_id(string): permite encontrar na api a última data de cobertura
        date_format (str): O formato da data a ser procurado no Django
        data_source_max_date (date): A data mais recente dos dados da fonte original

    Returns:
        bool: TRUE se a data da fonte original for maior que a data mais recente registrada na API e FALSE caso contrário.
    """

    if isinstance(data_source_max_date, datetime):
        data_source_max_date = data_source_max_date.date()
    if isinstance(data_source_max_date, str):
        data_source_max_date = datetime.strptime(
            data_source_max_date, date_format
        ).date()
    if isinstance(data_source_max_date, pd.Timestamp):
        data_source_max_date = data_source_max_date.date()

    backend = bd.Backend(graphql_url=constants.API_URL.value["prod"])

    if date_type == "data_max_date":
        data_api = get_api_most_recent_date(
            dataset_id=dataset_id,
            table_id=table_id,
            backend=backend,
            date_format=date_format,
        )

    if date_type == "last_update_date":
        data_api = get_api_last_update_date(
            dataset_id=dataset_id, table_id=table_id, backend=backend
        )

    log(f"Data na fonte: {data_source_max_date}")
    log(f"Data nos metadados da BD: {data_api}")

    update_data_source_poll(dataset_id, table_id, backend)
    # Compara as datas para verificar se há atualizações
    if data_source_max_date > data_api:
        log("Há atualizações disponíveis")
        update_data_source_update_date(
            dataset_id,
            table_id,
            date_type,
            data_source_max_date,
            backend,
        )
        return True  # Há atualizações disponíveis
    else:
        log("Não há novas atualizações disponíveis")
        return False


@task(
    max_retries=constants.TASK_MAX_RETRIES.value,
    retry_delay=timedelta(seconds=constants.TASK_RETRY_DELAY.value),
)
def check_if_data_is_outdated_by_size(
    dataset_id: str,
    table_id: str,
    byte_length: int,
    local_execution: bool = False,
) -> bool:
    """Essa task checa se há necessidade de atualizar os dados no BQ baseando-se no tamanho do dado

    Args:
        dataset_id e table_id (string): permite encontrar no redis a última data de cobertura
        byte_length (int): O tamanho do dado na fonte original em bytes
        local_execution (bool): Se True, conecta ao redis em localhost.
        *NOTE: para conectar o REDIS usando localhost é preciso ter uma proxy ativa com pod do redis no Ks8.
          *quando o flow roda na cloud o client se conecta ao REDIS utilizando o DNS do KS8

    Returns:
        bool: TRUE se o tamanho for maior que o registrado e FALSE caso contrário.
    """
    # NOTE: Hardcodei igual o padrão da task check_if_data_is_outdated
    backend = bd.Backend(graphql_url=constants.API_URL.value["prod"])

    if local_execution:
        redis_client = get_redis_client(host="localhost")
    else:
        redis_client = get_redis_client()

    today = datetime.today().strftime("%Y-%m-%d")

    dataset_data = redis_client.get(dataset_id) or {}
    table_data = dataset_data.get(table_id, {})

    if table_data:
        sorted_dates = sorted(table_data.keys(), reverse=True)
        latest_date = sorted_dates[0]
        latest_byte_length = table_data[latest_date]

        log(f"Tamanho na fonte: {byte_length}")
        log(f"Último tamanho registrado ({latest_date}): {latest_byte_length}")

        if byte_length > latest_byte_length:
            log("Há atualizações disponíveis (tamanho maior)")
            table_data[today] = byte_length
        elif byte_length == latest_byte_length:
            log("Não há novas atualizações disponíveis (tamanho igual)")
            update_data_source_poll(dataset_id, table_id, backend)
            return False
        else:
            log(
                f"ALERTA: Tamanho na fonte ({byte_length}) é MENOR que o último registrado ({latest_byte_length}). Isto pode indicar que houve uma alteração na tabela original que reduziu o tamanho dela. Ex. Uma coluna foi deletada; Variáveis de texto foram codificadas e etc;",
                "error",
            )
            log(f"Histórico de tamanhos: {table_data}", "error")
            raise ValueError(
                f"Tamanho na fonte ({byte_length}) é menor que o último registrado ({latest_byte_length})"
            )
    else:
        log(
            f"Nenhum registro anterior encontrado para {dataset_id}.{table_id}. Registrando primeiro tamanho: {byte_length}"
        )
        table_data[today] = byte_length

    # Keep only the last 10 runs
    if len(table_data) > 10:
        sorted_dates = sorted(table_data.keys(), reverse=True)
        table_data = {date: table_data[date] for date in sorted_dates[:10]}

    # Save back to Redis
    dataset_data[table_id] = table_data
    redis_client.set(dataset_id, dataset_data)

    ##NOTE:       #essa funcao tem um fallback que, se o date_type informado não é last_update_date
    # ela gera um today date adiciona hh:mm:ss e usa como data para subir o dado...
    # Não faz sentido;
    # para mim, no fundo, seria somente passar qualquer tipo de parametro e ele geraria um todfay date
    # que é o quie a minha task deve fazer......

    # isto é, fornecendo params aleatórios para date_type e data_spurce_max_date ela cria a today date kk
    update_data_source_update_date(
        dataset_id=dataset_id,
        table_id=table_id,
        date_type="today_date",
        data_source_max_date="1990-01-01",
        backend=backend,
    )

    return True


@task
def task_get_api_most_recent_date(
    dataset_id,
    table_id,
    date_format,
    api_mode: str = "prod",
):
    backend = bd.Backend(graphql_url=get_url(api_mode))
    return get_api_most_recent_date(
        dataset_id=dataset_id,
        table_id=table_id,
        date_format=date_format,
        backend=backend,
    )
