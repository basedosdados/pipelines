# -*- coding: utf-8 -*-
"""
Tasks for metadata
"""
import asyncio
from datetime import datetime
from datetime import timedelta
import pandas as pd
import basedosdados as bd

from pipelines.utils.metadata.utils_async import create_update_quality_checks_async
from prefect import task
from pipelines.constants import constants as constants_root
from pipelines.utils.constants import constants

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
from pipelines.utils.utils import log

@task
def get_today_date():
    d = datetime.today()
    return d.strftime("%Y-%m-%d")


@task  # noqa
def update_django_metadata(
    dataset_id: str,
    table_id: str,
    date_column_name: dict = {"year": "ano", "month": "mes"},
    date_format: str = "%Y-%m",
    coverage_type: str = "part_bdpro",
    time_delta: dict = {"months": 6},
    prefect_mode: str = "dev",
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

    check_if_values_are_accepted(
        coverage_type=coverage_type,
        time_delta=time_delta,
        date_column_name=date_column_name,
    )

    billing_project_id = get_billing_project_id(mode=prefect_mode)

    backend = bd.Backend(graphql_url=get_url(api_mode))

    django_table_id = backend._get_table_id_from_name(gcp_dataset_id=dataset_id, gcp_table_id=table_id)

    if api_mode == "prod" and bq_project != "basedosdados":
        log("WARNING: Production API Mode with Non-Production Project Selected")

        status = get_table_status(
            table_id=django_table_id, backend=backend
        )
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
        backend=backend
    )

    if free_parameters is not None:
        create_update(
            query_class="allDatetimerange",
            query_parameters={"$coverage_Id: ID": free_parameters["coverage"]},
            mutation_class="CreateUpdateDateTimeRange",
            mutation_parameters=free_parameters,
            update=True,
            backend=backend
        )

    if bdpro_parameters is not None:
        create_update(
            query_class="allDatetimerange",
            query_parameters={"$coverage_Id: ID": bdpro_parameters["coverage"]},
            mutation_class="CreateUpdateDateTimeRange",
            mutation_parameters=bdpro_parameters,
            update=True,
            backend=backend
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

    if able_to_query_bigquery_metadata(billing_project_id,bq_project):

        _, update_id = get_id(query_class='allUpdate',
                            query_parameters={"$table_Id: ID":django_table_id},
                            backend=backend)


        latest = update_date_from_bq_metadata(
            dataset_id=dataset_id,
            table_id=table_id,
            billing_project_id=billing_project_id,
            project_id=bq_project
        )

        create_update(
            query_class="allUpdate",
            query_parameters={"$id: ID": update_id},
            mutation_class="CreateUpdateUpdate",
            mutation_parameters={"id":update_id,"latest": latest.isoformat()},
            update=True,
            backend=backend
        )

@task(
    max_retries=constants_root.TASK_MAX_RETRIES.value,
    retry_delay=timedelta(seconds=constants_root.TASK_RETRY_DELAY.value),
)
def check_if_data_is_outdated(
    dataset_id: str,
    table_id: str,
    data_source_max_date: datetime,
    date_type: str = 'data_max_date',
    date_format: str = "%Y-%m-%d",
    api_mode: str = "prod",
) -> bool:
    """Essa task checa se há necessidade de atualizar os dados no BQ

    Args:
        dataset_id e table_id(string): permite encontrar na api a última data de cobertura
        date_format (str): O formato da data a ser procurado no Django
        data_source_max_date (date): A data mais recente dos dados da fonte original
        api_mode (str): pode ser 'prod ou 'staging'

    Returns:
        bool: TRUE se a data da fonte original for maior que a data mais recente registrada na API e FALSE caso contrário.
    """
    backend = bd.Backend(graphql_url=get_url(api_mode))

    if type(data_source_max_date) is datetime:
        data_source_max_date = data_source_max_date.date()
    if type(data_source_max_date) is str:
        data_source_max_date = datetime.strptime(
            data_source_max_date, date_format
        ).date()
    if type(data_source_max_date) is pd.Timestamp:
        data_source_max_date = data_source_max_date.date()

    backend = bd.Backend(graphql_url=constants.API_URL.value['prod'])

    if date_type == 'data_max_date':
        data_api = get_api_most_recent_date(
            dataset_id=dataset_id, table_id=table_id,backend = backend, date_format=date_format
        )

    if date_type == 'last_update_date':
        data_api = get_api_last_update_date(dataset_id=dataset_id, table_id=table_id, backend=backend)

    log(f"Data na fonte: {data_source_max_date}")
    log(f"Data nos metadados da BD: {data_api}")

    update_data_source_poll(dataset_id, table_id, backend)
    # Compara as datas para verificar se há atualizações
    if data_source_max_date > data_api:
        log("Há atualizações disponíveis")
        update_data_source_update_date(dataset_id,table_id,date_type,data_source_max_date, backend)
        return True  # Há atualizações disponíveis
    else:
        log("Não há novas atualizações disponíveis")
        return True


@task
def task_get_api_most_recent_date(dataset_id, table_id, date_format, api_mode: str = "prod",):

    backend = bd.Backend(graphql_url=get_url(api_mode))
    return get_api_most_recent_date(
        dataset_id=dataset_id, table_id=table_id, date_format=date_format, backend=backend
    )

#####             #####
## Quality check's   ##
#####             #####


@task
def query_tests_results() -> pd.DataFrame:
    """
    Task to query recent test results from basedosdados.

    Returns:
    - pd.DataFrame: A pandas DataFrame containing recent test results with the following columns:
        - 'name': The name of the test.
        - 'description': The description of the test result (typically the column name).
        - 'status': Indicates whether the test passed or failed.
        - 'dataset_id': The ID of the dataset containing the tested table.
        - 'table_id': The ID of the tested table.
    """

    billing_project_id = get_billing_project_id(mode="prod")
    query_bd = f"""
    with tests_order as (
    select
        test_short_name as name,
        column_name as description,
        status,
        schema_name as dataset_id,
        table_name as table_id,
        row_number() over (partition by test_short_name, schema_name, table_name order by created_at desc) as position,
        created_at
    from
        `basedosdados.elementary.elementary_test_results`
    where
        date(created_at) >= date_sub(current_date(), interval 7 DAY))
    select
        name,
        description,
        status,
        dataset_id,
        table_id
    from tests_order
    where position = 1
    """

    t = bd.read_sql(
        query=query_bd,
        billing_project_id=billing_project_id,
        from_file=True,
    )

    return t

@task
def create_update_quality_checks(tests_results: pd.DataFrame) -> None:
    """
    Task to create or update multiple quality checks based on test results asynchronously.

    Parameters:
    - tests_results (pd.DataFrame): A pandas DataFrame containing test results.

    Returns:
    - None

    """
    asyncio.run(create_update_quality_checks_async(tests_results=tests_results))