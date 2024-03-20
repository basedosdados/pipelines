# -*- coding: utf-8 -*-
"""
Tasks for metadata
"""
import asyncio
from datetime import datetime
import basedosdados as bd

import pandas as pd
from prefect import task

from pipelines.utils.metadata.utils import (
    check_if_values_are_accepted,
    create_update,
    extract_last_date_from_bq,
    get_api_most_recent_date,
    get_billing_project_id,
    get_coverage_parameters,
    get_credentials_utils,
    get_ids,
    get_id,
    get_table_status,
    update_row_access_policy,
    create_quality_check,
)
from pipelines.utils.utils import log
from pipelines.utils.metadata.utils_async import create_update_quality_checks_async

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

    (email, password) = get_credentials_utils(secret_path=f"api_user_{api_mode}")

    ids = get_ids(
        dataset_name=dataset_id,
        table_name=table_id,
        coverage_type=coverage_type,
        email=email,
        password=password,
        api_mode=api_mode,
    )

    log(f"IDS:{ids}")

    if api_mode == "prod" and bq_project != "basedosdados":
        log("WARNING: Production API Mode with Non-Production Project Selected")

        status = get_table_status(
            table_id=ids["table_id"], api_mode=api_mode, email=email, password=password
        )
        if status != "under_review":
            raise ValueError(
                "The table status should be under_review to update metadata with non-production data"
            )

    last_date = extract_last_date_from_bq(
        dataset_id=dataset_id,
        table_id=table_id,
        date_format=date_format,
        date_column=date_column_name,
        billing_project_id=billing_project_id,
        project_id=bq_project,
        historical_database=historical_database,
    )

    free_parameters, bdpro_parameters = get_coverage_parameters(
        coverage_type=coverage_type,
        last_date=last_date,
        time_delta=time_delta,
        ids=ids,
        date_format=date_format,
        historical_database=historical_database,
    )

    if free_parameters is not None:
        create_update(
            query_class="allDatetimerange",
            query_parameters={"$coverage_Id: ID": free_parameters["coverage"]},
            mutation_class="CreateUpdateDateTimeRange",
            mutation_parameters=free_parameters,
            update=True,
            email=email,
            password=password,
            api_mode=api_mode,
        )

    if bdpro_parameters is not None:
        create_update(
            query_class="allDatetimerange",
            query_parameters={"$coverage_Id: ID": bdpro_parameters["coverage"]},
            mutation_class="CreateUpdateDateTimeRange",
            mutation_parameters=bdpro_parameters,
            update=True,
            email=email,
            password=password,
            api_mode=api_mode,
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


@task
def check_if_data_is_outdated(
    dataset_id: str,
    table_id: str,
    data_source_max_date: datetime,
    date_format: str = "%Y-%m-%d",
) -> bool:
    """Essa task checa se há necessidade de atualizar os dados no BQ

    Args:
        dataset_id e table_id(string): permite encontrar na api a última data de cobertura
        data_api (date): A data mais recente dos dados da fonte original

    Returns:
        bool: TRUE se a data da fonte original for maior que a data mais recente registrada na API e FALSE caso contrário.
    """
    if type(data_source_max_date) is datetime:
        data_source_max_date = data_source_max_date.date()
    if type(data_source_max_date) is str:
        data_source_max_date = datetime.strptime(
            data_source_max_date, date_format
        ).date()
    if type(data_source_max_date) is pd.Timestamp:
        data_source_max_date = data_source_max_date.date()

    data_api = get_api_most_recent_date(
        dataset_id=dataset_id, table_id=table_id, date_format=date_format
    )

    log(f"Data na fonte: {data_source_max_date}")
    log(f"Data nos metadados da BD: {data_api}")

    # Compara as datas para verificar se há atualizações
    if data_source_max_date > data_api:
        log("Há atualizações disponíveis")
        return True  # Há atualizações disponíveis
    else:
        log("Não há novas atualizações disponíveis")
        return False


@task
def task_get_api_most_recent_date(dataset_id, table_id, date_format):
    return get_api_most_recent_date(
        dataset_id=dataset_id, table_id=table_id, date_format=date_format
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

    billing_project_id = get_billing_project_id(mode="dev")
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