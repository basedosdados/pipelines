# -*- coding: utf-8 -*-
"""
Tasks for metadata
"""

from datetime import datetime

from dateutil.relativedelta import relativedelta
from prefect import task

from pipelines.utils.metadata.utils import (
    check_if_values_are_accepted,
    create_update,
    extract_last_date_from_bq,
    get_api_most_recent_date,
    get_credentials_utils,
    get_ids,
    parse_temporal_coverage,
    sync_bdpro_free_coverages,
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
    date_column_name: str,
    date_format: str = "%Y-%m-%d",
    coverage_status: str = "all_bdpro",
    time_delta: int = 1,
    time_unit: str = "days",
    billing_project_id: str = "basedosdados-dev",
    api_mode = 'prod'
):
    """
    Updates Django metadata. Version 1.2.

    Args:

    Returns:
        -   None

    Raises:
        -   Exception: If the metadata_type is not supported.
        -   Exception: If the billing_project_id is not supported.

    """
    
    check_if_values_are_accepted(coverage_status = coverage_status,
                                 time_unit = time_unit,
                                 billing_project_id = billing_project_id)
    
    (email, password) = get_credentials_utils(secret_path=f"api_user_{api_mode}")

    ids = get_ids(
        dataset_id = dataset_id,
        table_id = table_id,
        coverage_status = coverage_status,
        email = email,
        password = password,
        api_mode=api_mode,
    )

    log(f"IDS:{ids}")

    last_date = extract_last_date_from_bq(
            dataset_id = dataset_id,
            table_id = table_id,
            date_format = date_format,
            date_column = date_column_name,
            billing_project_id = billing_project_id,
        )
    
    last_date_parameters = parse_temporal_coverage(f"{last_date}")
     
    if coverage_status == 'all_free':

        all_free_parameters = last_date_parameters
        all_free_parameters["coverage"] = ids.get("coverage_id_free")

        log(f"Mutation parameters: {all_free_parameters}")
        create_update(
            query_class="allDatetimerange",
            query_parameters={"$coverage_Id: ID": ids.get("coverage_id_free")},
            mutation_class="CreateUpdateDateTimeRange",
            mutation_parameters=all_free_parameters,
            update=True,
            email=email,
            password=password,
            api_mode=api_mode
        )

    elif coverage_status == 'all_bdpro':
        log(f"Cobertura PRO ->> {last_date}")
        bdpro_parameters = last_date_parameters
        bdpro_parameters["coverage"] = ids.get("coverage_id_pro")

        log(f"Mutation parameters: {bdpro_parameters}")
        create_update(
            query_class="allDatetimerange",
            query_parameters={"$coverage_Id: ID": ids.get("coverage_id_pro")},
            mutation_class="CreateUpdateDateTimeRange",
            mutation_parameters=bdpro_parameters,
            update=True,
            email=email,
            password=password,
            api_mode=api_mode,
        )

    elif coverage_status == 'partially_bdpro':
        bdpro_parameters = last_date_parameters
        bdpro_parameters["coverage"] = ids.get("coverage_id_pro")

        delta_kwargs = {time_unit: time_delta}
        delta = relativedelta(**delta_kwargs)
        free_data = datetime.strptime(last_date, date_format) - delta
        free_data = free_data.strftime(date_format)
        free_parameters = parse_temporal_coverage(f"{free_data}")
        free_parameters["coverage"] = ids.get("coverage_id_free")

        bdpro_parameters = sync_bdpro_free_coverages(date_format = date_format,
                                  bdpro_parameters = bdpro_parameters,
                                  free_parameters = free_parameters)
        
        log(
            f"Cobertura PRO ->> {last_date} || Cobertura Grátis ->> {free_data}"
        )
        
        log(f"Mutation parameters: {bdpro_parameters}")
        create_update(
            query_class="allDatetimerange",
            query_parameters={"$coverage_Id: ID": ids.get("coverage_id_pro")},
            mutation_class="CreateUpdateDateTimeRange",
            mutation_parameters=bdpro_parameters,
            update=True,
            email=email,
            password=password
        )
        
        log(f"Mutation parameters: {free_parameters}")
        create_update(
            query_class="allDatetimerange",
            query_parameters={"$coverage_Id: ID": ids.get("coverage_id_free")},
            mutation_class="CreateUpdateDateTimeRange",
            mutation_parameters=free_parameters,
            update=True,
            email=email,
            password=password,
            api_mode=api_mode,
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
    if type(data_source_max_date) == datetime:
        data_source_max_date = data_source_max_date.date()
    if type(data_source_max_date) == str:
        data_source_max_date = datetime.strptime(
            data_source_max_date, "%Y-%m-%d"
        ).date()

    # antigo parse_coverage
    data_api = get_api_most_recent_date(
        dataset_id=dataset_id, table_id=table_id, date_format=date_format
    )

    log(f"Data na fonte: {data_source_max_date}")
    log(f"Data nos metadados da BD: {data_api}")

    # Compara as datas para verificar se há atualizações
    if data_source_max_date > data_api:
        return True  # Há atualizações disponíveis
    else:
        return False
