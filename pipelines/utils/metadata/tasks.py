# -*- coding: utf-8 -*-
"""
Tasks for metadata
"""

from datetime import datetime

from prefect import task

from pipelines.utils.metadata.utils import (
    check_if_values_are_accepted,
    create_update,
    extract_last_date_from_bq,
    get_api_most_recent_date,
    get_billing_project_id,
    get_credentials_utils,
    get_ids,
    get_parcially_bdpro_coverage_parameters,
    get_table_status,
    parse_temporal_coverage,
)
from pipelines.utils.utils import extract_last_date, log


@task
def get_today_date():
    d = datetime.today()
    return d.strftime("%Y-%m-%d")


@task  # noqa
def update_django_metadata(
    dataset_id: str,
    table_id: str,
    date_column_name = {'year':'ano','month':'mes'},
    date_format: str = "%Y-%m",
    coverage_type: str = "partially_bdpro",
    time_delta: dict = {"months":6},
    prefect_mode: str = "dev",
    api_mode: str = "prod",
    bq_project: str = "basedosdados",
    historical_database: bool = True
):
    """
    Updates Django metadata. Version 1.3.

    Args:
        date_column_name: Pode ser uma única coluna com a data 
            ou um dicionário com as chaves 'year' e 'month' 
            para indicar as colunas correspondentes para a atualização
    Returns:
        -   None

    Raises:
        -   Exception: If the metadata_type is not supported.
        -   Exception: If the billing_project_id is not supported.

    """
    
    check_if_values_are_accepted(coverage_type = coverage_type,
                                 time_delta = time_delta,
                                 date_column_name = date_column_name)
    
    billing_project_id = get_billing_project_id(mode = prefect_mode)

    (email, password) = get_credentials_utils(secret_path=f"api_user_{api_mode}")

    ids = get_ids(
        dataset_name = dataset_id,
        table_name = table_id,
        coverage_type = coverage_type,
        email = email,
        password = password,
        api_mode=api_mode,
    )

    log(f"IDS:{ids}")

    if api_mode=='prod' and bq_project!='basedosdados':
        log("WARNING: Production API Mode with Non-Production Project Selected")

        status = get_table_status(
            table_id = ids["table_id"],
            api_mode=api_mode,
            email = email,
            password = password
            )
        if status != "under_review":
            raise ValueError("The table status should be under_review to update metadata with non-production data")

    last_date = extract_last_date_from_bq(
            dataset_id = dataset_id,
            table_id = table_id,
            date_format = date_format,
            date_column = date_column_name,
            billing_project_id = billing_project_id,
            project_id = bq_project,
            historical_database = historical_database
        )
    
     
    if coverage_type == 'all_free':

        all_free_parameters = parse_temporal_coverage(f"{last_date}",historical_database)
        all_free_parameters["coverage"] = ids.get("coverage_id_free")

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

    elif coverage_type == 'all_bdpro':
        log(f"Cobertura PRO ->> {last_date}")
        bdpro_parameters = parse_temporal_coverage(f"{last_date}",historical_database)
        bdpro_parameters["coverage"] = ids.get("coverage_id_pro")

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

    elif coverage_type == 'partially_bdpro':
        if not historical_database:
            raise ValueError("Invalid Selection: Non-historical base and partially bdpro coverage chosen, not compatible.")

        bdpro_parameters, free_parameters = get_parcially_bdpro_coverage_parameters(
            time_delta = time_delta,
            ids = ids,
            last_date = last_date,
            date_format = date_format
            )
        
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
    if type(data_source_max_date) is datetime:
        data_source_max_date = data_source_max_date.date()
    if type(data_source_max_date) is str:
        data_source_max_date = datetime.strptime(
            data_source_max_date, date_format
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