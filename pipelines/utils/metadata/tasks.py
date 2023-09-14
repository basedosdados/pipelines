# -*- coding: utf-8 -*-
"""
Tasks for metadata
"""

from prefect import task
from datetime import datetime
from pipelines.utils.utils import log, get_credentials_from_secret
from typing import Tuple
from pipelines.utils.metadata.utils import (
    get_ids,
    parse_temporal_coverage,
    get_credentials_utils,
    create_update,
    extract_last_update,
    extract_last_date,
    get_first_date,
    get_id,
)
from dateutil.relativedelta import relativedelta


@task
def get_today_date():
    d = datetime.today()

    return d.strftime("%Y-%m-%d")


@task  # noqa
def update_django_metadata(
    dataset_id: str,
    table_id: str,
    metadata_type: str,
    _last_date: str = None,
    date_format: str = "yy-mm-dd",
    bq_last_update: bool = True,
    bq_table_last_year_month: bool = False,
    api_mode: str = "prod",
    billing_project_id: str = "basedosdados-dev",
    is_bd_pro: bool = False,
    is_free: bool = False,
    time_delta: int = 1,
    time_unit: str = "days",
):
    """
    Updates Django metadata. Version 1.2.

    Args:
        -   `dataset_id (str):` The ID of the dataset.
        -   `table_id (str):` The ID of the table.
        -   `metadata_type (str):` The type of metadata to update.
        -   `_last_date (str):` The last date for metadata update if `bq_last_update` is False. Defaults to None.
        -   `date_format (str, optional):` The date format to use when parsing dates.Defaults to 'yy-mm-dd'. Accepted values
            are 'yy-mm-dd', 'yy-mm' or 'dd'.
        -   `bq_last_update (bool, optional):` Flag indicating whether to use BigQuery's last update date for metadata.
            If True, `_last_date` is ignored. Defaults to True.
        -   `api_mode (str, optional):` The API mode to be used ('prod', 'staging'). Defaults to 'prod'.
        -   `billing_project_id (str):` the billing_project_id to be used when the extract_last_update function is triggered. Note that it has
        to be equal to the prefect agent. For prod agents use basedosdados where as for dev agents use basedosdados-dev. The default value is
        to 'basedosdados-dev'.
        -   `bq_table_last_year_month (bool):` If true extract YYYY-MM from the table in Big Query to update the Coverage. Note
        that in needs the table to have ano and mes columns.
        -   `is_bd_pro (bool):` If true updates the closed DateTimeRange metadata.
        -   `is_bd_free (bool):` If true updates the open DateTimeRange metadata.
        -   `time_delta (int):` Indicates the integer number of lags between the DateTimeRange of the closed table and the open table.
        -   `time_unit (str):` Time unit of the lag, which can be "months", "years", "days" or "weeks".

    Example:

        Eg 1. In this example, the function will search for example_table in the basedosdata project.
        It will look for a column name `data` in date_format = 'yyyy-mm-dd' in BQ and retrieve its maximum value. This table is BD Pro and also free, so the `is_free` and `is_bd_pro` arguments are set to `True`. In addition, the lag between the time coverage of the BD Pro observations and the free observations is 2 months, so the `time_delta` and `time_unit` parameters are equal to 3 and "months" respectively.

        ```
        update_django_metadata(
                dataset_id = 'example_dataset',
                table_id = 'example_table,
                metadata_type="DateTimeRange",
                bq_last_update=False,
                bq_table_last_year_month=True,
                billing_project_id="basedosdados",
                api_mode="prod",
                date_format="yy-mm-dd",
                is_bd_pro = True,
                is_free = True,
                time_delta = 3,
                time_unit = "months",
                upstream_tasks=[wait_for_materialization],
            )
        ```
    Returns:
        -   None

    Raises:
        -   Exception: If the metadata_type is not supported.
        -   Exception: If the billing_project_id is not supported.

    """
    accepted_billing_project_id = [
        "basedosdados-dev",
        "basedosdados",
        "basedosdados-staging",
    ]
    unidades_permitidas = {
        "years": "years",
        "months": "months",
        "weeks": "weeks",
        "days": "days",
    }
    if not isinstance(_last_date, str):
        raise ValueError("O parâmetro `last_date` deve ser do tipo string")

    if time_unit not in unidades_permitidas:
        raise ValueError(
            f"Unidade temporal inválida. Escolha entre {', '.join(unidades_permitidas.keys())}"
        )

    if not isinstance(time_delta, int) or time_delta <= 0:
        raise ValueError("Defasagem deve ser um número inteiro positivo")

    if billing_project_id not in accepted_billing_project_id:
        raise Exception(
            f"The given billing_project_id: {billing_project_id} is invalid. The accepted valuesare {accepted_billing_project_id}"
        )

    (email, password) = get_credentials_utils(secret_path=f"api_user_{api_mode}")

    ids = get_ids(
        dataset_id,
        table_id,
        email,
        is_bd_pro,
        password,
        is_free,
        api_mode,
    )
    log(f"IDS:{ids}")

    if metadata_type == "DateTimeRange":
        if bq_last_update:
            if is_free and not is_bd_pro:
                log(
                    f"Attention! bq_last_update was set to TRUE, it will update the temporal coverage according to the metadata of the last modification made to {table_id}.{dataset_id}"
                )
                last_date = extract_last_update(
                    dataset_id,
                    table_id,
                    date_format,
                    billing_project_id=billing_project_id,
                )

                resource_to_temporal_coverage = parse_temporal_coverage(f"{last_date}")
                resource_to_temporal_coverage["coverage"] = ids.get("coverage_id")
                log(f"Mutation parameters: {resource_to_temporal_coverage}")

                create_update(
                    query_class="allDatetimerange",
                    query_parameters={"$coverage_Id: ID": ids.get("coverage_id")},
                    mutation_class="CreateUpdateDateTimeRange",
                    mutation_parameters=resource_to_temporal_coverage,
                    update=True,
                    email=email,
                    password=password,
                    api_mode=api_mode,
                )
            elif is_bd_pro and is_free:
                last_date = extract_last_update(
                    dataset_id,
                    table_id,
                    date_format,
                    billing_project_id=billing_project_id,
                )

                delta_kwargs = {unidades_permitidas[time_unit]: time_delta}
                delta = relativedelta(**delta_kwargs)
                resource_to_temporal_coverage = parse_temporal_coverage(f"{last_date}")
                if date_format == "yy-mm-dd":
                    free_data = datetime.strptime(last_date, "%Y-%m-%d") - delta
                    free_data = free_data.strftime("%Y-%m-%d")
                    resource_to_temporal_coverage_free = parse_temporal_coverage(
                        f"{free_data}"
                    )
                    resource_to_temporal_coverage[
                        "startYear"
                    ] = resource_to_temporal_coverage_free["endYear"]
                    resource_to_temporal_coverage[
                        "startMonth"
                    ] = resource_to_temporal_coverage_free["endMonth"]
                    resource_to_temporal_coverage[
                        "startDay"
                    ] = resource_to_temporal_coverage_free["endDay"]

                elif date_format == "yy-mm":
                    free_data = datetime.strptime(last_date, "%Y-%m-%d") - delta
                    free_data = free_data.strftime("%Y-%m")
                    resource_to_temporal_coverage_free = parse_temporal_coverage(
                        f"{free_data}"
                    )
                    resource_to_temporal_coverage[
                        "startYear"
                    ] = resource_to_temporal_coverage_free["endYear"]
                    resource_to_temporal_coverage[
                        "startMonth"
                    ] = resource_to_temporal_coverage_free["endMonth"]
                elif date_format == "yy":
                    free_data = datetime.strptime(last_date, "%Y") - delta
                    free_data = free_data.strftime("%Y")
                    resource_to_temporal_coverage_free = parse_temporal_coverage(
                        f"{free_data}"
                    )
                    resource_to_temporal_coverage[
                        "startYear"
                    ] = resource_to_temporal_coverage_free["endYear"]

                log(
                    f"Cobertura PRO ->> {_last_date} || Cobertura Grátis ->> {free_data}"
                )
                # resource_to_temporal_coverage = parse_temporal_coverage(f"{last_date}")

                resource_to_temporal_coverage["coverage"] = ids.get("coverage_id_pro")
                log(f"Mutation parameters: {resource_to_temporal_coverage}")

                create_update(
                    query_class="allDatetimerange",
                    query_parameters={"$coverage_Id: ID": ids.get("coverage_id_pro")},
                    mutation_class="CreateUpdateDateTimeRange",
                    mutation_parameters=resource_to_temporal_coverage,
                    update=True,
                    email=email,
                    password=password,
                    api_mode=api_mode,
                )
                # resource_to_temporal_coverage = parse_temporal_coverage(f"{free_data}")

                resource_to_temporal_coverage_free["coverage"] = ids.get("coverage_id")
                log(f"Mutation parameters: {resource_to_temporal_coverage}")

                create_update(
                    query_class="allDatetimerange",
                    query_parameters={"$coverage_Id: ID": ids.get("coverage_id")},
                    mutation_class="CreateUpdateDateTimeRange",
                    mutation_parameters=resource_to_temporal_coverage_free,
                    update=True,
                    email=email,
                    password=password,
                    api_mode=api_mode,
                )
            elif is_bd_pro and not is_free:
                last_date = extract_last_update(
                    dataset_id,
                    table_id,
                    date_format,
                    billing_project_id=billing_project_id,
                )
                log(f"Cobertura PRO ->> {_last_date}")
                resource_to_temporal_coverage = parse_temporal_coverage(f"{last_date}")

                resource_to_temporal_coverage["coverage"] = ids.get("coverage_id_pro")
                log(f"Mutation parameters: {resource_to_temporal_coverage}")

                create_update(
                    query_class="allDatetimerange",
                    query_parameters={"$coverage_Id: ID": ids.get("coverage_id_pro")},
                    mutation_class="CreateUpdateDateTimeRange",
                    mutation_parameters=resource_to_temporal_coverage,
                    update=True,
                    email=email,
                    password=password,
                    api_mode=api_mode,
                )
        elif bq_table_last_year_month:
            if is_free and not is_bd_pro:
                log(
                    f"Attention! bq_last_update was set to TRUE, it will update the temporal coverage according to the metadata of the last modification made to {table_id}.{dataset_id}"
                )
                last_date = extract_last_date(
                    dataset_id,
                    table_id,
                    date_format,
                    billing_project_id=billing_project_id,
                )

                resource_to_temporal_coverage = parse_temporal_coverage(f"{last_date}")
                resource_to_temporal_coverage["coverage"] = ids.get("coverage_id")
                log(f"Mutation parameters: {resource_to_temporal_coverage}")

                create_update(
                    query_class="allDatetimerange",
                    query_parameters={"$coverage_Id: ID": ids.get("coverage_id")},
                    mutation_class="CreateUpdateDateTimeRange",
                    mutation_parameters=resource_to_temporal_coverage,
                    update=True,
                    email=email,
                    password=password,
                    api_mode=api_mode,
                )
            elif is_bd_pro and is_free:
                last_date = extract_last_date(
                    dataset_id,
                    table_id,
                    date_format,
                    billing_project_id=billing_project_id,
                )

                delta_kwargs = {unidades_permitidas[time_unit]: time_delta}
                delta = relativedelta(**delta_kwargs)
                resource_to_temporal_coverage = parse_temporal_coverage(f"{last_date}")
                if date_format == "yy-mm-dd":
                    free_data = datetime.strptime(last_date, "%Y-%m-%d") - delta
                    free_data = free_data.strftime("%Y-%m-%d")
                    resource_to_temporal_coverage_free = parse_temporal_coverage(
                        f"{free_data}"
                    )
                    resource_to_temporal_coverage[
                        "startYear"
                    ] = resource_to_temporal_coverage_free["endYear"]
                    resource_to_temporal_coverage[
                        "startMonth"
                    ] = resource_to_temporal_coverage_free["endMonth"]
                    resource_to_temporal_coverage[
                        "startDay"
                    ] = resource_to_temporal_coverage_free["endDay"]

                elif date_format == "yy-mm":
                    free_data = datetime.strptime(last_date, "%Y-%m") - delta
                    free_data = free_data.strftime("%Y-%m")
                    resource_to_temporal_coverage_free = parse_temporal_coverage(
                        f"{free_data}"
                    )
                    resource_to_temporal_coverage[
                        "startYear"
                    ] = resource_to_temporal_coverage_free["endYear"]
                    resource_to_temporal_coverage[
                        "startMonth"
                    ] = resource_to_temporal_coverage_free["endMonth"]
                elif date_format == "yy":
                    free_data = datetime.strptime(last_date, "%Y") - delta
                    free_data = free_data.strftime("%Y")
                    resource_to_temporal_coverage_free = parse_temporal_coverage(
                        f"{free_data}"
                    )
                    resource_to_temporal_coverage[
                        "startYear"
                    ] = resource_to_temporal_coverage_free["endYear"]

                log(
                    f"Cobertura PRO ->> {_last_date} || Cobertura Grátis ->> {free_data}"
                )
                # resource_to_temporal_coverage = parse_temporal_coverage(f"{last_date}")

                resource_to_temporal_coverage["coverage"] = ids.get("coverage_id_pro")
                log(f"Mutation parameters: {resource_to_temporal_coverage}")

                create_update(
                    query_class="allDatetimerange",
                    query_parameters={"$coverage_Id: ID": ids.get("coverage_id_pro")},
                    mutation_class="CreateUpdateDateTimeRange",
                    mutation_parameters=resource_to_temporal_coverage,
                    update=True,
                    email=email,
                    password=password,
                    api_mode=api_mode,
                )
                # resource_to_temporal_coverage = parse_temporal_coverage(f"{free_data}")

                resource_to_temporal_coverage_free["coverage"] = ids.get("coverage_id")
                log(f"Mutation parameters: {resource_to_temporal_coverage}")

                create_update(
                    query_class="allDatetimerange",
                    query_parameters={"$coverage_Id: ID": ids.get("coverage_id")},
                    mutation_class="CreateUpdateDateTimeRange",
                    mutation_parameters=resource_to_temporal_coverage_free,
                    update=True,
                    email=email,
                    password=password,
                    api_mode=api_mode,
                )
            elif is_bd_pro and not is_free:
                last_date = extract_last_date(
                    dataset_id,
                    table_id,
                    date_format,
                    billing_project_id=billing_project_id,
                )
                log(f"Cobertura PRO ->> {_last_date}")
                resource_to_temporal_coverage = parse_temporal_coverage(f"{last_date}")

                resource_to_temporal_coverage["coverage"] = ids.get("coverage_id_pro")
                log(f"Mutation parameters: {resource_to_temporal_coverage}")

                create_update(
                    query_class="allDatetimerange",
                    query_parameters={"$coverage_Id: ID": ids.get("coverage_id_pro")},
                    mutation_class="CreateUpdateDateTimeRange",
                    mutation_parameters=resource_to_temporal_coverage,
                    update=True,
                    email=email,
                    password=password,
                    api_mode=api_mode,
                )
        else:
            if is_free and not is_bd_pro:
                last_date = _last_date
                resource_to_temporal_coverage = parse_temporal_coverage(f"{_last_date}")
                log(f"Cobertura Grátis ->> {_last_date}")
                resource_to_temporal_coverage["coverage"] = ids.get("coverage_id")
                log(f"Mutation parameters: {resource_to_temporal_coverage}")

                create_update(
                    query_class="allDatetimerange",
                    query_parameters={"$coverage_Id: ID": ids.get("coverage_id")},
                    mutation_class="CreateUpdateDateTimeRange",
                    mutation_parameters=resource_to_temporal_coverage,
                    update=True,
                    email=email,
                    password=password,
                    api_mode=api_mode,
                )
            elif is_bd_pro and is_free:
                last_date = _last_date

                delta_kwargs = {unidades_permitidas[time_unit]: time_delta}
                delta = relativedelta(**delta_kwargs)

                free_data = datetime.strptime(last_date, "%Y-%m-%d") - delta
                free_data = free_data.strftime("%Y-%m-%d")

                log(
                    f"Cobertura PRO ->> {_last_date} || Cobertura Grátis ->> {free_data}"
                )
                resource_to_temporal_coverage = parse_temporal_coverage(f"{last_date}")
                resource_to_temporal_coverage_free = parse_temporal_coverage(
                    f"{free_data}"
                )

                resource_to_temporal_coverage["coverage"] = ids.get("coverage_id_pro")
                resource_to_temporal_coverage[
                    "startYear"
                ] = resource_to_temporal_coverage_free["endYear"]
                resource_to_temporal_coverage[
                    "startMonth"
                ] = resource_to_temporal_coverage_free["endMonth"]
                resource_to_temporal_coverage[
                    "startDay"
                ] = resource_to_temporal_coverage_free["endDay"]

                log(f"Mutation parameters: {resource_to_temporal_coverage}")

                create_update(
                    query_class="allDatetimerange",
                    query_parameters={"$coverage_Id: ID": ids.get("coverage_id_pro")},
                    mutation_class="CreateUpdateDateTimeRange",
                    mutation_parameters=resource_to_temporal_coverage,
                    update=True,
                    email=email,
                    password=password,
                    api_mode=api_mode,
                )
                # resource_to_temporal_coverage = parse_temporal_coverage(f"{free_data}")

                # resource_to_temporal_coverage_free["coverage"] = ids.get("coverage_id")
                log(f"Mutation parameters: {resource_to_temporal_coverage_free}")

                create_update(
                    query_class="allDatetimerange",
                    query_parameters={"$coverage_Id: ID": ids.get("coverage_id")},
                    mutation_class="CreateUpdateDateTimeRange",
                    mutation_parameters=resource_to_temporal_coverage_free,
                    update=True,
                    email=email,
                    password=password,
                    api_mode=api_mode,
                )
            elif is_bd_pro and not is_free:
                last_date = _last_date
                log(f"Cobertura PRO ->> {_last_date}")
                resource_to_temporal_coverage = parse_temporal_coverage(f"{last_date}")

                resource_to_temporal_coverage["coverage"] = ids.get("coverage_id_pro")
                log(f"Mutation parameters: {resource_to_temporal_coverage}")

                create_update(
                    query_class="allDatetimerange",
                    query_parameters={"$coverage_Id: ID": ids.get("coverage_id_pro")},
                    mutation_class="CreateUpdateDateTimeRange",
                    mutation_parameters=resource_to_temporal_coverage,
                    update=True,
                    email=email,
                    password=password,
                    api_mode=api_mode,
                )


@task
def test_ids(dataset_id, table_id, api_mode="staging", is_bd_pro=True, is_free=False):
    (email, password) = get_credentials_utils(secret_path=f"api_user_{api_mode}")
    log(email)
    log(password)

    ids = get_ids(
        dataset_id,
        table_id,
        email,
        is_bd_pro,
        password,
        is_free,
        api_mode,
    )

    log(f"ids ->> ->> {ids}")
