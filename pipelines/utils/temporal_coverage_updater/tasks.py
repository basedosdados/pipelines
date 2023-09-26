# -*- coding: utf-8 -*-
"""
Tasks for temporal_coverage_updater
"""


from prefect import task

# from basedosdados.upload.base import Base
import basedosdados as bd
from pipelines.utils.temporal_coverage_updater.utils import (
    find_ids,
    parse_temporal_coverage,
    get_credentials,
    create_update,
    extract_last_update,
    get_first_date,
)
from pipelines.utils.utils import log, get_credentials_from_secret


## TODO: Transformar flow em task OK
## TODO: Criar novo argumento na função update_temporal_coverage p/ selecionar o "tipo" (bool) do first date e last date OK
## TODO: migrar p/ utils.tasks
## TODO: fazer check dentro do parse se está no formato padrão da BD e avisar ao usuário quando n estiver OK
@task
def update_django_metadata(
    dataset_id: str,
    table_id: str,
    metadata_type: str,
    _last_date=None,
    bq_last_update: bool = True,
):
    """
    Updates Django metadata.

    Args:
        dataset_id (str): The ID of the dataset.
        table_id (str): The ID of the table.
        metadata_type (str): The type of metadata to update.
        _last_date (optional): The last date for metadata update if `bq_last_update` is False. Defaults to None.
        bq_last_update (bool, optional): Flag indicating whether to use BigQuery's last update date for metadata.
            If True, `_last_date` is ignored. Defaults to True.

    Returns:
        None

    Raises:
        Exception: If the metadata_type is not supported.

    """
    (email, password) = get_credentials(secret_path="api_user_prod")

    ids = find_ids(
        dataset_id,
        table_id,
        email,
        password,
    )

    if metadata_type == "DateTimeRange":
        if bq_last_update:
            last_date = extract_last_update(
                dataset_id,
                table_id,
            )
            first_date = get_first_date(
                ids,
                email,
                password,
            )

            resource_to_temporal_coverage = parse_temporal_coverage(
                f"{first_date}{last_date}"
            )
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
            )
        else:
            last_date = _last_date
            log(f"Última data {last_date}")
            first_date = get_first_date(
                ids,
                email,
                password,
            )

            resource_to_temporal_coverage = parse_temporal_coverage(
                f"{first_date}{last_date}"
            )

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
            )
