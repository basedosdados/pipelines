# -*- coding: utf-8 -*-
"""
Tasks for dumping data directly from BigQuery to GCS.
"""

from typing import List, Dict
import math

import basedosdados as bd
from prefect import task

from pipelines.utils.cross_update.utils import _safe_fetch, _dict_from_page


@task
def last_updated_tables() -> List[Dict[str, str]]:
    """
    This function uses `bd_dataset_search` website API
    enpoint to retrieve a list of tables updated in last 7 days.
    Args:
        mode (str): prod or staging
    Returns:
        list of lists with dataset_id and table_id
    """
    # first request is made separately since we need to now the number of pages before the iteration
    page_size = 100  # this function will only made more than one requisition if there are more than 100 datasets in the API response #pylint: disable=C0301
    url = f"https://basedosdados.org/api/3/action/bd_dataset_search?q=&resource_type=bdm_table&page=1&page_size={page_size}"  # pylint: disable=C0301
    response = _safe_fetch(url)
    json_response = response.json()
    n_datasets = json_response["result"]["count"]
    n_pages = math.ceil(n_datasets / page_size)
    temp_dict = _dict_from_page(json_response)

    temp_dicts = [temp_dict]
    for page in range(2, n_pages + 1):
        url = f"https://basedosdados.org/api/3/action/bd_dataset_search?q=&resource_type=bdm_table&page={page}&page_size={page_size}"  # pylint: disable=C0301
        response = _safe_fetch(url)
        json_response = response.json()
        temp_dict = _dict_from_page(json_response)
        temp_dicts.append(temp_dict)

    dataset_dict = defaultdict(list)

    for d in temp_dicts:  # pylint: disable=C0103
        for key, value in d.items():
            dataset_dict[key].append(value)

    print(dataset_dict.keys())


@task
def get_nrows(dataset_id: str, table_id: str) -> List[Dict[str, str]]:
    """Get number of rows in a table

    Args:
        dataset_id (str): dataset id
        table_id (str): table id
    Returns:
        int: number of rows
    """
    config_map = {}
    config_map.update({"database": "basedosdados.{dataset_id}_staging.{table_id}"})
    config_map.update({"project_id": "basedosdados"})

    # ideally, we would consume the API to get the number of rows,
    # but it is not available yet.
    # See: https://stackoverflow.com/questions/69313542/num-rows-issue-for-views-in-big-query-python-library
    query = f"SELECT COUNT(*) AS n_rows FROM `{config_map['database']}`"
    n_rows = bd.read_sql(
        query=query, billing_project_id=config_map["project_id"], from_file=True
    )["n_rows"].to_list()[0]

    return n_rows


@tasks
def update_nrows(dataset_id: str, table_id: str, nrows: int) -> None:
    """Update number of rows in a table

    Args:
        dataset_id (str): dataset id
        table_id (str): table id
        nrows (int): number of rows
    """
