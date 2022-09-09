# -*- coding: utf-8 -*-
"""
Tasks for cross update of metadata.
"""
# pylint: disable=invalid-name
from typing import List, Dict
import datetime
from datetime import timedelta

import basedosdados as bd
from prefect import task
import ruamel.yaml as ryaml


from pipelines.utils.cross_update.utils import _safe_fetch
from pipelines.utils.utils import log


@task
def crawler_datasets() -> Dict:
    """
    This task uses `bd_dataset_search` website API
    enpoint to retrieve a list of tables updated in last 7 days.
    Args:
        mode (str): prod or staging
    Returns:
        list of lists with dataset_id and table_id
    """
    # first request is made separately since we need to know the number of pages before the iteration
    page_size = 100
    url = f"https://basedosdados.org/api/3/action/bd_dataset_search?q=&resource_type=bdm_table&page=1&page_size={page_size}"  # pylint: disable=C0301
    response = _safe_fetch(url)
    json_response = response.json()

    return json_response


@task
def last_updated_tables(json_response) -> List[Dict[str, str]]:
    """
    Generate a dict from BD's API response with dataset_id and description as keys
    """
    n_datasets = json_response["result"]["count"]
    n_resources = [
        json_response["result"]["datasets"][k]["num_resources"]
        for k in range(n_datasets)
    ]
    datasets = json_response["result"]["datasets"]
    bdm_tables = []

    for i in range(n_datasets):
        for j in range(n_resources[i]):
            if datasets[i]["resources"][j]["resource_type"] == "bdm_table":
                bdm_tables.append(datasets[i]["resources"][j])
    result = []
    for bdm_table in bdm_tables:
        tmp_dict = {
            "dataset_id": bdm_table["dataset_id"],
            "table_id": bdm_table["table_id"],
            "last_updated": bdm_table["metadata_modified"],
        }
        result.append(tmp_dict)

    # filter list of dictionaries by last_updated and remove older than 7 days
    today = datetime.datetime.today()
    seven_days_ago = today - timedelta(days=1)
    result = [
        x
        for x in result
        if datetime.datetime.strptime(x["last_updated"], "%Y-%m-%dT%H:%M:%S.%f")
        > seven_days_ago
    ]

    # remove last_updated key
    for x in result:
        del x["last_updated"]

    return result


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

    return {"dataset_id": dataset_id, "table_id": table_id, "n_rows": n_rows}


@task
def update_nrows(dataset_id: str, table_id: str, nrows: int) -> None:
    """
    Update metadata for a selected table
    dataset_id: dataset_id,
    table_id: table_id,
    fields_to_update: list of dictionaries with key and values to be updated
    """
    fields_to_update = [{"number_rows": nrows}]

    # add credentials to config.toml
    handle = bd.Metadata(dataset_id=dataset_id, table_id=table_id)
    handle.create(if_exists="replace")

    yaml = ryaml.YAML()
    yaml.preserve_quotes = True
    yaml.indent(mapping=4, sequence=6, offset=4)

    config_file = handle.filepath.as_posix()

    with open(config_file, encoding="utf-8") as fp:
        data = yaml.load(fp)

    for field in fields_to_update:
        for k, v in field.items():
            if isinstance(v, dict):
                for i, j in v.items():
                    data[k][i] = j
            else:
                data[k] = v

    with open(config_file, "w", encoding="utf-8") as fp:
        yaml.dump(data, fp)

    if handle.validate():
        handle.publish(if_exists="replace")
        log(f"Metadata for {table_id} updated")
    else:
        log("Fail to validate metadata.")
