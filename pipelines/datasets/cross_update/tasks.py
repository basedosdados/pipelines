# -*- coding: utf-8 -*-
"""
Tasks for cross update of metadata.
"""
import datetime
import re
from datetime import timedelta

# pylint: disable=invalid-name, too-many-locals
from typing import List, Dict, Tuple

import basedosdados as bd
import ruamel.yaml as ryaml
from google.cloud import storage
from prefect import task
from tqdm import tqdm

from pipelines.datasets.cross_update.utils import _safe_fetch
from pipelines.utils.dump_to_gcs.constants import constants as dump_to_gcs_constants
from pipelines.utils.utils import log


@task
def datasearch_json(page_size: int, mode: str) -> Dict:
    """
    This task uses `bd_dataset_search` website API
    enpoint to retrieve a list of tables updated in last 7 days.
    Args:
        page_size (int): Number of tables to be retrieved in each request.
        mode (str): prod or staging
    Returns:
        dict with api response
    """
    if mode == "prod":
        url = "https://basedosdados.org/api/3/action/bd_dataset_search"
    elif mode == "dev":
        url = "https://staging.basedosdados.org/api/3/action/bd_dataset_search"
    else:
        raise ValueError("mode must be prod or dev")
    url = f"{url}?q=&resource_type=bdm_table&page=1&page_size={page_size}"  # pylint: disable=C0301
    response = _safe_fetch(url)
    json_response = response.json()

    return json_response


@task(nout=2)
def crawler_tables(
    json_response: dict, days: int = 7
) -> Tuple[List[Dict[str, str]], List[Dict[str, str]]]:
    """Generate a list of dicts like {"dataset_id": "dataset_id", "table_id": "table_id"}
    where all tables were update in the last 7 days
    """
    # the line below returns the full number of datasets, despite the page_size this approach
    # leads to an IndexError when the number of datasets is greater than page_size (in the flow)
    # n_datasets = json_response["result"]["count"]

    # changed n_datasets to the actual number of datasets, limited by page_size
    n_datasets = len(json_response["result"]["datasets"])
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

    log(f"Found {len(bdm_tables)} bdm tables")
    log(bdm_tables[0].keys())
    to_update = []
    for bdm_table in bdm_tables:
        # skip if dataset_id, table_id, last_updated, and number of rows are not in the bdm_table keys
        if not all(
            key in bdm_table.keys()
            for key in ["dataset_id", "table_id", "metadata_modified", "number_rows"]
        ):
            continue
        tmp_dict = {
            "dataset_id": bdm_table["dataset_id"],
            "table_id": bdm_table["table_id"],
            "last_updated": bdm_table["metadata_modified"],
            "number_rows": bdm_table["number_rows"],
        }
        to_update.append(tmp_dict)

    # filter list of dictionaries by last_updated and remove older than X days
    today = datetime.datetime.today()
    seven_days_ago = today - timedelta(days=days)
    to_update = [
        x
        for x in to_update
        if datetime.datetime.strptime(x["last_updated"], "%Y-%m-%dT%H:%M:%S.%f")
        > seven_days_ago
    ]

    log(f"Found {len(to_update)} tables updated in last {days} days")

    to_zip = []
    for bdm_table in to_update:
        tmp_dict = {
            "dataset_id": bdm_table["dataset_id"],
            "table_id": bdm_table["table_id"],
            "project_id": "basedosdados",
            "maximum_bytes_processed": dump_to_gcs_constants.MAX_BYTES_PROCESSED_PER_TABLE.value,
            "number_rows": bdm_table["number_rows"],
        }
        to_zip.append(tmp_dict)

    # remove tables where number of rows is None
    to_zip = [x for x in to_zip if x["number_rows"] is not None]
    # keep only tables with less than 200k lines
    to_zip = [x for x in to_zip if x["number_rows"] <= 200000]

    log(f"Found {len(to_zip)} tables to zip")
    for table in to_zip:
        log(
            f"Zipping: dataset_id: {table['dataset_id']}, table_id: {table['table_id']}"
        )

    for x in to_zip:
        del x["number_rows"]

    for x in to_update:
        del x["number_rows"]
        del x["last_updated"]

    return (to_update, to_zip)


@task
def update_nrows(table_dict: Dict[str, str], mode: str) -> None:
    """Get number of rows in a table

    Args:
        table_dict (Dict[str, str]): Dict with dataset_id and table_id
        mode (str): prod or dev
    Returns:
        None
    """
    dataset_id = table_dict["dataset_id"]
    table_id = table_dict["table_id"]
    config_map = {}
    if mode == "prod":
        config_map.update({"database": f"basedosdados.{dataset_id}.{table_id}"})
        config_map.update({"project_id": "basedosdados"})
    elif mode == "dev":
        config_map.update(
            {"database": f"basedosdados-dev.{dataset_id}_staging.{table_id}"}
        )
        config_map.update({"project_id": "basedosdados-dev"})
    else:
        raise ValueError("mode must be prod or dev")

    # ideally, we would consume the API to get the number of rows,
    # but it is not available yet.
    # See: https://stackoverflow.com/questions/69313542/num-rows-issue-for-views-in-big-query-python-library
    query = f"SELECT COUNT(*) AS n_rows FROM `{config_map['database']}`"
    n_rows = bd.read_sql(
        query=query, billing_project_id=config_map["project_id"], from_file=True
    )["n_rows"].to_list()[0]

    fields_to_update = [{"number_rows": int(n_rows)}]

    # add credentials to config.toml
    handle = bd.Metadata(dataset_id=dataset_id, table_id=table_id)
    handle.create(if_exists="replace")

    yaml = ryaml.YAML()
    yaml.preserve_quotes = True
    yaml.indent(mapping=4, sequence=6, offset=4)

    config_file = handle.filepath.as_posix()

    with open(config_file, encoding="utf-8") as fp:
        data = yaml.load(fp)

    # log(data)

    for field in fields_to_update:
        for k, v in field.items():
            if isinstance(v, dict):
                for i, j in v.items():
                    data[k][i] = j
            else:
                data[k] = v

    with open(config_file, "w", encoding="utf-8") as fp:
        yaml.dump(data, fp)

    # log(data)

    if handle.validate():
        handle.publish(if_exists="replace")
        log(f"Metadata for {table_id} updated")
    else:
        log("Fail to validate metadata.")


@task
def rename_blobs() -> None:
    """Rename blobs in basedosdados-dev bucket"""
    storage_client = storage.Client()

    bucket = storage_client.get_bucket("basedosdados-public")
    blobs = bucket.list_blobs(prefix="one-click-download/")

    for blob in tqdm(list(blobs)):
        table_id = blob.name.split("/")[-2]
        new_name = re.sub(r"data0*\.csv\.gz", table_id + ".csv.gz", blob.name)
        bucket.rename_blob(blob, new_name)
