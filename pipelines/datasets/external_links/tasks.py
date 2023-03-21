# -*- coding: utf-8 -*-

# ! import log from general utils pipelines repo
from prefect import task
import requests
import pandas as pd
import os
from datetime import datetime, timedelta

# from utils import (
#     make_request,
#     log,
# )
from pipelines.utils.utils import (
    log,
)
from pipelines.datasets.external_links.utils import (
    make_request,
)
from pipelines.constants import constants

# ? insert functions paths from utils

# ? importa logs
# pylint: disable=C0103


@task(
    max_retries=constants.TASK_MAX_RETRIES.value,
    retry_delay=timedelta(seconds=constants.TASK_RETRY_DELAY.value),
)
def crawler_external_links_status():
    """
    Pulls metadata about external links from Base dos Dados' APIs and returns it structured in a csv file.

    Args:

    Returns:
        Path to csv file with structured metadata about external links.
    """

    url = "https://basedosdados.org/api/3/action/bd_dataset_search?page_size=50000&resource_type=external_link"
    r = requests.get(url, timeout=10)
    datasets_json = r.json()["result"]["datasets"]

    resources = []

    for dataset in datasets_json:
        for resource in dataset["resources"]:
            if resource["resource_type"] == "external_link":

                resources.append(
                    {
                        "dataset_id": dataset.get("id"),
                        "id": resource.get("id"),
                        "name": resource.get("name"),
                        "date_created": resource.get("created")[0:10],
                        "date_last_modified": resource.get("metadata_modified")[0:10],
                        "url": resource.get("url"),
                    }
                )

    for dic in resources:

        external_url = dic["url"]

        try:
            request_status = make_request(external_url)
            print(f"status {request_status}")
            dic["url_status"] = request_status

        except Exception as e:
            # TODO change to url_check_status_error
            dic["url_status_error"] = e
            print(f"erro: a url {external_url} retornou o erro {e}")

    # log('dados baixados com sucesso')

    df = pd.DataFrame.from_dict(resources)

    os.system("mkdir -p /tmp/data/")
    df.to_csv("/tmp/data/external_link_status.csv", index=False)

    return "/tmp/data/external_link_status.csv"


# @task
# def save_dataframe(df: pd.DataFrame, filepath: str) -> None:
#     """
#     Saves dataframe to csv file.
#     """
#     df.to_csv(filepath, index=False)
