# -*- coding: utf-8 -*-
import requests
import pandas as pd
from datetime import datetime
from collections import OrderedDict

def get_dataset_name(organization_id: str, dataset_id: str, header: dict[str, str]) -> str | None:
    """
    Retrieve the name of a dataset given its organization ID and dataset ID.

    Parameters:
    - organization_id (str): The ID of the organization whose datasets are to be fetched.
    - dataset_id (str): The ID of the dataset whose name is to be retrieved.
    - header (dict[str, str]): A dictionary containing HTTP headers to be sent with the request.
                                This typically includes authentication tokens.

    Returns:
    - str | None: The name of the dataset if found, otherwise None.
    """
    response_list = []
    page_number = 1
    dataset_name = None
    with requests.Session() as session:
        session.headers.update(header)
        while True:
            request_url = f"https://dados.gov.br/dados/api/publico/conjuntos-dados?isPrivado=false&idOrganizacao={organization_id}&pagina={page_number}"
            response = session.get(url=request_url)
            response.raise_for_status()
            data = response.json()
            if not data:
                break
            response_list.extend(data)
            page_number += 1
    for dataset in response_list:
        if dataset["id"] == dataset_id:
            dataset_name = dataset["nome"]
    return dataset_name

def get_resources_response(dataset_name: str) -> dict[str, any]:
    """
    Fetch resources associated with a dataset by its name.

    Parameters:
    - dataset_name (str): The name of the dataset for which resources are to be fetched.

    Returns:
    - dict[str, any]: The JSON response parsed into a dictionary. The keys are strings representing various data fields,
                       and the values can be of any type depending on the API's response structure.
    """
    request_url = f"https://dados.gov.br/api/publico/conjuntos-dados/{dataset_name}"

    response = requests.get(url=request_url)
    response.raise_for_status()
    return response.json()

def get_resources_dict_list(json_response: dict[str, any], base_name: str, preview_suffix: str) -> list[dict[str, any]]:
    """
    Extracts and organizes CSV resource information from a JSON response.

    Parameters:
    - json_response (dict[str, any]): The JSON response from which to extract resource information. Expected to contain a 'resources' key mapping to a list of resource dictionaries.
    - base_name (str): A substring that, if present in a resource's name, indicates the resource should be processed.
    - preview_suffix (str): A suffix in the resource name indicating it is a preview. Used to determine if the 'preview' field should be set to True.

    Returns:
    - list[dict[str, any]]: A list of dictionaries, each containing organized information about a resource. Each dictionary includes keys for 'year', 'preview', 'uri', and 'created_at'.
    """
    uri_list = []
    resource_info = OrderedDict()

    for resource in json_response.get("resources", []):
        if base_name in resource.get("name", "") and "CSV" in resource.get("format", "").upper():
            year_marker = resource["name"].split(base_name)[-1]
            resource_info["year"] = int(year_marker.replace(preview_suffix, ""))
            resource_info["preview"] = preview_suffix in year_marker
            resource_info["uri"] = resource.get("url", "")
            resource_info["created_at"] = resource.get("created", "")
            uri_list.append(dict(resource_info))
            resource_info.clear()

    return uri_list

def filter_entries_by_creation_date(uri_list: list[dict[str, any]], creation_threshold: datetime) -> list[dict[str, any]]:
    """
    Filters a list of resource dictionaries by comparing their creation dates against a threshold.

    Parameters:
    - uri_list (list[dict[str, any]]): A list of dictionaries, where each dictionary represents a resource. Each dictionary is expected to contain a 'created_at' key
                                       mapping to a string representing the creation datetime of the resource in a format compatible with datetime.strptime().
    - creation_threshold (datetime): A datetime object representing the threshold for filtering. Resources with a 'created_at' value later than this datetime will be included
                                     in the returned list.

    Returns:
    - list[dict[str, any]]: A filtered list of dictionaries, containing only those entries from uri_list that have a 'created_at' value later than the creation_threshold.
                             Each dictionary in the returned list represents a resource that meets the filtering criteria.

    Note:
    - The 'created_at' strings in the dictionaries must be in a format that matches the datetime format used to create the creation_threshold datetime object.
    """
    filtered_list = [entry for entry in uri_list if datetime.strptime(entry.get("created_at", ""), "%Y-%m-%dT%H:%M:%S.%f") > creation_threshold]
    return filtered_list

dataset_name = get_dataset_name(ID_ORGANIZACAO, ID_CONJUNTO, HEADER)
resources_response = get_resources_response(dataset_name)
resources_dict_list = get_resources_dict_list(resources_response, BASE_NOMENCLATURA, SUFIXO_PREVIA)
filtered_entries = filter_entries_by_creation_date(resources_dict_list, datetime(2024, 5, 10))
print(filtered_entries)
