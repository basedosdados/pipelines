import requests
import pandas as pd
from datetime import datetime
from collections import OrderedDict

ID_ORGANIZACAO          = "5b283f30-ced3-4ccc-b44a-406e8a92e1ad"
ID_CONJUNTO             = "5f121f4d-47c6-428e-8ec6-e8ec56417172"
HEADER                  = {"chave-api-dados-abertos":
                           "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJqdGkiOiI4ME8tSEloOEZRa0pzc0tIZ3hieUMtWWhuMnBiWnpXRVh5YU9fX2VFSjFBb3o5UEc0ejZESTcwWjJfamZQUmt3Z3pZMXNWYjk4T1R1RjJ1WiIsImlhdCI6MTcwNzQxODUwMH0.82UtfzBHFaUC-YZZcmYKQ1hgCmBrgda_Q7lY9-DjvhE"}
BASE_NOMENCLATURA       = "Mortalidade Geral"
SUFIXO_PREVIA           = " - prévia"

def get_last_update_timestamp(organization_id: str, dataset_id: str, header: dict[str, str]) -> str:
    """
    Fetches the last update timestamp for a specific dataset from the given organization ID.

    Args:
        organization_id (str): The organization ID to fetch data from.
        dataset_id (str): The dataset ID to identify which dataset to check for the update timestamp.
        header (dict[str, str]): A dictionary containing the request headers.

    Returns:
        str: The last update timestamp in the format "YYYY-MM-DD HH:MM:SS" if found,
             or an empty string if the dataset ID is not found or if there was an error.
    
    Raises:
        requests.exceptions.RequestException: If an error occurs during the request.
    """
    response_list = []
    page_number = 1
    last_update_timestamp = ""

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
            update_string = dataset["dataAtualizacao"]
            update_datetime = datetime.strptime(update_string, "%d/%m/%Y %H:%M:%S")
            last_update_timestamp = update_datetime.strftime("%Y-%m-%d %H:%M:%S")

    return last_update_timestamp

def get_resources_response(dataset_id: str, header: dict[str, str]) -> dict[str, any]:
    """
    Fetches resource data from the given dataset ID using the provided headers.

    Args:
        dataset_id (str): The dataset ID to fetch data from.
        header (dict[str, str]): A dictionary containing the request headers.

    Returns:
        dict[str, any]: The JSON response from the API containing the resource data.

    Raises:
        requests.exceptions.RequestException: If an error occurs during the request.
    """
    request_url = f"https://dados.gov.br/dados/api/publico/conjuntos-dados/{dataset_id}"
    
    response = requests.get(url=request_url, headers=header)
    response.raise_for_status()
    return response.json()

def get_uri_list(json_response: dict[str, any], base_name: str, preview_suffix: str) -> list[dict[str, any]]:
    """
    Extracts a list of URIs from the JSON response based on the base name and suffix.

    Args:
        json_response (dict[str, any]): The JSON response containing the resources. It is expected to have a key "recursos"
                                        which is a list of dictionaries with resource details.
        base_name (str): The base name to look for in the resource titles.
        preview_suffix (str): The suffix to identify the preview status.

    Returns:
        list[dict[str, any]]: A list of dictionaries where each dictionary contains:
                              - "year": The year extracted from the resource title.
                              - "preview": A boolean indicating if the resource title contains the preview suffix.
                              - "uri": The URI link to the resource.
    """
    uri_list = []
    resource_info = OrderedDict()

    for resource in json_response.get("recursos", []):
        if base_name in resource.get("titulo", ""):
            year_marker = resource["titulo"].split(base_name)[-1]
            resource_info["year"] = int(year_marker.replace(preview_suffix, ""))
            resource_info["preview"] = preview_suffix in year_marker
            resource_info["uri"] = resource.get("link", "")
            uri_list.append(dict(resource_info))
            resource_info.clear()

    return uri_list

def filter_uri_list_by_year(uri_list: list[dict[str, any]], year: int) -> list[dict[str, any]]:
    """
    Filters the URI list to include only entries where the year is greater than the specified year.

    Args:
        uri_list (list[dict[str, any]]): The list of URIs with year, preview status, and URI.
        year (int): The year to filter the URI list by.

    Returns:
        list[dict[str, any]]: A filtered list of dictionaries where the "year" is greater than the specified year.
    """
    filtered_list = [entry for entry in uri_list if entry.get("year", 0) > year]
    return filtered_list

res = get_resources_response(ID_CONJUNTO, HEADER)

uri_list = get_uri_list(res, BASE_NOMENCLATURA, SUFIXO_PREVIA)

filtered = filter_uri_list_by_year(uri_list, 2021)

for obj in filtered:
    print(obj)