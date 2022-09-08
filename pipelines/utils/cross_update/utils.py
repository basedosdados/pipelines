'''
Utils for cross_update pipeline
'''

def _safe_fetch(url: str):
    """
    Safely fetchs urls and, if somehting goes wrong, informs user what is the possible cause
    """
    response = None
    try:
        response = requests.get(url)
        response.raise_for_status()
    except requests.exceptions.HTTPError as errh:
        print("Http Error:", errh)
    except requests.exceptions.ConnectionError as errc:
        print("Error Connecting:", errc)
    except requests.exceptions.Timeout as errt:
        print("Timeout Error:", errt)
    except requests.exceptions.RequestException as err:
        print("This url doesn't appear to exists:", err)

    return response


def _dict_from_page(json_response):
    """
    Generate a dict from BD's API response with dataset_id and description as keys
    """
    temp_dict = {
        "dataset_id": [
            dataset["name"] for dataset in json_response["result"]["datasets"]
        ],
        "description": [
            dataset["notes"] if "notes" in dataset.keys() else None
            for dataset in json_response["result"]["datasets"]
        ],
    }

    return temp_dict