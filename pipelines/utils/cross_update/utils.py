# -*- coding: utf-8 -*-
"""
Utils for cross_update pipeline
"""
import requests


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
