# -*- coding: utf-8 -*-
"""
utils for br_bd_metadados_external_link_status
"""
import requests

# ? doesnt need to import prefect when running in cloud
# import prefect


# def log(msg: str) -> None:
#     """
#     Logs message as a Task
#     """
#     logger = prefect.context.logger.info(f'\n{msg}')


# (
#    max_retries=constants.TASK_MAX_RETRIES.value,
#    retry_delay=timedelta(seconds=constants.TASK_RETRY_DELAY.value),
# )
def make_request(url: str) -> int:
    """makes a request to the given URL

    Args:
        url (str): a URL

    Returns:
        int: a URL status code
    """
    r = requests.get(url, timeout=15).status_code
    # print(type(r))
    return r
