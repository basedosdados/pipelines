# -*- coding: utf-8 -*-
"""
Tasks related to the traceroute flow.
"""

import subprocess
from typing import List, Tuple, Union

from prefect import task
import requests

from pipelines.utils.traceroute.utils import (
    get_ip_location,
    traceroute,
)
from pipelines.utils.utils import log


@task
def log_traceroute(hostname: str):
    """
    Log the traceroute.
    """
    log(f"Traceroute for {hostname}:")
    ip_list = traceroute(hostname)
    ip, country, city, lat, lon = get_ip_location()
    log(
        f"My information: IP: {ip}, Country: {country}, City: {city}, Lat: {lat}, Lon: {lon}"
    )
    route_locations = get_ip_location(ip_list)
    log("Route information:")
    for ip, country, city, lat, lon in route_locations:
        log(f"IP: {ip}, Country: {country}, City: {city}, Lat: {lat}, Lon: {lon}")
