# -*- coding: utf-8 -*-
"""
Utilities related to the traceroute flow.
"""

import subprocess
from typing import List, Tuple, Union

import requests


def get_ip_location(
    ip_address: Union[List[str], str] = None
) -> Union[
    List[Tuple[str, str, str, float, float]], Tuple[str, str, str, float, float]
]:
    """
    Get the location of an IP address.
    """

    def parse_data(data: dict) -> Tuple[str, str, str, float, float]:
        return (
            data.get("query", ""),
            data.get("country", "Unknown"),
            data.get("city", "Unknown"),
            data.get("lat", 0.0),
            data.get("lon", 0.0),
        )

    # If it's a list of length > 1
    if isinstance(ip_address, list) and len(ip_address) > 1:
        url = "http://ip-api.com/batch"
        data = [{"query": ip} for ip in ip_address]
        response = requests.post(url, json=data)
        data = response.json()
        return [parse_data(d) for d in data]
    # If it's a list of length 1, a string or None
    elif (
        isinstance(ip_address, list)
        and len(ip_address) == 1
        or isinstance(ip_address, str)
        or ip_address is None
    ):
        url = "http://ip-api.com/json/"
        if ip_address:
            url = f"{url}{ip_address}"
        response = requests.get(url)
        data = response.json()
        try:
            ip, country, city, lat, lon = parse_data(data)
        except KeyError:
            raise KeyError(f"Error while parsing data: {data}")
        return ip, country, city, lat, lon
    else:
        raise ValueError(f"Invalid ip_address: {ip_address}")


def traceroute(hostname: str):
    """
    Launches traceroute command and parses results.
    """
    traceroute = subprocess.Popen(
        ["traceroute", hostname], stdout=subprocess.PIPE, stderr=subprocess.STDOUT
    )
    ipList = []
    for line in iter(traceroute.stdout.readline, b""):
        line = line.decode("UTF-8")
        IP = line.split("  ")
        if len(IP) > 1:
            IP = IP[1].split("(")
            if len(IP) > 1:
                IP = IP[1].split(")")
                ipList.append(IP[0])
    return ipList
