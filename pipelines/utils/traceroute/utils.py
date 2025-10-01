"""
Utilities related to the traceroute flow.
"""

import subprocess

import requests


def get_ip_location(
    ip_address: list[str] | str | None = None,
) -> (
    list[tuple[str, str, str, float, float]]
    | tuple[str, str, str, float, float]
):
    """
    Get the location of an IP address.
    """

    def parse_data(data: dict) -> tuple[str, str, str, float, float]:
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
        response = requests.post(url, json=data, timeout=300)
        data = response.json()
        return [parse_data(d) for d in data]
    # If it's a list of length 1, a string or None
    if (
        (isinstance(ip_address, list) and len(ip_address) == 1)
        or isinstance(ip_address, str)
        or ip_address is None
    ):
        url = "http://ip-api.com/json/"
        if ip_address:
            url = f"{url}{ip_address}"
        response = requests.get(url, timeout=300)
        data = response.json()
        try:
            ip, country, city, lat, lon = parse_data(data)
        except KeyError as err:
            raise KeyError(f"Error while parsing data: {data}") from err
        return ip, country, city, lat, lon

    raise ValueError(f"Invalid ip_address: {ip_address}")


def traceroute(hostname: str):
    """
    Launches traceroute command and parses results.
    """
    traceroute = subprocess.Popen(
        ["traceroute", hostname],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
    )
    ip_list = []
    for line in iter(traceroute.stdout.readline, b""):
        line = line.decode("UTF-8")
        ip = line.split("  ")
        if len(ip) > 1:
            ip = ip[1].split("(")
            if len(ip) > 1:
                ip = ip[1].split(")")
                ip_list.append(ip[0])
    return ip_list
