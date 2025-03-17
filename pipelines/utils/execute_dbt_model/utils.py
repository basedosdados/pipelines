# -*- coding: utf-8 -*-
"""
General utilities for interacting with dbt-rpc
"""

import json
import os
from datetime import datetime, timedelta
from typing import List

import yaml
from dbt_client import DbtClient
from prefect.schedules.clocks import IntervalClock

from pipelines.utils.utils import log


def get_dbt_client(
    host: str = "dbt-rpc",
    port: int = 8580,
    jsonrpc_version: str = "2.0",
) -> DbtClient:
    """
    Returns a DBT RPC client.

    Args:
        host: The hostname of the DBT RPC server.
        port: The port of the DBT RPC server.
        jsonrpc_version: The JSON-RPC version to use.

    Returns:
        A DBT RPC client.
    """
    return DbtClient(
        host=host,
        port=port,
        jsonrpc_version=jsonrpc_version,
    )


def generate_execute_dbt_model_schedules(  # pylint: disable=too-many-arguments,too-many-locals
    interval: timedelta,
    start_date: datetime,
    labels: List[str],
    table_parameters: dict,
    runs_interval_minutes: int = 15,
) -> List[IntervalClock]:
    """
    Generates multiple schedules for execute dbt model.
    """
    clocks = []
    for count, (table_id, parameters) in enumerate(table_parameters.items()):
        parameter_defaults = {
            "dataset_id": parameters["dataset_id"],
            "table_id": table_id,
            "mode": parameters["mode"],
        }
        clocks.append(
            IntervalClock(
                interval=interval,
                start_date=start_date
                + timedelta(minutes=runs_interval_minutes * count),
                labels=labels,
                parameter_defaults=parameter_defaults,
            )
        )
    return clocks


def merge_vars(vars1, vars2):
    try:
        dict1 = json.loads(vars1)
        dict2 = json.dumps(vars2, ensure_ascii=False)
        dict2 = json.loads(dict2)
        merged = {**dict1, **dict2}
        return json.dumps(merged)
    except (json.JSONDecodeError, TypeError) as e:
        log(f"Erro ao mesclar variáveis: {e}")
        raise


def update_keyfile_path_in_profiles(
    dbt_repository_path: str,
    custom_keyfile_path: str,
    profile_name: str = "default",
    target_name: str = "dev",
) -> None:
    """
    Update the keyfile path in profiles.yml for a specific profile and target.

    This function searches for profiles.yml in common locations and updates
    the keyfile path only for the specified profile and target.

    *Note*: it updates the default Big Query credentials in DBT profiles.yml value to your local credentials

    Args:
        dbt_repository_path (str): Path to the dbt repository.
        custom_keyfile_path (str): New path to use for the keyfile.
        profile_name (str): The profile to update. Defaults to "default".
        target_name (str): The target environment to update. Defaults to "dev".

    Raises:
        Exception: If profiles.yml is not found or cannot be updated.
    """

    possible_profile_locations = [
        os.path.join(dbt_repository_path, "profiles.yml"),
        os.path.join(dbt_repository_path, ".dbt", "profiles.yml"),
        os.path.expanduser("~/.dbt/profiles.yml"),
    ]

    profiles_path = None
    for path in possible_profile_locations:
        if os.path.exists(path):
            profiles_path = path
            break

    if not profiles_path:
        raise Exception(
            "profiles.yml not found in any of the expected locations"
        )

    log(f"Found profiles.yml at {profiles_path}", level="info")

    with open(profiles_path, "r") as f:
        profiles = yaml.safe_load(f)

    modified = False
    if (
        profile_name in profiles
        and isinstance(profiles[profile_name], dict)
        and "outputs" in profiles[profile_name]
    ):
        outputs = profiles[profile_name]["outputs"]
        if (
            target_name in outputs
            and isinstance(outputs[target_name], dict)
            and "keyfile" in outputs[target_name]
        ):
            old_keyfile = outputs[target_name]["keyfile"]
            outputs[target_name]["keyfile"] = custom_keyfile_path
            modified = True
            log(
                f"Updated keyfile for profile '{profile_name}.{target_name}' from '{old_keyfile}' to '{custom_keyfile_path}'",
                level="info",
            )

    if modified:
        with open(profiles_path, "w") as f:
            yaml.dump(profiles, f, default_flow_style=False)
        log(
            f"Updated profiles.yml with custom keyfile path for {profile_name}.{target_name}",
            level="info",
        )
    else:
        log(
            f"Profile '{profile_name}.{target_name}' not found or doesn't have a keyfile path to update",
            level="warning",
        )
