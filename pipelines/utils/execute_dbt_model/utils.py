# -*- coding: utf-8 -*-
"""
General utilities for interacting with dbt-rpc
"""

import json
import os
import re
from typing import Dict

import pandas as pd
import yaml

from pipelines.utils.utils import log


def update_profiles_for_env_credentials(dbt_repository_path: str) -> None:
    """
    Update the DBT profiles.yml to use environment variables for BigQuery authentication.

    This function modifies the profiles.yml file to use environment variables
    directly for authentication instead of referencing a keyfile on disk.

    Args:
        dbt_repository_path (str): Path to the DBT repository

    Raises:
        Exception: If profiles.yml cannot be found or updated
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
    for profile_name, profile in profiles.items():
        if "outputs" in profile:
            for target_name, target in profile["outputs"].items():
                if target.get("type") == "bigquery":
                    if "keyfile" in target:
                        old_keyfile = target["keyfile"]

                        target["method"] = "oauth"

                        del target["keyfile"]

                        log(
                            f"Updated profile '{profile_name}.{target_name}' from using keyfile '{old_keyfile}' to using environment-based authentication",
                            level="info",
                        )
                        modified = True

    if modified:
        with open(profiles_path, "w") as f:
            yaml.dump(profiles, f, default_flow_style=False)
        log(
            "Profiles updated to use environment-based authentication",
            level="info",
        )
    else:
        log(
            "No BigQuery profiles found that use keyfile authentication",
            level="warning",
        )


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


def process_dbt_log_file(
    log_path: str = "dbt_repository/logs/dbt.log",
) -> pd.DataFrame:
    """
    Process the contents of a dbt log file and return a DataFrame containing the parsed log entries.
    This is the primary function for extracting log information from dbt log files.

    Args:
        log_path (str): The path to the dbt log file. Defaults to "dbt_repository/logs/dbt.log".

    Returns:
        pd.DataFrame: A DataFrame containing the parsed log entries with columns for time, level, and text.
    """

    if not os.path.exists(log_path):
        log(f"Log file not found at path: {log_path}", level="warning")
        return pd.DataFrame(columns=["time", "level", "text"])

    try:
        with open(
            log_path, "r", encoding="utf-8", errors="ignore"
        ) as log_file:
            log_content = log_file.read()

        log(f"Log file size: {len(log_content)} bytes", level="debug")

        # Try different patterns to parse the logs
        # Pattern 1: Look for timestamp followed by log level in brackets
        entries = re.findall(
            r"(\d{2}:\d{2}:\d{2}\.\d{6})\s+\[(\w+)]\s+(.*?)(?=\d{2}:\d{2}:\d{2}\.\d{6}|\Z)",
            log_content,
            re.DOTALL,
        )

        if entries:
            log(
                f"Found {len(entries)} log entries using pattern 1",
                level="debug",
            )
            return pd.DataFrame(entries, columns=["time", "level", "text"])

        # Pattern 2: Alternative pattern for ANSI colored logs
        result = re.split(r"(\x1b\[0m\d{2}:\d{2}:\d{2}\.\d{6})", log_content)
        parts = [part.strip() for part in result if part.strip()]

        if len(parts) > 1:
            log(
                f"Found {len(parts) // 2} log entries using pattern 2",
                level="debug",
            )
            splitted_log = []
            for i in range(0, len(parts) - 1, 2):
                if i + 1 >= len(parts):
                    break

                time = parts[i].replace(r"\x1b[0m", "")
                level_match = re.search(r"\[(\w+)]", parts[i + 1])
                level = level_match.group(1) if level_match else "INFO"
                text = re.sub(r"^\[\w+]\s*", "", parts[i + 1])
                splitted_log.append((time, level, text))

            return pd.DataFrame(
                splitted_log, columns=["time", "level", "text"]
            )

        # Pattern 3: Simple line-by-line parsing (fallback)
        log("Using fallback log parsing method", level="debug")
        lines = log_content.split("\n")
        entries = []

        for line in lines:
            time_match = re.search(r"^(\d{2}:\d{2}:\d{2}\.\d{6})", line)
            if time_match:
                time = time_match.group(1)
                level_match = re.search(r"\[(\w+)]", line)
                level = level_match.group(1) if level_match else "INFO"
                text = re.sub(
                    r"^\d{2}:\d{2}:\d{2}\.\d{6}\s+\[\w+]\s*", "", line
                )
                entries.append((time, level, text))

        if entries:
            log(
                f"Found {len(entries)} log entries using fallback method",
                level="debug",
            )
            return pd.DataFrame(entries, columns=["time", "level", "text"])

        log("Could not parse DBT log file with any pattern", level="warning")
        return pd.DataFrame(columns=["time", "level", "text"])

    except Exception as e:
        log(f"Error parsing DBT log file: {str(e)}", level="error")
        return pd.DataFrame(columns=["time", "level", "text"])


def extract_model_execution_status_from_logs(logs_df: pd.DataFrame) -> Dict:
    """
    Extract the execution status of each model from the logs DataFrame

    Args:
        logs_df: DataFrame containing parsed log entries

    Returns:
        Dict: Dictionary mapping model names to their execution status
    """
    model_status = {}

    model_start_pattern = r"Running model ([a-zA-Z0-9_.]+)"
    model_error_pattern = r"Compilation Error in model ([a-zA-Z0-9_.]+)"
    model_success_pattern = r"OK (model|test) ([a-zA-Z0-9_.]+)"

    for _, row in logs_df.iterrows():
        match = re.search(model_start_pattern, row["text"])
        if match:
            model_name = match.group(1)
            model_status[model_name] = "running"

        match = re.search(model_error_pattern, row["text"])
        if match:
            model_name = match.group(1)
            model_status[model_name] = "error"

        match = re.search(model_success_pattern, row["text"])
        if match:
            model_name = match.group(2) if match.group(2) else match.group(1)
            model_status[model_name] = "success"

        if "FAIL" in row["text"].upper():
            for model_name in model_status.keys():
                if model_name in row["text"]:
                    model_status[model_name] = "fail"

    return model_status


def log_dbt_from_file(log_path: str = "dbt_repository/logs/dbt.log") -> Dict:
    """
    Process a dbt log file and log its contents with appropriate log levels.
    This function is designed to be the main entry point for log file parsing.

    Args:
        log_path (str): Path to the dbt log file

    Returns:
        Dict: Summary statistics about the logs (error count, warning count, etc.)
    """
    logs_df = process_dbt_log_file(log_path)

    if logs_df.empty:
        log("No log entries found in DBT log file", level="warning")
        return {"error_count": 0, "warning_count": 0, "success": False}

    error_count = 0
    warning_count = 0

    error_logs = logs_df[
        logs_df.level.str.upper().isin(["ERROR", "CRITICAL", "FATAL"])
    ]
    for _, row in error_logs.iterrows():
        log(f"DBT ERROR [{row['time']}]: {row['text']}", level="error")
        error_count += 1

    warning_logs = logs_df[logs_df.level.str.upper().isin(["WARNING", "WARN"])]
    for _, row in warning_logs.iterrows():
        log(f"DBT WARNING [{row['time']}]: {row['text']}", level="warning")
        warning_count += 1

    important_keywords = [
        "success",
        "fail",
        "error",
        "complete",
        "finished",
        "started",
        "running",
        "model",
    ]
    for _, row in logs_df[logs_df.level.str.upper() == "INFO"].iterrows():
        if any(
            keyword in row["text"].lower() for keyword in important_keywords
        ):
            log(f"DBT INFO [{row['time']}]: {row['text']}", level="info")

    success = error_count == 0

    if error_count > 0:
        log(
            f"DBT execution completed with {error_count} errors and {warning_count} warnings",
            level="error",
        )
    elif warning_count > 0:
        log(
            f"DBT execution completed with {warning_count} warnings",
            level="warning",
        )
    else:
        log(
            "DBT execution completed successfully with no errors or warnings",
            level="info",
        )

    return {
        "error_count": error_count,
        "warning_count": warning_count,
        "success": success,
    }


def parse_and_log_dbt_runner_result(dbt_runner_result):
    """
    Process dbtRunnerResult object, parse logs, log them, and return execution status
    Focuses on runtime logging of success, fail, or warn states

    Args:
        dbt_runner_result: A dbtRunnerResult object from dbt execution

    Returns:
        bool: True if execution was successful, False otherwise
    """

    if (
        not hasattr(dbt_runner_result, "success")
        or not dbt_runner_result.success
    ):
        log("DBT execution failed", level="error")
        success = False
    else:
        log("DBT execution completed", level="info")
        success = True

    has_errors = False
    has_warnings = False

    if hasattr(dbt_runner_result, "result") and hasattr(
        dbt_runner_result.result, "logs"
    ):
        error_messages = []
        warning_messages = []

        for log_entry in dbt_runner_result.result.logs:
            level_name = log_entry.get("levelname", "INFO").upper()
            message = log_entry.get("message", "")

            if not message:
                continue

            if level_name in ("ERROR", "CRITICAL", "FATAL"):
                error_messages.append(message)
                has_errors = True
            elif level_name in ("WARNING", "WARN"):
                warning_messages.append(message)
                has_warnings = True

        for msg in error_messages:
            log(f"DBT ERROR: {msg}", level="error")

        for msg in warning_messages:
            log(f"DBT WARNING: {msg}", level="warning")

    if has_errors:
        log(
            f"DBT execution completed with {len(error_messages)} errors",
            level="error",
        )
        success = False
    elif has_warnings:
        log(
            f"DBT execution completed with {len(warning_messages)} warnings",
            level="warning",
        )
    else:
        log("DBT execution completed successfully", level="info")

    return success
