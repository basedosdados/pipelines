# -*- coding: utf-8 -*-
"""
General utilities for interacting with dbt-rpc
"""

import os
import re

import pandas as pd

from pipelines.utils.utils import log


def process_dbt_log_file(log_path: str) -> pd.DataFrame:
    """
    Process the contents of a dbt log file and return a DataFrame containing the parsed log entries.
    This is the primary function for extracting log information from dbt log files.

    Args:
        log_path (str): The path to the dbt log file.

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


def extract_model_execution_status_from_logs(logs_df: pd.DataFrame) -> dict:
    """
    Extract the execution status of each model from the logs DataFrame

    Args:
        logs_df: DataFrame containing parsed log entries

    Returns:
        dict: Dictionary mapping model names to their execution status
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


def log_dbt_from_file(log_path) -> dict:
    """
    Process a dbt log file and log its contents with appropriate log levels.
    This function is designed to be the main entry point for log file parsing.

    Args:
        log_path (str): Path to the dbt log file

    Returns:
        dict: Summary statistics about the logs (error count, warning count, etc.)
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
