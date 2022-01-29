"""
General utilities for all pipelines.
"""

from typing import Any, Dict

import logging
import prefect


def log(msg: Any, level: str = 'info') -> None:
    """
    Logs a message to prefect's logger.
    """
    levels = {
        'debug': logging.DEBUG,
        'info': logging.INFO,
        'warning': logging.WARNING,
        'error': logging.ERROR,
        'critical': logging.CRITICAL,
    }
    if level not in levels:
        raise ValueError(f"Invalid log level: {level}")
    prefect.context.logger.log(levels[level], msg)  # pylint: disable=E1101

@prefect.task(checkpoint=False)
def log_task(msg: Any, level: str = "info"):
    """
    Logs a message to prefect's logger.
    """
    log(msg, level)

def run_local(flow: prefect.Flow, parameters: Dict[str, Any] = None):
    """
    Runs a flow locally.
    """
    # Setup for local run
    flow.storage = None
    flow.run_config = None
    flow.schedule = None

    # Run flow
    if parameters:
        return flow.run(parameters=parameters)
    return flow.run()