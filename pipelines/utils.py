"""
General utilities for all pipelines.
"""

from typing import Any

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
