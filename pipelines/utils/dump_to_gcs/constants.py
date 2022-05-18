# -*- coding: utf-8 -*-
"""
Constants for Dump to GCS pipeline.
"""

from enum import Enum


class constants(Enum):  # pylint: disable=c0103
    """
    Constants for utils.
    """

    MAX_BYTES_PROCESSED_PER_TABLE = 1 * 1024 * 1024 * 1024  # 1GB
