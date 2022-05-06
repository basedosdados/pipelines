# -*- coding: utf-8 -*-
"""
Constants for utils.
"""

from enum import Enum


class constants(Enum):  # pylint: disable=c0103
    """
    Constants for utils.
    """

    FLOW_EXECUTE_DBT_MODEL_NAME = "BD template - Executa DBT model"
    FLOW_DUMP_TO_GCS_NAME = "BD template - Ingerir tabela zipada para GCS"
