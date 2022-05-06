# -*- coding: utf-8 -*-
"""
Constants for utils.
"""

from enum import Enum


class constants(Enum):  # pylint: disable=c0103
    """
    Constants for utils.
    """

    FLOW_EXECUTE_DBT_MODEL_NAME = "DB: template - Executa DBT model"
    FLOW_DUMP_DB_NAME = "DB: template - Ingerir tabela de banco SQL"
    FLOW_DUMP_BASEDOSDADOS_NAME = "DB: template - Ingerir tabela do basedosdados"
    FLOW_DUMP_TO_GCS_NAME = "BD: template - Ingerir tabela zipada para GCS"
