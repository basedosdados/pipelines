# -*- coding: utf-8 -*-
"""
Constants for utils.
"""

from enum import Enum


class constants(Enum):  # pylint: disable=c0103
    """
    Constants for utils.
    """

    FLOW_EXECUTE_DBT_MODEL_NAME = "EMD: template - Executa DBT model"
    FLOW_DUMP_DB_NAME = "EMD: template - Ingerir tabela de banco SQL"
    FLOW_DUMP_BASEDOSDADOS_NAME = "EMD: template - Ingerir tabela do basedosdados"
