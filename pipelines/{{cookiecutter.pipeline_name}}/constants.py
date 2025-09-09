"""
Constant values for the {{cookiecutter.pipeline_name}} pipeline
"""


###############################################################################
#
# Esse é um arquivo onde podem ser declaratas constantes que serão usadas
# por todos os arquivos da pipeline {{cookiecutter.pipeline_name}}.
#
# Por ser um arquivo opcional, pode ser removido sem prejuízo ao funcionamento
# da pipeline, caos não esteja em uso.
#
# Para declarar constantes, basta fazer conforme o exemplo abaixo:
#
# ```
# class Constants(Enum):
#     """
#     Constant values for the {{cookiecutter.pipeline_name}} pipeline
#     """
#     FOO = "bar"
# ```
#
# Para usá-las, basta fazer conforme o exemplo abaixo:
#
# ```py
# from pipelines.datasets.{{cookiecutter.pipeline_name}}.constants import Constants
# print(Constants.FOO.value)
# ```
#
###############################################################################

from enum import Enum


class Constants(Enum):
    """
    Constant values for the {{cookiecutter.pipeline_name}} pipeline
    """

    FOO = "bar"
