# -*- coding: utf-8 -*-
"""
Constant values for the datasets projects
"""


###############################################################################
#
# Esse é um arquivo onde podem ser declaratas constantes que serão usadas
# pelo projeto br_fgv_igp.
#
# Por ser um arquivo opcional, pode ser removido sem prejuízo ao funcionamento
# do projeto, caos não esteja em uso.
#
# Para declarar constantes, basta fazer conforme o exemplo abaixo:
#
# ```
# class constants(Enum):
#     """
#     Constant values for the br_fgv_igp project
#     """
#     FOO = "bar"
# ```
#
# Para usá-las, basta fazer conforme o exemplo abaixo:
#
# ```py
# from pipelines.datasets.br_fgv_igp.constants import constants
# print(constants.FOO.value)
# ```
#
###############################################################################

from enum import Enum
from pathlib import Path


class constants(Enum):  # pylint: disable=c0103
    """
    Constant values for the br_fgv_igp project
    """

    FGV_INDEX = dict(
        IGPDI=("IGP12_IGPDI12", None, None),
        IGPM=("IGP12_IGPM12", "IGP12_IGPMG1D12", "IGP12_IGPMG2D12"),
        IGPOG=("IGP12_IGPOG12", None, None),
        IGP10=("IGP12_IGP1012", None, None),
    )

    ROOT = Path("tmp/data")
