# -*- coding: utf-8 -*-
"""
Constant values for the datasets projects
"""

from enum import Enum


class constants(Enum):  # pylint: disable=c0103
    """
    Constant values for the br_cgu_bolsa_familia project
    """

    ROOT_URL = (
        "https://portaldatransparencia.gov.br/download-de-dados/novo-bolsa-familia"
    )

    MAIN_URL = "https://dadosabertos-download.cgu.gov.br/PortalDaTransparencia/saida/novo-bolsa-familia/"
