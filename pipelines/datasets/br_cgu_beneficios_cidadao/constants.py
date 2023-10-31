# -*- coding: utf-8 -*-
"""
Constant values for the datasets projects
"""

from enum import Enum

import numpy as np


class constants(Enum):  # pylint: disable=c0103
    """
    Constant values for the br_cgu_bolsa_familia project
    """

    ROOT_URL = (
        "https://portaldatransparencia.gov.br/download-de-dados/novo-bolsa-familia"
    )

    MAIN_URL = "https://dadosabertos-download.cgu.gov.br/PortalDaTransparencia/saida/novo-bolsa-familia/"
    DTYPES = {
        "MÊS COMPETÊNCIA": str,
        "MÊS REFERÊNCIA": str,
        "UF": str,
        "CÓDIGO MUNICÍPIO SIAFI": str,
        "NOME MUNICÍPIO": str,
        "CPF FAVORECIDO": str,
        "NIS FAVORECIDO": str,
        "NOME FAVORECIDO": str,
        "VALOR PARCELA": np.float64,
    }
    RENAMER = {
        "MÊS COMPETÊNCIA": "mes_competencia",
        "MÊS REFERÊNCIA": "mes_referencia",
        "UF": "sigla_uf",
        "CÓDIGO MUNICÍPIO SIAFI": "id_municipio_siafi",
        "NOME MUNICÍPIO": "municipio",
        "CPF FAVORECIDO": "cpf",
        "NIS FAVORECIDO": "nis",
        "NOME FAVORECIDO": "nome",
        "VALOR PARCELA": "valor",
    }
