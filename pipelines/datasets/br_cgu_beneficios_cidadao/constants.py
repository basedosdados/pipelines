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

    CHROME_DRIVER = "https://chromedriver.storage.googleapis.com/114.0.5735.90/chromedriver_linux64.zip"

    PATH = "/tmp/data/br_cgu_beneficios_cidadao"

    TMP_DATA_DIR = "/tmp/data/br_cgu_beneficios_cidadao/tmp"
    ROOT_URL = (
        "https://portaldatransparencia.gov.br/download-de-dados/novo-bolsa-familia"
    )
    ROOT_URL_GARANTIA_SAFRA = (
        "https://portaldatransparencia.gov.br/download-de-dados/garantia-safra"
    )
    ROOT_URL_BPC = "https://portaldatransparencia.gov.br/download-de-dados/bpc"

    MAIN_URL_NOVO_BOLSA_FAMILIA = "https://dadosabertos-download.cgu.gov.br/PortalDaTransparencia/saida/novo-bolsa-familia/"

    MAIN_URL_GARANTIA_SAFRA = "https://dadosabertos-download.cgu.gov.br/PortalDaTransparencia/saida/garantia-safra/"

    MAIN_URL_BPC = (
        "https://dadosabertos-download.cgu.gov.br/PortalDaTransparencia/saida/bpc/"
    )

    DTYPES_NOVO_BOLSA_FAMILIA = {
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
    DTYPES_GARANTIA_SAFRA = {
        "MÊS REFERÊNCIA": str,
        "UF": str,
        "CÓDIGO MUNICÍPIO SIAFI": str,
        "NOME MUNICÍPIO": str,
        "NIS FAVORECIDO": str,
        "NOME FAVORECIDO": str,
        "VALOR PARCELA": np.float64,
    }

    DTYPES_BPC = {
        "MÊS COMPETÊNCIA": str,
        "MÊS REFERÊNCIA": str,
        "UF": str,
        "CÓDIGO MUNICÍPIO SIAFI": str,
        "NOME MUNICÍPIO": str,
        "NIS BENEFICIÁRIO": str,
        "CPF BENEFICIÁRIO": str,
        "NOME BENEFICIÁRIO": str,
        "NIS REPRESENTANTE LEGAL": str,
        "CPF REPRESENTANTE LEGAL": str,
        "NOME REPRESENTANTE LEGAL": str,
        "NÚMERO BENEFÍCIO": str,
        "BENEFÍCIO CONCEDIDO JUDICIALMENTE": str,
        "VALOR PARCELA": np.float64,
    }

    RENAMER_NOVO_BOLSA_FAMILIA = {
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
    RENAMER_GARANTIA_SAFRA = {
        "MÊS REFERÊNCIA": "mes_referencia",
        "UF": "sigla_uf",
        "CÓDIGO MUNICÍPIO SIAFI": "id_municipio_siafi",
        "NOME MUNICÍPIO": "municipio",
        "NIS FAVORECIDO": "nis",
        "NOME FAVORECIDO": "nome",
        "VALOR PARCELA": "valor",
    }
    RENAMER_BPC = {
        "MÊS COMPETÊNCIA": "mes_competencia",
        "MÊS REFERÊNCIA": "mes_referencia",
        "UF": "sigla_uf",
        "CÓDIGO MUNICÍPIO SIAFI": "id_municipio_siafi",
        "NOME MUNICÍPIO": "municipio",
        "NIS BENEFICIÁRIO": "nis",
        "CPF BENEFICIÁRIO": "cpf",
        "NOME BENEFICIÁRIO": "nome",
        "NIS REPRESENTANTE LEGAL": "nis_representante",
        "CPF REPRESENTANTE LEGAL": "cpf_representante",
        "NOME REPRESENTANTE LEGAL": "nome_representante",
        "NÚMERO BENEFÍCIO": "numero",
        "BENEFÍCIO CONCEDIDO JUDICIALMENTE": "concedido_judicialmente",
        "VALOR PARCELA": "valor",
    }
