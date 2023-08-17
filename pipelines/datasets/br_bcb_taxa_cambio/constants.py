# -*- coding: utf-8 -*-
"""
Constant values for the datasets projects
"""
from enum import Enum


class constants(Enum):  # pylint: disable=c0103
    """
    Constant values for the br_bcb_indicadores project
    """

    ARCHITECTURE_URL = {
        "taxa_cambio": "https://docs.google.com/spreadsheets/d/1EHjZmmPMlGxD0ACORxH-8Yhh3bcs77Y_yWMFhOWnvlc/edit#gid=0",
    }

    API_URL = {
        "taxa_cambio_moedas": "https://olinda.bcb.gov.br/olinda/servico/PTAX/versao/v1/odata/Moedas",
        "taxa_cambio": "https://olinda.bcb.gov.br/olinda/servico/PTAX/versao/v1/odata/CotacaoMoedaPeriodo(moeda='{}',dataInicial='{}',dataFinalCotacao='{}')",
    }
