"""
Constant values for br_bcb_taxa_cambio.
"""

from enum import Enum

DATASET_ID = "br_bcb_taxa_cambio"

# Tabela única do dataset.
TAXA_CAMBIO_TABLE_ID = "taxa_cambio"


class constants(Enum):
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
