"""
Constantes de br_inep_censo_educacao_superior.
"""

from enum import Enum


class constants(Enum):
    """Constantes de br_inep_censo_educacao_superior."""

    SOURCE_LINK = "https://www.gov.br/inep/pt-br/acesso-a-informacao/dados-abertos/microdados/censo-da-educacao-superior"
    DOWNLOAD_LINK = "https://download.inep.gov.br/microdados/microdados_censo_da_educacao_superior_{ano}.zip"
