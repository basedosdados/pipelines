"""
Constantes de br_inep_censo_educacao_superior.
"""

from enum import Enum
from pathlib import Path

# Raiz do repositório: as arquiteturas versionadas são a fonte única do esquema.
_REPO_ROOT = Path(__file__).resolve().parents[3]


class constants(Enum):
    """Constantes de br_inep_censo_educacao_superior."""

    SOURCE_LINK = "https://www.gov.br/inep/pt-br/acesso-a-informacao/dados-abertos/microdados/censo-da-educacao-superior"
    DOWNLOAD_LINK = "https://download.inep.gov.br/microdados/microdados_censo_da_educacao_superior_{ano}.zip"

    ARCHITECTURE_DIR = (
        _REPO_ROOT
        / "models"
        / "br_inep_censo_educacao_superior"
        / "code"
        / "architecture"
    )

    # Trecho do nome do CSV de cada tabela dentro do zip. O nome completo muda
    # entre anos (inclusive a caixa da extensão), por isso a busca é por trecho.
    MEMBERS = {
        "curso": "CADASTRO_CURSOS",
        "ies": "_IES_",
    }

    # Prefixos de nomes antigos de colunas do Inep e o prefixo atual. Aplicados
    # antes da arquitetura, que só conhece os nomes atuais. Sem isso, as colunas
    # de docentes da ies de 2009 (`DOC_EX_FEMI`, `QT_DOCENTE_TOTAL`...) ficariam
    # nulas, e a BD tem esses dados.
    OLD_PREFIXES = {
        "curso": {
            # Só em 2020 o código CINE se chama CO_CINE_ROTULO2.
            "CO_CINE_ROTULO2": "CO_CINE_ROTULO",
        },
        "ies": {
            "DOC_EX_": "QT_DOC_EX_",
            "QT_DOCENTE_": "QT_DOC_",
        },
    }

    ENCODING = "windows-1252"
    SEPARATOR = ";"

    # Linhas lidas por vez. O CSV de cursos de 2024 tem 720 mil linhas e ~440 MB;
    # lido de uma vez, como texto, passa de 12 GB de memória.
    CHUNK_SIZE = 100_000
