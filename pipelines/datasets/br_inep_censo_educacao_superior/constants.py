"""
Constantes de br_inep_censo_educacao_superior.
"""

from enum import Enum

from pipelines.utils.metadata.domain import AllFree, DateFormat, YearOnly


class constants(Enum):
    """Constantes de br_inep_censo_educacao_superior."""

    DATASET_ID = "br_inep_censo_educacao_superior"
    TABLES = ["curso", "ies"]

    # Primeiro ano das tabelas na BD. A página do Inep lista edições desde 1995,
    # mas a curso e a ies começam em 2009.
    FIRST_YEAR = 2009

    SOURCE_LINK = "https://www.gov.br/inep/pt-br/acesso-a-informacao/dados-abertos/microdados/censo-da-educacao-superior"
    DOWNLOAD_LINK = "https://download.inep.gov.br/microdados/microdados_censo_da_educacao_superior_{ano}.zip"

    # Planilhas de arquitetura no Drive, a fonte do esquema: o tratamento lê delas
    # quais colunas pegar do CSV do Inep, como renomear e em que ordem gravar.
    # Precisam estar compartilhadas como "qualquer pessoa com o link pode ver",
    # porque a pipeline lê sem login. A ordem das linhas é a ordem das colunas
    # no staging: não reordene nem apague linhas.
    ARCHITECTURE_SHEETS = {
        "curso": "https://docs.google.com/spreadsheets/d/1txedBwUea1blHZKutvodNmwMcDJCwXMrj5n4LLoCj5M/edit",
        "ies": "https://docs.google.com/spreadsheets/d/1659-uPwzBQ6qzSrteieQ4tXC-4Ksl2iLMxh1aU2bU04/edit",
    }

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


# Tabelas anuais e públicas: sem janela BD Pro. Fica fora do Enum porque é um
# objeto pydantic, usado como `COVERAGE.model_dump()`.
COVERAGE = AllFree(
    date_column=YearOnly(col="ano"), date_format=DateFormat.YEAR
)
