"""Constantes do Censo Escolar.

O porquê de cada escolha está no README do conjunto.
"""

import os
from pathlib import Path

ANO = 2025

DATASET_ID = "br_inep_censo_escolar"

TABLE_ID = "escola"

PROJECT_ID = "basedosdados"

BILLING_PROJECT_ID = "basedosdados-dev"

DATA_DIR = Path(
    os.environ.get(
        "CENSO_DATA_DIR",
        Path.home() / "Downloads" / "br_inep_censo_escolar_data",
    )
)

INPUT = DATA_DIR / "input"

OUTPUT = DATA_DIR / "output"

#: O sublinhado no fim é do arquivo republicado em julho de 2026.
URL = "https://download.inep.gov.br/dados_abertos/microdados_censo_escolar_2025_.zip"

ZIP_PATH = INPUT / f"{ANO}.zip"

ARCHITECTURE_URL = "https://docs.google.com/spreadsheets/d/1WmKRJjOmcG9uFL0LaBx4EwZUA_o2VpZK2MO3hFfTnmM/edit#gid=0"

#: Arquivos do microdado que compõem a tabela `escola`, na ordem da junção. A
#: edição 2025 passou a distribuí-los separados; até 2024 era um arquivo só.
TABELAS = ("escola", "matricula", "turma", "docente")

ENCODING = "iso-8859-1"

DELIMITER = ";"

#: Chave da junção entre os quatro arquivos.
CHAVE = "id_escola"

#: Código da unidade da federação -> sigla, para completar a coluna quando a
#: fonte a deixa em branco. São os dois primeiros dígitos do código do
#: município, e as 27 unidades não mudam.
ID_UF_SIGLA = {
    "11": "RO",
    "12": "AC",
    "13": "AM",
    "14": "RR",
    "15": "PA",
    "16": "AP",
    "17": "TO",
    "21": "MA",
    "22": "PI",
    "23": "CE",
    "24": "RN",
    "25": "PB",
    "26": "PE",
    "27": "AL",
    "28": "SE",
    "29": "BA",
    "31": "MG",
    "32": "ES",
    "33": "RJ",
    "35": "SP",
    "41": "PR",
    "42": "SC",
    "43": "RS",
    "50": "MS",
    "51": "MT",
    "52": "GO",
    "53": "DF",
}

#: Colunas que a arquitetura declara e a tabela publicada não tem. Acrescentar
#: uma coluna quebraria a leitura das partições de 2007 a 2024, que já estão no
#: bucket com o layout antigo — a tabela externa é CSV e casa por posição.
COLUNAS_NAO_PUBLICADAS = ("id_distrito",)
