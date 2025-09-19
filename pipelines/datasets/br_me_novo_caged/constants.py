# -*- coding: utf-8 -*-
"""
Constants for br_me_novo_caged
"""

from enum import Enum
from pathlib import Path


class constants(Enum):  # pylint: disable=c0103
    FTP_HOST = "ftp.mtps.gov.br"
    REMOTE_DIR = "pdet/microdados/NOVO CAGED"

    URL_SCHEDULE = "https://www.gov.br/trabalho-e-emprego/pt-br/assuntos/estatisticas-trabalho/o-pdet/calendario-de-divulgacao-do-novo-caged"
    CSS_SELECTOR_SCHEDULES = (
        "#c0c7e623-d6c5-41c0-b9cc-acb0acd78ac7 > div > div > ul > li"
    )

    FILE_TYPES = ["EXC", "FOR", "MOV"]
    RENAME_DICT = {
        "uf": "sigla_uf",
        "municipio": "id_municipio",
        "secao": "cnae_2_secao",
        "subclasse": "cnae_2_subclasse",
        "cbo2002ocupacao": "cbo_2002",
        "saldomovimentacao": "saldo_movimentacao",
        "categoria": "categoria",
        "graudeinstrucao": "grau_instrucao",
        "idade": "idade",
        "horascontratuais": "horas_contratuais",
        "racacor": "raca_cor",
        "sexo": "sexo",
        "salario": "salario_mensal",
        "tipoempregador": "tipo_empregador",
        "tipoestabelecimento": "tipo_estabelecimento",
        "tipomovimentacao": "tipo_movimentacao",
        "tipodedeficiencia": "tipo_deficiencia",
        "indtrabintermitente": "indicador_trabalho_intermitente",
        "indtrabparcial": "indicador_trabalho_parcial",
        "tamestabjan": "tamanho_estabelecimento_janeiro",
        "indicadoraprendiz": "indicador_aprendiz",
        "origemdainformacao": "origem_informacao",
        "indicadordeforadoprazo": "indicador_fora_prazo",
        "indicadordeexclusao": "indicador_exclusao",
    }

    UF_DICT = {
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
        "99": "UF não identificada",
    }

    FULL_MONTHS = {
        "janeiro": 1,
        "fevereiro": 2,
        "março": 3,
        "abril": 4,
        "maio": 5,
        "junho": 6,
        "julho": 7,
        "agosto": 8,
        "setembro": 9,
        "outubro": 10,
        "novembro": 11,
        "dezembro": 12,
    }
    COLUMNS_TO_DROP = {
        "microdados_movimentacao": [
            "sigla_uf",
            "regiao",
            "unidadesalariocodigo",
            "valorsalariofixo",
        ],
        "microdados_movimentacao_fora_prazo": [
            "sigla_uf",
            "regiao",
            "unidadesalariocodigo",
            "valorsalariofixo",
        ],
        "microdados_movimentacao_excluida": [
            "sigla_uf",
            "regiao",
            "unidadesalariocodigo",
            "valorsalariofixo",
        ],
    }

    TMP_DIR = Path("tmp")
    DATASET_DIR = TMP_DIR / "br_me_novo_caged"

    TMP_DIR.mkdir(exist_ok=True, parents=True)
    DATASET_DIR.mkdir(exist_ok=True, parents=True)
