# -*- coding: utf-8 -*-
"""
Constant values for the datasets projects
"""

from enum import Enum


class constants(Enum):  # pylint: disable=c0103
    """
    Constant values for the br_denatran_frota project
    """

    MONTHS = {
        "janeiro": 1,
        "fevereiro": 2,
        "marco": 3,
        "abril": 4,
        "maio": 5,
        "junho": 6,
        "julho": 7,
        "agosto": 8,
        "setembro": 9,
        "outubro": 10,
        "novembro": 11,
        "dezembro": 12,
        # some months have capital letters deppending on the year
        "Janeiro": 1,
        "Fevereiro": 2,
        "Marco": 3,
        "Abril": 4,
        "Maio": 5,
        "Junho": 6,
        "Julho": 7,
        "Agosto": 8,
        "Setembro": 9,
        "Outubro": 10,
        "Novembro": 11,
        "Dezembro": 12,
    }

    DATASET = "br_denatran_frota"

    HEADERS = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36"
    }

    DICT_UFS = {
        "AC": "Acre",
        "AL": "Alagoas",
        "AP": "Amapá",
        "AM": "Amazonas",
        "BA": "Bahia",
        "CE": "Ceará",
        "DF": "Distrito Federal",
        "ES": "Espírito Santo",
        "GO": "Goiás",
        "MA": "Maranhão",
        "MT": "Mato Grosso",
        "MS": "Mato Grosso do Sul",
        "MG": "Minas Gerais",
        "PA": "Pará",
        "PB": "Paraíba",
        "PR": "Paraná",
        "PE": "Pernambuco",
        "PI": "Piauí",
        "RJ": "Rio de Janeiro",
        "RN": "Rio Grande do Norte",
        "RS": "Rio Grande do Sul",
        "RO": "Rondônia",
        "RR": "Roraima",
        "SC": "Santa Catarina",
        "SP": "São Paulo",
        "SE": "Sergipe",
        "TO": "Tocantins",
    }

    SUBSTITUTIONS = {
        ("RN", "assu"): "acu",
        ("PB", "sao domingos de pombal"): "sao domingos",
        ("PB", "santarem"): "joca claudino",
        ("SP", "embu"): "embu das artes",
        ("TO", "sao valerio da natividade"): "sao valerio",
        ("PB", "campo de santana"): "tacima",
        ("AP", "amapari"): "pedra branca do amapari",
        ("BA", "maracani"): "macarani",
        ("BA", "livramento do brumado"): "livramento de nossa senhora",
        ("PB", "sao bento de pombal"): "sao bentinho",
        ("PB", "serido"): "sao vicente do serido",
        ("PR", "vila alta"): "alto paraiso",
        ("RN", "espirito santo do oeste"): "parau",
        ("RO", "jamari"): "itapua do oeste",
        ("SC", "picarras"): "balneario picarras",
        ("SC", "barra do sul"): "balneario barra do sul",
    }

    DOWNLOAD_PATH = f"/tmp/input/{DATASET}"

    OUTPUT_PATH = f"/tmp/output/{DATASET}"

    UF_TIPO_BASIC_FILENAME = "frota_por_uf_e_tipo_de_veiculo"

    MUNIC_TIPO_BASIC_FILENAME = "frota_por_municipio_e_tipo"

    MONTHS_SHORT = {month[:3]: number for month, number in MONTHS.items()}

    UF_TIPO_HEADER = [
        "Grandes Regiões e\nUnidades da Federação",
        "TOTAL",
        "AUTOMÓVEL",
        "BONDE",
        "CAMINHÃO",
        "CAMINHÃO TRATOR",
        "CAMINHONETE",
        "CAMIONETA",
        "CHASSI PLATAFORMA",
        "CICLOMOTOR",
        "MICROÔNIBUS",
        "MOTOCICLETA",
        "MOTONETA",
        "ÔNIBUS",
        "QUADRICICLO",
        "REBOQUE",
        "SEMI-REBOQUE",
        "SIDE-CAR",
        "OUTROS",
        "TRATOR ESTEIRA",
        "TRATOR RODAS",
        "TRICICLO",
        "UTILITÁRIO",
    ]

    MUNICIPIO_TIPO_HEADER = [
        "UF",
        "MUNICIPIO",
        "TOTAL",
        "AUTOMÓVEL",
        "BONDE",
        "CAMINHÃO",
        "CAMINHÃO TRATOR",
        "CAMINHONETE",
        "CAMIONETA",
        "CHASSI PLATAFORMA",
        "CICLOMOTOR",
        "MICROÔNIBUS",
        "MOTOCICLETA",
        "MOTONETA",
        "ÔNIBUS",
        "QUADRICICLO",
        "REBOQUE",
        "SEMI-REBOQUE",
        "SIDE-CAR",
        "OUTROS",
        "TRATOR ESTEIRA",
        "TRATOR RODAS",
        "TRICICLO",
        "UTILITÁRIO",
    ]
