"""
Constant values for the datasets projects
"""

from enum import Enum


class constants(Enum):
    """
    Constant values for the br_senatran_estatisticas project
    """

    BASE_URL_PRE_2012 = "https://www.gov.br/transportes/pt-br/assuntos/transito/arquivos-senatran/estatisticas/renavam"
    BASE_URL_POST_2012 = "https://www.gov.br/transportes/pt-br/assuntos/transito/conteudo-Senatran"
    MONTHS = {
        "janeiro": 1,
        "fevereiro": 2,
        "marco": 3,
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

    MONTHS_SHORT = {month[:3]: number for month, number in MONTHS.items()}

    DATASET = "br_senatran_estatisticas"
    HEADERS = {
        "sec-ch-ua": '"Not=A?Brand";v="99", "Google Chrome";v="151", "Chromium";v="151"',
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": '"Windows"',
        "Upgrade-Insecure-Requests": "1",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/151.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
        "Sec-Fetch-Site": "same-origin",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-User": "?1",
        "Sec-Fetch-Dest": "document",
        "host": "www.gov.br",
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
        ("SP", "ibitiuva"): "pitangueiras",
    }

    UF_TIPO_BASIC_FILENAME = "frota_por_uf_e_tipo_de_veiculo"
    MUNIC_TIPO_BASIC_FILENAME = "frota_por_municipio_e_tipo"

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
