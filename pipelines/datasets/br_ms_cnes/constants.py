# -*- coding: utf-8 -*-
"""
Constant values for the datasets projects
"""


from enum import Enum


class constants(Enum):  # pylint: disable=c0103
    """
    Constant values for the br_ms_cnes project
    """

    # to build paths
    PATH = [
        "/tmp/br_ms_cnes/input/",
        "/tmp/br_ms_cnes/output/",
    ]

    # to build paths
    TABLE = [
        "estabelecimento",
        "profissional",
        "equipamento",
        "leito",
        "equipe",
        "estabelecimento_ensino",
        "dados_complementares",
        "estabelecimento_filantropico",
        "gestao_metas",
        "habilitacao",
        "incentivos",
        "regra_contratual",
        "servico_especializado",
    ]

    # to download files from datasus FTP server
    DATABASE_GROUPS = {
        # group: table name
        "CNES": [
            "ST",
            "PF",
            "EQ",
            "LT",
            "EP",
            "EE",
            "DC",
            "EF",
            "GM",
            "HB",
            "IN",
            "RC",
            "SR",
        ],
    }

    COLUMNS_TO_KEEP = {
        # equipamento
        "EP": [
            "CODUFMUN",
            "CNES",
            "TIPEQUIP",
            "CODEQUIP",
            "QT_EXIST",
            "QT_USO",
            "IND_SUS",
            "IND_NSUS",
        ],
        # leito
        "LT": ["CNES", "TP_LEITO", "CODLEITO", "QT_EXIST", "QT_CONTR", "QT_SUS"],
        # equipe
        "EQ": [
            "CODUFMUN",
            "CNES",
            "ID_EQUIPE",
            "TIPO_EQP",
            "NOME_EQP",
            "ID_AREA",
            "NOMEAREA",
            "ID_SEGM",
            "DESCSEGM",
            "TIPOSEGM",
            "DT_ATIVA",
            "DT_DESAT",
            "MOTDESAT",
            "TP_DESAT",
            "QUILOMBO",
            "ASSENTAD",
            "POPGERAL",
            "ESCOLA",
            "INDIGENA",
            "PRONASCI",
        ],
        # profissional
        "PF": [
            "COMPETEN",
            "CNES",
            "UFMUNRES",
            "NOMEPROF",
            "CNS_PROF",
            "CBO",
            "REGISTRO",
            "CONSELHO",
            "TERCEIRO",
            "VINCULAC",
            "VINCUL_C",
            "VINCUL_A",
            "VINCUL_N",
            "PROF_SUS",
            "PROFNSUS",
            "HORAOUTR",
            "HORAHOSP",
            "HORA_AMB",
        ],
    }

    # generate YYYYMM to parse correct files from FTP server
    # usually the files are released with a 2 month delay. So this dict
    # maps the representative values of months to it as an int - 2
    GENERATE_MONTH_TO_PARSE = {
        # january : november
        1: "11",
        # february : december an so on
        2: "12",
        3: "01",
        4: "02",
        5: "03",
        6: "04",
        7: "05",
        8: "06",
        9: "07",
        10: "08",
        12: "09",
        11: "10",
    }
