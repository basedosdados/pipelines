# -*- coding: utf-8 -*-
"""
Constant values for the datasets projects
"""

from enum import Enum


class constants(Enum):  # pylint: disable=c0103
    """
    Constant values for the crawler_datasus project
    """

    # to build paths
    PATH = [
        "/tmp/br_ms_cnes/input/",
        "/tmp/br_ms_cnes/output/",
    ]

    # to download files from datasus FTP server
    DATASUS_DATABASE = {
        "br_ms_cnes": "CNES",
        "br_ms_sia": "SIA",
        "br_ms_sih": "SIH",
        "br_ms_sinan": "SINAN",
    }

    DATASUS_DATABASE_TABLE = {
        # CNES
        "estabelecimento": "ST",
        "profissional": "PF",
        "equipamento": "EQ",
        "leito": "LT",
        "equipe": "EP",
        "estabelecimento_ensino": "EE",
        "dados_complementares": "DC",
        "estabelecimento_filantropico": "EF",
        "gestao_metas": "GM",
        "habilitacao": "HB",
        "incentivos": "IN",
        "regra_contratual": "RC",
        "servico_especializado": "SR",
        # SIA
        "producao_ambulatorial": "PA",
        "psicossocial": "PS",
        # SIH
        "servicos_profissionais": "SP",
        "aihs_reduzidas": "RD",
        # SINAN
        "microdados_dengue": "DENG",
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
        "LT": [
            "CNES",
            "TP_LEITO",
            "CODLEITO",
            "QT_EXIST",
            "QT_CONTR",
            "QT_SUS",
        ],
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
        # estabelecimento_ensino
        "EE": [
            "CODUFMUN",
            "CNES",
            "CMPT_INI",
            "CMPT_FIM",
            "SGRUPHAB",
            "DTPORTAR",
            "PORTARIA",
            "MAPORTAR",
        ],
        # estabelecimento_filantropico
        "EF": [
            "CODUFMUN",
            "CNES",
            "CMPT_INI",
            "CMPT_FIM",
            "SGRUPHAB",
            "DTPORTAR",
            "PORTARIA",
            "MAPORTAR",
        ],
        # gestao_metas
        "GM": [
            "CODUFMUN",
            "CNES",
            "CMPT_INI",
            "CMPT_FIM",
            "SGRUPHAB",
            "DTPORTAR",
            "PORTARIA",
            "MAPORTAR",
        ],
        # habilitacao
        "HB": [
            "CODUFMUN",
            "CNES",
            "NAT_JUR",
            "NULEITOS",
            "SGRUPHAB",
            "CMPT_INI",
            "CMPT_FIM",
            "DTPORTAR",
            "PORTARIA",
            "MAPORTAR",
        ],
        # incentivos
        "IN": [
            "CODUFMUN",
            "CNES",
            "CMPT_INI",
            "CMPT_FIM",
            "SGRUPHAB",
            "DTPORTAR",
            "PORTARIA",
            "MAPORTAR",
        ],
        # regra_contratual
        "RC": [
            "CODUFMUN",
            "CNES",
            "CMPT_INI",
            "CMPT_FIM",
            "SGRUPHAB",
            "DTPORTAR",
            "PORTARIA",
            "MAPORTAR",
        ],
        # servico_especializado
        "SR": [
            "CODUFMUN",
            "CNES",
            "SERV_ESP",
            "CLASS_SR",
            "SRVUNICO",
            "CARACTER",
            "AMB_NSUS",
            "AMB_SUS",
            "HOSP_NSUS",
            "HOSP_SUS",
            "CONTSRVU",
            "CNESTERC",
        ],
        # dados complementares
        "DC": [
            "CODUFMUN",
            "CNES",
            "S_HBSAGP",
            "S_HBSAGN",
            "S_DPI",
            "S_DPAC",
            "S_REAGP",
            "S_REAGN",
            "S_REHCV",
            "MAQ_PROP",
            "MAQ_OUTR",
            "F_AREIA",
            "F_CARVAO",
            "ABRANDAD",
            "DEIONIZA",
            "OSMOSE_R",
            "OUT_TRAT",
            "CNS_NEFR",
            "DIALISE",
            "SIMUL_RD",
            "PLANJ_RD",
            "ARMAZ_FT",
            "CONF_MAS",
            "SALA_MOL",
            "BLOCOPER",
            "S_ARMAZE",
            "S_PREPAR",
            "S_QCDURA",
            "S_QLDURA",
            "S_CPFLUX",
            "S_SIMULA",
            "S_ACELL6",
            "S_ALSEME",
            "S_ALCOME",
            "ORTV1050",
            "ORV50150",
            "OV150500",
            "UN_COBAL",
            "EQBRBAIX",
            "EQBRMEDI",
            "EQBRALTA",
            "EQ_MAREA",
            "EQ_MINDI",
            "EQSISPLN",
            "EQDOSCLI",
            "EQFONSEL",
            "CNS_ADM",
            "CNS_OPED",
            "CNS_CONC",
            "CNS_OCLIN",
            "CNS_MRAD",
            "CNS_FNUC",
            "QUIMRADI",
            "S_RECEPC",
            "S_TRIHMT",
            "S_TRICLI",
            "S_COLETA",
            "S_AFERES",
            "S_PREEST",
            "S_PROCES",
            "S_ESTOQU",
            "S_DISTRI",
            "S_SOROLO",
            "S_IMUNOH",
            "S_PRETRA",
            "S_HEMOST",
            "S_CONTRQ",
            "S_BIOMOL",
            "S_IMUNFE",
            "S_TRANSF",
            "S_SGDOAD",
            "QT_CADRE",
            "QT_CENRE",
            "QT_REFSA",
            "QT_CONRA",
            "QT_EXTPL",
            "QT_FRE18",
            "QT_FRE30",
            "QT_AGIPL",
            "QT_SELAD",
            "QT_IRRHE",
            "QT_AGLTN",
            "QT_MAQAF",
            "QT_REFRE",
            "QT_REFAS",
            "QT_CAPFL",
            "CNS_HMTR",
            "CNS_HMTL",
            "CNS_CRES",
            "CNS_RTEC",
            "HEMOTERA",
        ],
        "DENG": [
            "TP_NOT",
            "ID_AGRAVO",
            "DT_NOTIFIC",
            "SEM_NOT",
            "SG_UF_NOT",
            "ID_REGIONA",
            "ID_MUNICIP",
            "ID_UNIDADE",
            "DT_SIN_PRI",
            "SEM_PRI",
            "ID_PAIS",
            "SG_UF",
            "ID_RG_RESI",
            "ID_MN_RESI",
            "ANO_NASC",
            "DT_NASC",
            "NU_IDADE_N",
            "CS_SEXO",
            "CS_RACA",
            "CS_ESCOL_N",
            "ID_OCUPA_N",
            "CS_GESTANT",
            "AUTO_IMUNE",
            "DIABETES",
            "HEMATOLOG",
            "HEPATOPAT",
            "RENAL",
            "HIPERTENSA",
            "ACIDO_PEPT",
            "VACINADO",
            "DT_DOSE",
            "DT_INVEST",
            "FEBRE",
            "DT_FEBRE",
            "DURACAO",
            "CEFALEIA",
            "EXANTEMA",
            "DOR_COSTAS",
            "PROSTACAO",
            "MIALGIA",
            "VOMITO",
            "NAUSEAS",
            "DIARREIA",
            "CONJUNTVIT",
            "DOR_RETRO",
            "ARTRALGIA",
            "ARTRITE",
            "LEUCOPENIA",
            "EPISTAXE",
            "PETEQUIA_N",
            "GENGIVO",
            "METRO",
            "HEMATURA",
            "SANGRAM",
            "COMPLICA",
            "ASCITE",
            "PLEURAL",
            "PERICARDI",
            "ABDOMINAL",
            "HEPATO",
            "MIOCARDI",
            "HIPOTENSAO",
            "CHOQUE",
            "INSUFICIEN",
            "OUTROS",
            "SIN_OUT",
            "DT_CHOQUE",
            "LACO_N",
            "HOSPITALIZ",
            "DT_INTERNA",
            "UF",
            "MUNICIPIO",
            "ALRM_HIPOT",
            "ALRM_PLAQ",
            "ALRM_VOM",
            "ALRM_SANG",
            "ALRM_HEMAT",
            "ALRM_ABDOM",
            "ALRM_LETAR",
            "ALRM_HEPAT",
            "ALRM_LIQ",
            "DT_ALRM",
            "GRAV_PULSO",
            "GRAV_CONV",
            "GRAV_ENCH",
            "GRAV_INSUF",
            "GRAV_TAQUI",
            "GRAV_EXTRE",
            "GRAV_HIPOT",
            "GRAV_HEMAT",
            "GRAV_MELEN",
            "GRAV_METRO",
            "GRAV_SANG",
            "GRAV_AST",
            "GRAV_MIOC",
            "GRAV_CONSC",
            "GRAV_ORGAO",
            "DT_GRAV",
            "DT_COL_HEM",
            "HEMA_MAIOR",
            "DT_COL_PLQ",
            "PALQ_MAIOR",
            "DT_COL_HE2",
            "HEMA_MENOR",
            "DT_COL_PL2",
            "PLAQ_MENOR",
            "DT_CHIK_S1",
            "DT_SOROR1",
            "RES_CHIKS1",
            "S1_IGM",
            "S1_IGG",
            "S1_TIT1",
            "DT_CHIK_S2",
            "DT_SOROR2",
            "RES_CHIKS2",
            "S2_IGM",
            "S2_IGG",
            "S2_TIT1",
            "DT_PRNT",
            "RESUL_PRNT",
            "DT_NS1",
            "RESUL_NS1",
            "DT_VIRAL",
            "RESUL_VI_N",
            "DT_PCR",
            "RESUL_PCR_",
            "AMOS_PCR",
            "AMOS_OUT",
            "TECNICA",
            "RESUL_OUT",
            "DT_SORO",
            "RESUL_SORO",
            "SOROTIPO",
            "HISTOPA_N",
            "IMUNOH_N",
            "MANI_HEMOR",
            "CLASSI_FIN",
            "CRITERIO",
            "CON_FHD",
            "TPAUTOCTO",
            "COPAISINF",
            "COUFINF",
            "COMUNINF",
            "DOENCA_TRA",
            "CLINC_CHIK",
            "EVOLUCAO",
            "DT_OBITO",
            "DT_ENCERRA",
            "TP_SISTEMA",
            "DT_DIGITA",
            "NDUPLIC_N",
            "CS_FLXRET",
        ],
    }

    GENERATE_MONTH_TO_PARSE = {
        12: "11",
        1: "12",
        2: "01",
        3: "02",
        4: "03",
        5: "04",
        6: "05",
        7: "06",
        8: "07",
        9: "08",
        10: "09",
        11: "10",
    }
