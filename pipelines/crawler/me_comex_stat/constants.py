"""
Constants for br_me_comex_stat
"""

from enum import Enum


class constants(Enum):
    """
    Constants for br_me_comex_stat
    """

    DOWNLOAD_LINK = "https://www.gov.br/mdic/pt-br/assuntos/comercio-exterior/estatisticas/base-de-dados-bruta"
    VALIDATION_LINK = "https://balanca.economia.gov.br/balanca/bd/comexstat-bd"

    PATH = "/tmp/br_me_comex_stat/"

    TABLE_TYPE = ["mun", "ncm"]

    TABLE_MUNICIPIO = [
        "EXP_COMPLETA_MUN",
        "IMP_COMPLETA_MUN",
    ]

    TABLE_NCM = [
        "EXP_COMPLETA",
        "IMP_COMPLETA",
    ]

    TABLE_NAME = [
        "mun_imp",
        "mun_exp",
        "ncm_imp",
        "ncm_exp",
    ]

    RENAME_NCM = {
        "CO_ANO": "ano",
        "CO_MES": "mes",
        "CO_NCM": "id_ncm",
        "CO_UNID": "id_unidade",
        "CO_PAIS": "id_pais",
        "SG_UF_NCM": "sigla_uf_ncm",
        "CO_VIA": "id_via",
        "CO_URF": "id_urf",
        "QT_ESTAT": "quantidade_estatistica",
        "KG_LIQUIDO": "peso_liquido_kg",
        "VL_FOB": "valor_fob_dolar",
        "VL_FRETE": "valor_frete",
        "VL_SEGURO": "valor_seguro",
    }

    RENAME_MUN = {
        "CO_ANO": "ano",
        "CO_MES": "mes",
        "SH4": "id_sh4",
        "CO_PAIS": "id_pais",
        "SG_UF_MUN": "sigla_uf",
        "CO_MUN": "id_municipio",
        "KG_LIQUIDO": "peso_liquido_kg",
        "VL_FOB": "valor_fob_dolar",
    }
