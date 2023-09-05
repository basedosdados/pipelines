# -*- coding: utf-8 -*-
"""
Constant values for the datasets projects
"""
from enum import Enum


class constants(Enum):  # pylint: disable=c0103
    RENAME = {
        "NULOTCTM": "lote",
        "INDICE_CADASTRAL": "indice_cadastral",
        "ZONEAMENTO_PVIPTU": "zoneamento",
        "ZONA_HOMOGENIA": "zona_homogenea",
        "CEP": "cep",
        "TIPO_LOGRADOURO": "tipo_logradouro",
        "NOME_LOGRADOURO": "logradouro",
        "NUMERO_IMOVEL": "numero_imovel",
        "TIPO_CONSTRUTIVO": "tipo_construtivo",
        "TIPO_OCUPACAO": "tipo_ocupacao",
        "PADRAO_ACABAMENTO": "padrao_acabamento",
        "TIPOLOGIA": "tipologia",
        "QUANTIDADE_ECONOMIAS": "codigo_quantidade_economia",
        "FREQUENCIA_COLETA": "frequencia_coleta",
        "IND_REDE_TELEFONICA": "indicador_rede_telefonica",
        "IND_MEIO_FIO": "indicador_meio_fio",
        "IND_PAVIMENTACAO": "indicador_pavimentacao",
        "IND_ARBORIZACAO": "indicador_arborizacao",
        "IND_GALERIA_PLUVIAL": "indicador_galeria_pluvial",
        "IND_ILUMINACAO_PUBLICA": "indicador_iluminacao_publica",
        "IND_REDE_ESGOTO": "indicador_rede_esgoto",
        "IND_REDE_AGUA": "indicador_agua",
        "GEOMETRIA": "poligono",
        "FRACAO_IDEAL": "fracao_ideal",
        "AREA_TERRENO": "area_terreno",
        "AREA_CONSTRUCAO": "area_construida",
    }

    ORDEM = [
        "ano",
        "mes",
        "indice_cadastral",
        "lote",
        "zoneamento",
        "zona_homogenea",
        "cep",
        "endereco",
        "tipo_construtivo",
        "tipo_ocupacao",
        "padrao_acabamento",
        "tipologia",
        "codigo_quantidade_economia",
        "frequencia_coleta",
        "indicador_rede_telefonica",
        "indicador_meio_fio",
        "indicador_pavimentacao",
        "indicador_arborizacao",
        "indicador_galeria_pluvial",
        "indicador_iluminacao_publica",
        "indicador_rede_esgoto",
        "indicador_agua",
        "poligono",
        "fracao_ideal",
        "area_terreno",
        "area_construida",
    ]

    URLS = [
        "https://dados.pbh.gov.br/dataset/cadastro-imobiliario-regional-venda-nova",
        "https://dados.pbh.gov.br/dataset/cadastro-imobiliario-regional-pampulha",
        "https://dados.pbh.gov.br/dataset/cadastro-imobiliario-regional-oeste",
        "https://dados.pbh.gov.br/dataset/cadastro-imobiliario-regional-norte",
        "https://dados.pbh.gov.br/dataset/cadastro-imobiliario-regional-noroeste",
        "https://dados.pbh.gov.br/dataset/cadastro-imobiliario-regional-nordeste",
        "https://dados.pbh.gov.br/dataset/cadastro-imobiliario-regional-leste",
        "https://dados.pbh.gov.br/dataset/cadastro-imobiliario-regional-centro-sul",
        "https://dados.pbh.gov.br/dataset/cadastro-imobiliario",
    ]

    INPUT_PATH = "/tmp/input/"

    OUTPUT_PATH = "/tmp/output/"
