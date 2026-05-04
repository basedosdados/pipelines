"""
Constants for br_rf_cno
"""

from enum import Enum


class constants(Enum):
    XML_BODY = """<?xml version="1.0" encoding="utf-8" ?>
              <d:propfind xmlns:d="DAV:">
                <d:allprop/>
              </d:propfind>
              """

    HEADERS = {
        "Depth": "1",
        "Content-Type": "application/xml",
        "Accept": "application/xml",
        "User-Agent": "Mozilla/5.0",
    }
    URLS = {
        "url_base": "https://arquivos.receitafederal.gov.br/public.php/dav/files/gn672Ad4CF8N6TK/Dados/Cadastros/CNO/",
        "url_download": "https://arquivos.receitafederal.gov.br/public.php/dav/files/gn672Ad4CF8N6TK/Dados/Cadastros/CNO/cno.zip",
    }

    TABLES_RENAME = {
        "br_rf_cno": {
            "microdados": "cno.csv",
            "totais": "cno_totais.csv",
            "vinculos": "cno_vinculos.csv",
            "areas": "cno_areas.csv",
            "cnaes": "cno_cnaes.csv",
        }
    }

    COLUMNS_RENAME = {
        "br_rf_cno": {
            "vinculos": {
                "Data de registro": "data_registro",
                "Data de início": "data_inicio",
                "Data de fim": "data_fim",
                "CNO": "id_cno",
                "NI do responsável": "id_responsavel",
                "Qualificação do contribuinte": "qualificacao_contribuinte",
            },
            "cnaes": {
                "Data de registro": "data_registro",
                "CNO": "id_cno",
                "CNAE": "cnae_2_subclasse",
            },
            "microdados": {
                "Data da situação": "data_situacao",
                "Data de registro": "data_registro",
                "Data de início": "data_inicio",
                "Data de inicio da responsabilidade": "data_inicio_responsabilidade",
                "Código do Pais": "id_pais",
                "Nome do pais": "nome_pais",
                "Estado": "sigla_uf",
                "Código do municipio": "id_municipio_rf",
                "Bairro": "bairro",
                "CEP": "cep",
                "Logradouro": "logradouro",
                "Tipo de logradouro": "tipo_logradouro",
                "Número do logradouro": "numero_logradouro",
                "Caixa Postal": "caixa_postal",
                "CNO": "id_cno",
                "CNO vinculado": "id_cno_vinculado",
                "Situação": "situacao",
                "Complemento": "complemento",
                "NI do responsável": "id_responsavel",
                "Nome": "nome_responsavel",
                "Qualificação do responsavel": "qualificacao_responsavel",
                "Nome empresarial": "nome_empresarial",
                "Área total": "area",
                "Unidade de medida": "unidade_medida",
                "Código de localização": "id_localizacao",
            },
            "areas": {
                "CNO": "id_cno",
                "Categoria": "categoria",
                "Destinação": "destinacao",
                "Tipo de obra": "tipo_obra",
                "Tipo de Área": "tipo_area",
                "Tipo de Área Complementar": "tipo_area_complementar",
                "Metragem": "metragem",
            },
        }
    }
