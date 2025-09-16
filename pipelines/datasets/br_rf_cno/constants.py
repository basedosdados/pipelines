"""
Constants for br_rf_cno
"""

from enum import Enum


class constants(Enum):
    URL = "https://arquivos.receitafederal.gov.br/dados/cno/cno.zip"
    URL_FTP = "https://arquivos.receitafederal.gov.br/dados/cno/"

    TABLES_RENAME = {
        "cno.csv": "microdados",
        "cno_totais.csv": "totais",
        "cno_vinculos.csv": "vinculos",
        "cno_areas.csv": "areas",
        "cno_cnaes.csv": "cnaes",
    }

    COLUMNS_RENAME = {
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
