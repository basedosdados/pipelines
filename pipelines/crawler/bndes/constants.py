"""
Constant values for the br_bndes_operacoes_contratadas crawler.

Notas de modelagem (detalhe em task_davi/README.md):
- RENAME mapeia o header snake_case do CSV do dados abertos para os nomes BD;
  "municipio" (nome) e descartado (vem do diretorio via id_municipio).
- ORDER_COLUMNS segue a arquitetura e inclui `ano`. SCHEMA nao inclui `ano`:
  ele e derivado de data_contratacao e vira coluna de particao (ano=<n>/).
- LAST_MODIFIED_FORMAT casa com o campo CKAN result.last_modified
  (ISO com microssegundos, sem timezone; ex.: "2026-07-02T05:06:06.332040").
- DOWNLOAD_URL e o campo result.url do recurso (CSV consolidado, ~1,11 GB,
  ; / cp1252).
"""

from enum import Enum

import pyarrow as pa


class constants(Enum):
    DATASET_ID = "br_bndes_operacoes_contratadas"
    TABLE_ID = "operacoes_indiretas_automaticas"
    CKAN_RESOURCE_ID = "612faa0b-b6be-4b2c-9317-da5dc2c0b901"

    RESOURCE_SHOW_URL = (
        "https://dadosabertos.bndes.gov.br/api/3/action/resource_show"
        "?id=612faa0b-b6be-4b2c-9317-da5dc2c0b901"
    )
    DOWNLOAD_URL = (
        "https://dadosabertos.bndes.gov.br/dataset/"
        "10e21ad1-568e-45e5-a8af-43f2c05ef1a2/resource/"
        "612faa0b-b6be-4b2c-9317-da5dc2c0b901/download/"
        "operacoes-financiamento-operacoes-indiretas-automaticas.csv"
    )
    LAST_MODIFIED_FORMAT = "%Y-%m-%dT%H:%M:%S.%f"

    INPUT_PATH = "/tmp/input/br_bndes_operacoes_contratadas/"
    OUTPUT_PATH = "/tmp/output/br_bndes_operacoes_contratadas/"
    CSV_FILENAME = (
        "operacoes-financiamento-operacoes-indiretas-automaticas.csv"
    )

    RENAME = {
        "cliente": "nome_cliente",
        "cpf_cnpj": "cnpj_cliente",
        "uf": "sigla_uf",
        "municipio_codigo": "id_municipio",
        "data_da_contratacao": "data_contratacao",
        "valor_da_operacao_em_reais": "valor_operacao",
        "valor_desembolsado_reais": "valor_desembolsado",
        "fonte_de_recurso_desembolsos": "fonte_recurso",
        "custo_financeiro": "custo_financeiro",
        "juros": "taxa_juros",
        "prazo_carencia_meses": "prazo_carencia",
        "prazo_amortizacao_meses": "prazo_amortizacao",
        "modalidade_de_apoio": "modalidade_apoio",
        "forma_de_apoio": "forma_apoio",
        "produto": "produto",
        "instrumento_financeiro": "instrumento_financeiro",
        "inovacao": "inovacao",
        "area_operacional": "area_operacional",
        "setor_cnae": "setor_cnae",
        "subsetor_cnae_agrupado": "subsetor_cnae_agrupado",
        "subsetor_cnae_codigo": "codigo_subsetor_cnae",
        "subsetor_cnae_nome": "nome_subsetor_cnae",
        "setor_bndes": "setor_bndes",
        "subsetor_bndes": "subsetor_bndes",
        "porte_do_cliente": "porte_cliente",
        "natureza_do_cliente": "natureza_cliente",
        "instituicao_financeira_credenciada": "instituicao_financeira_credenciada",
        "cnpj_do_agente_financeiro": "cnpj_agente_financeiro",
        "situacao_da_operacao": "situacao_operacao",
    }

    DROP_COLUMNS = ["municipio"]

    ORDER_COLUMNS = [
        "ano",
        "data_contratacao",
        "sigla_uf",
        "id_municipio",
        "cnpj_cliente",
        "nome_cliente",
        "porte_cliente",
        "natureza_cliente",
        "valor_operacao",
        "valor_desembolsado",
        "fonte_recurso",
        "custo_financeiro",
        "taxa_juros",
        "prazo_carencia",
        "prazo_amortizacao",
        "modalidade_apoio",
        "forma_apoio",
        "produto",
        "instrumento_financeiro",
        "inovacao",
        "area_operacional",
        "setor_cnae",
        "subsetor_cnae_agrupado",
        "codigo_subsetor_cnae",
        "nome_subsetor_cnae",
        "setor_bndes",
        "subsetor_bndes",
        "instituicao_financeira_credenciada",
        "cnpj_agente_financeiro",
        "situacao_operacao",
    ]

    # staging do BD e sempre todo STRING; a tipagem e feita no safe_cast do dbt.
    # o schema explicito garante string consistente entre particoes (mesmo em
    # chunk com a coluna toda nula, que senao inferiria tipo null).
    SCHEMA = pa.schema(
        [
            (col, pa.string())
            for col in [
                "data_contratacao",
                "sigla_uf",
                "id_municipio",
                "cnpj_cliente",
                "nome_cliente",
                "porte_cliente",
                "natureza_cliente",
                "valor_operacao",
                "valor_desembolsado",
                "fonte_recurso",
                "custo_financeiro",
                "taxa_juros",
                "prazo_carencia",
                "prazo_amortizacao",
                "modalidade_apoio",
                "forma_apoio",
                "produto",
                "instrumento_financeiro",
                "inovacao",
                "area_operacional",
                "setor_cnae",
                "subsetor_cnae_agrupado",
                "codigo_subsetor_cnae",
                "nome_subsetor_cnae",
                "setor_bndes",
                "subsetor_bndes",
                "instituicao_financeira_credenciada",
                "cnpj_agente_financeiro",
                "situacao_operacao",
            ]
        ]
    )
