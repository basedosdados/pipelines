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


class constants_operacoes_nao_automaticas(Enum):
    DATASET_ID = "br_bndes_operacoes_contratadas"
    TABLE_ID = "operacoes_nao_automaticas"
    CKAN_RESOURCE_ID = "6f56b78c-510f-44b6-8274-78a5b7e931f4"
    RESOURCE_SHOW_URL = (
        "https://dadosabertos.bndes.gov.br/api/3/action/resource_show"
        "?id=6f56b78c-510f-44b6-8274-78a5b7e931f4"
    )
    DOWNLOAD_URL = (
        "https://dadosabertos.bndes.gov.br/dataset/"
        "10e21ad1-568e-45e5-a8af-43f2c05ef1a2/resource/"
        "6f56b78c-510f-44b6-8274-78a5b7e931f4/download/"
        "operacoes-financiamento-operacoes-nao-automaticas.csv"
    )
    LAST_MODIFIED_FORMAT = "%Y-%m-%dT%H:%M:%S.%f"

    INPUT_PATH = "/tmp/input/br_bndes_operacoes_contratadas/"
    OUTPUT_PATH = "/tmp/output/br_bndes_operacoes_contratadas/"
    CSV_FILENAME = "operacoes-financiamento-operacoes-nao-automaticas.csv"

    DROP_COLUMNS = []

    ORDER_COLUMNS = []

    # staging do BD e sempre todo STRING; a tipagem e feita no safe_cast do dbt.
    # o schema explicito garante string consistente entre particoes (mesmo em
    # chunk com a coluna toda nula, que senao inferiria tipo null).
    SCHEMA = pa.schema([(col, pa.string()) for col in []])


class constants_administracao_publica(Enum):
    """
    Config da 2a tabela do conjunto: operacoes_administracao_publica.

    Fonte: conjunto CKAN "operacoes-com-entes-da-administracao-publica-direta".
    Decisoes de modelagem em task_davi/bndes/plano_execucao.md (e no README do
    conjunto). Pontos que importam pra quem for implementar o transform:
    - NIVEL_ATUAL_KEEP: a tabela filtra so 'CONTRATADA' (a fonte traz o funil
      inteiro). Filtro isolado numa linha do transform; a coluna nivel_atual
      permanece no schema (fica constante) pra reverter facil.
    - Valores em REAIS (nao milhares) — sem x1000. Sao inteiros, sem virgula
      decimal na fonte (parse_decimal_ptbr NAO e necessario aqui).
    - Geografia: uf -> sigla_uf ('-' -> NA); municipio (NOME) vira nome_municipio
      no STAGING; o id_municipio e resolvido no modelo dbt (join normalizado ao
      diretorio) — por isso o staging guarda nome_municipio, nao id_municipio.
    - RENAME/ORDER_COLUMNS/SCHEMA transcrevem a arquitetura aprovada. Confira o
      RENAME contra o header real do CSV antes de rodar.
    """

    DATASET_ID = "br_bndes_operacoes_contratadas"
    TABLE_ID = "operacoes_administracao_publica"
    CKAN_RESOURCE_ID = "ea4e5da3-e586-4225-a460-c5aa09e36100"

    RESOURCE_SHOW_URL = (
        "https://dadosabertos.bndes.gov.br/api/3/action/resource_show"
        "?id=ea4e5da3-e586-4225-a460-c5aa09e36100"
    )
    DOWNLOAD_URL = (
        "https://dadosabertos.bndes.gov.br/dataset/"
        "e1612ac6-f70d-4228-ba87-283848f432e3/resource/"
        "ea4e5da3-e586-4225-a460-c5aa09e36100/download/"
        "operacoes-com-entes-da-administracao-publica-direta-"
        "operacoes-com-entes-da-administracao-direta.csv"
    )
    LAST_MODIFIED_FORMAT = "%Y-%m-%dT%H:%M:%S.%f"

    # workspace compartilhado com a irma (o clean escreve em OUTPUT_PATH/TABLE_ID,
    # entao as duas tabelas nao colidem).
    INPUT_PATH = "/tmp/input/br_bndes_operacoes_contratadas/"
    OUTPUT_PATH = "/tmp/output/br_bndes_operacoes_contratadas/"
    CSV_FILENAME = "operacoes_administracao_publica.csv"

    # so filtra este nivel (a fonte traz PERSPECTIVA/C-CONSULTA/EM ANALISE/
    # APROVADA/CONTRATADA; ver dicionario).
    NIVEL_ATUAL_KEEP = "CONTRATADA"

    # header snake_case do CSV -> nome BD. Nao ha coluna a dropar (todas entram,
    # renomeadas). municipio (nome) vira nome_municipio e e resolvido pra
    # id_municipio no dbt.
    RENAME = {
        "ente_publico": "ente_publico",
        "uf": "sigla_uf",
        "municipio": "nome_municipio",
        "programa": "programa",
        "modalidade_operacional": "modalidade_operacional",
        "data_do_nivel_atual": "data_nivel_atual",
        "nivel_atual": "nivel_atual",
        "situacao_da_operacao": "situacao_operacao",
        "objetivo_do_projeto": "descricao_projeto",
        "valor_da_operacao_historico_em_reais": "valor_operacao",
        "valor_desembolsado_em_reais": "valor_desembolsado",
        "saldo_a_liberar_atualizado_em_reais": "valor_saldo_liberar",
    }

    # ordem da arquitetura; inclui `ano` (derivado de data_nivel_atual, coluna de
    # particao). SCHEMA (staging all-string) NAO inclui `ano`.
    ORDER_COLUMNS = [
        "ano",
        "sigla_uf",
        "nome_municipio",
        "ente_publico",
        "programa",
        "modalidade_operacional",
        "data_nivel_atual",
        "nivel_atual",
        "situacao_operacao",
        "descricao_projeto",
        "valor_operacao",
        "valor_desembolsado",
        "valor_saldo_liberar",
    ]

    SCHEMA = pa.schema(
        [
            (col, pa.string())
            for col in [
                "sigla_uf",
                "nome_municipio",
                "ente_publico",
                "programa",
                "modalidade_operacional",
                "data_nivel_atual",
                "nivel_atual",
                "situacao_operacao",
                "descricao_projeto",
                "valor_operacao",
                "valor_desembolsado",
                "valor_saldo_liberar",
            ]
        ]
    )
