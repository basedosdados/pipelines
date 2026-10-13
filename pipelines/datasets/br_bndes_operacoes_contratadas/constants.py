"""
Constant values for br_bndes_operacoes_contratadas.
"""

from enum import Enum

from pipelines.utils.metadata.domain import AllFree, DateFormat, YearOnly

DATASET_ID = "br_bndes_operacoes_contratadas"

# 5 das 6 tabelas do dataset — migradas pro pipeline em estágios (staged
# pipeline). A lógica de download/clean de cada uma segue existindo em
# `pipelines/crawler/bndes/` (tasks.py/utils.py/constants.py), reaproveitada
# por `tasks.py` deste pacote — só a fiação de check_update/extract_and_load
# é nova. A 6ª tabela, `operacoes_pre_embarque`, é uma exceção de diretório
# único (ver `constants`/`Enum` abaixo e `flows.py`) — ainda não migrada pro
# pipeline em estágios.
OPERACOES_INDIRETAS_AUTOMATICAS_TABLE_ID = "operacoes_indiretas_automaticas"
OPERACOES_NAO_AUTOMATICAS_TABLE_ID = "operacoes_nao_automaticas"
OPERACOES_ADMINISTRACAO_PUBLICA_TABLE_ID = "operacoes_administracao_publica"
OPERACOES_EXPORTACAO_BENS_TABLE_ID = "operacoes_exportacao_bens"
OPERACOES_EXPORTACAO_SERVICOS_TABLE_ID = "operacoes_exportacao_servicos"

# Mesma coverage pras 5 tabelas acima (dado público, anual, coluna `ano`) —
# igual ao que `_run_operacoes*` já registrava em
# `register_table_materialization_task` (pipelines/crawler/bndes/flows.py).
COVERAGE = AllFree(
    date_column=YearOnly(col="ano"), date_format=DateFormat.YEAR
)

# Granularidade do `last_modified` do recurso CKAN (RawDataSource Update) que
# o poll compara — não confundir com a coverage anual da tabela (`ano`). Ver
# a nota de duas datas em pipelines/crawler/bndes/flows.py.
SOURCE_DATE_FORMAT = "%Y-%m-%d"


class constants(Enum):
    """Constantes da tabela operacoes_pre_embarque."""

    CKAN_RESOURCE_ID = "81f5d4d7-b5d0-460d-8639-423df942045b"
    RESOURCE_SHOW_URL = (
        "https://dadosabertos.bndes.gov.br/api/3/action/resource_show"
    )

    PATH = "/tmp/br_bndes_operacoes_contratadas/operacoes_pre_embarque/"
    CSV_FILENAME = (
        "operacoes-exportacao-operacoes-de-exportacao-pre-embarque.csv"
    )

    SOURCE_DATE_FORMAT = "%d/%m/%Y"

    RENAME = {
        "cliente": "nome_cliente",
        "cpf_cnpj": "cnpj_cliente",
        "uf": "sigla_uf",
        "municipio_codigo": "id_municipio",
        "data_da_contratacao": "data_contratacao",
        "valor_da_operacao_em_reais": "valor_operacao",
        "valor_desembolsado_em_reais": "valor_desembolsado",
        "fonte_de_recurso_desembolsos": "fonte_recurso",
        "modalidade_de_apoio": "modalidade_apoio",
        "forma_de_apoio": "forma_apoio",
        "produto": "produto",
        "instrumento_financeiro": "instrumento_financeiro",
        "inovacao": "indicador_inovacao",
        "area_operacional": "area_operacional",
        "setor_cnae": "setor_cnae",
        "subsetor_cnae_agrupado": "subsetor_cnae_agrupado",
        "subsetor_cnae_codigo": "codigo_subsetor_cnae",
        "subsetor_cnae_nome": "nome_subsetor_cnae",
        "setor_bndes": "setor_bndes",
        "subsetor_bndes": "subsetor_bndes",
        "porte_do_cliente": "porte_cliente",
        "natureza_do_cliente": "natureza_cliente",
        "instituicao_financeira_credenciada": (
            "nome_instituicao_financeira_credenciada"
        ),
        "cnpj_do_agente_financeiro": "cnpj_instituicao_financeira_credenciada",
        "situacao_da_operacao": "situacao_operacao",
    }
    DROP_COLUMNS = ["municipio"]

    PARTITION_COLUMNS = ["ano"]

    COLUMNS = [
        "data_contratacao",
        "sigla_uf",
        "id_municipio",
        "cnpj_cliente",
        "nome_cliente",
        "porte_cliente",
        "natureza_cliente",
        "fonte_recurso",
        "modalidade_apoio",
        "forma_apoio",
        "produto",
        "instrumento_financeiro",
        "indicador_inovacao",
        "area_operacional",
        "setor_cnae",
        "subsetor_cnae_agrupado",
        "codigo_subsetor_cnae",
        "nome_subsetor_cnae",
        "setor_bndes",
        "subsetor_bndes",
        "nome_instituicao_financeira_credenciada",
        "cnpj_instituicao_financeira_credenciada",
        "situacao_operacao",
        "valor_operacao",
        "valor_desembolsado",
    ]
