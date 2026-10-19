"""
Constant values for br_anp_precos_combustiveis.
"""

from enum import Enum

from pipelines.utils.metadata.domain import (
    DateFormat,
    DateOnly,
    FreeLag,
    PartBdpro,
)

DATASET_ID = "br_anp_precos_combustiveis"

# Única tabela do dataset.
MICRODADOS_TABLE_ID = "microdados"

# Tier part_bdpro com defasagem de 6 semanas pra liberação free — mesma
# coverage que o flow monolítico antigo já registrava em
# `register_table_materialization_task`.
COVERAGE = PartBdpro(
    date_column=DateOnly(col="data_coleta"),
    date_format=DateFormat.YEAR_MD,
    free_lag=FreeLag(unit="weeks", value=6),
)


class constants(Enum):
    RENAME = {
        "Revenda": "nome_estabelecimento",
        "CNPJ da Revenda": "cnpj_revenda",
        "Bairro": "bairro_revenda",
        "Cep": "cep_revenda",
        "Produto": "produto",
        "Valor de Venda": "preco_venda",
        "Valor de Compra": "preco_compra",
        "Unidade de Medida": "unidade_medida",
        "Bandeira": "bandeira_revenda",
        "Estado - Sigla": "sigla_uf",
        "Municipio": "nome",
        "Data da Coleta": "data_coleta",
        "Nome da Rua": "nome_rua",
        "Numero Rua": "numero_rua",
        "Complemento": "complemento",
    }

    ORDEM = [
        "ano",
        "sigla_uf",
        "id_municipio",
        "bairro_revenda",
        "cep_revenda",
        "endereco_revenda",
        "cnpj_revenda",
        "nome_estabelecimento",
        "bandeira_revenda",
        "data_coleta",
        "produto",
        "unidade_medida",
        "preco_compra",
        "preco_venda",
    ]

    URLS = [
        "https://www.gov.br/anp/pt-br/centrais-de-conteudo/dados-abertos/arquivos/shpc/qus/ultimas-4-semanas-glp.csv",
        "https://www.gov.br/anp/pt-br/centrais-de-conteudo/dados-abertos/arquivos/shpc/qus/ultimas-4-semanas-gasolina-etanol.csv",
        "https://www.gov.br/anp/pt-br/centrais-de-conteudo/dados-abertos/arquivos/shpc/qus/ultimas-4-semanas-diesel-gnv.csv",
    ]

    URLS_DATA = [
        "https://www.gov.br/anp/pt-br/centrais-de-conteudo/dados-abertos/arquivos/shpc/qus/ultimas-4-semanas-glp.csv"
    ]

    PATH_INPUT = "/tmp/input/"

    PATH_OUTPUT = "/tmp/output/"

    URL_DIESEL_GNV = "/tmp/input/ultimas-4-semanas-diesel-gnv.csv"
    URL_GASOLINA_ETANOL = "/tmp/input/ultimas-4-semanas-gasolina-etanol.csv"
    URL_GLP = "/tmp/input/ultimas-4-semanas-glp.csv"
