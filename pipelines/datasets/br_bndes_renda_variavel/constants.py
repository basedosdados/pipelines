"""
Constantes de br_bndes_renda_variavel.

O contexto da fonte e as decisões de modelagem estão no README do conjunto.
"""

from dataclasses import dataclass
from enum import Enum

DATASET_ID = "br_bndes_renda_variavel"

EMPRESA_PARTICIPACAO_ACIONARIA_TABLE_ID = "empresa_participacao_acionaria"
FUNDO_PARTICIPACAO_TABLE_ID = "fundo_participacao"
EMPRESA_DEBENTURE_TABLE_ID = "empresa_debenture"
DESEMBOLSO_TABLE_ID = "desembolso"


@dataclass(frozen=True)
class TableSpec:
    """O que muda de uma tabela para outra.

    Attributes:
        resource_id: ID do recurso no CKAN do BNDES.
        rename: Coluna da fonte -> coluna da tabela.
        columns: Colunas da tabela, na ordem da arquitetura.
    """

    resource_id: str
    rename: dict[str, str]
    columns: list[str]


class constants(Enum):
    """Constantes de br_bndes_renda_variavel."""

    RESOURCE_SHOW_URL = (
        "https://dadosabertos.bndes.gov.br/api/3/action/resource_show"
    )
    PATH = "/tmp/br_bndes_renda_variavel/"
    SEPARATOR = ";"
    ENCODING = "cp1252"
    PARTITION_COLUMNS = ["ano"]

    NULL_VALUES = ["", "N/A", "-"]
    INDICATOR_VALUES = {"SIM": "1", "NÃO": "0"}

    CNPJ_COLUMN = "cnpj"
    CATEGORY_COLUMNS = [
        "tipo_ativo",
        "setor_atividade",
        "tipo_capital",
        "objetivo_predominante",
    ]
    INDICATOR_COLUMNS = [
        "indicador_acordo_acionistas",
        "indicador_conselho_administracao",
        "indicador_conselho_fiscal",
    ]
    DECIMAL_COLUMNS = [
        "proporcao_participacao_on",
        "proporcao_participacao_pn",
        "proporcao_participacao_total",
        "valor_desembolsado",
    ]

    TABLES = {
        EMPRESA_PARTICIPACAO_ACIONARIA_TABLE_ID: TableSpec(
            resource_id="8fdf87c4-9f60-4dc3-bd53-d9f291d5530c",
            rename={
                "ano": "ano",
                "cnpj": "cnpj",
                "sigla": "sigla",
                "razao_social": "razao_social",
                "tipo_de_ativo": "tipo_ativo",
                "setor_de_atividade": "setor_atividade",
                "aberta_fechada": "tipo_capital",
                "acordo_de_acionistas_em_vigor": "indicador_acordo_acionistas",
                "indicado_para_conselho_de_administracao": (
                    "indicador_conselho_administracao"
                ),
                "indicado_para_conselho_fiscal": "indicador_conselho_fiscal",
                "on_pp": "proporcao_participacao_on",
                "pn_pp": "proporcao_participacao_pn",
                "total_pp": "proporcao_participacao_total",
            },
            columns=[
                "ano",
                "cnpj",
                "sigla",
                "razao_social",
                "tipo_ativo",
                "setor_atividade",
                "tipo_capital",
                "indicador_acordo_acionistas",
                "indicador_conselho_administracao",
                "indicador_conselho_fiscal",
                "proporcao_participacao_on",
                "proporcao_participacao_pn",
                "proporcao_participacao_total",
            ],
        ),
        FUNDO_PARTICIPACAO_TABLE_ID: TableSpec(
            resource_id="a086290e-6cbd-4947-aa7a-ec4e26314228",
            rename={
                "ano": "ano",
                "cnpj": "cnpj",
                "sigla": "sigla",
                "razao_social": "razao_social",
                "tipo_de_ativo": "tipo_ativo",
                "setor_de_atividade": "setor_atividade",
                "participacao_pp_final": "proporcao_participacao_total",
            },
            columns=[
                "ano",
                "cnpj",
                "sigla",
                "razao_social",
                "tipo_ativo",
                "setor_atividade",
                "proporcao_participacao_total",
            ],
        ),
        EMPRESA_DEBENTURE_TABLE_ID: TableSpec(
            resource_id="8881462b-3e5b-4f44-972a-dadc2318baac",
            rename={
                "ano": "ano",
                "cnpj": "cnpj",
                "sigla": "sigla",
                "razao_social": "razao_social",
                "tipo_de_ativo": "tipo_ativo",
                "setor_de_atividade": "setor_atividade",
                "aberta_fechada": "tipo_capital",
                "acordo_de_acionistas_em_vigor": "indicador_acordo_acionistas",
                "indicado_para_conselho_de_administracao": (
                    "indicador_conselho_administracao"
                ),
                "indicado_para_conselho_fiscal": "indicador_conselho_fiscal",
                "quantidade_final": "quantidade_debentures",
            },
            columns=[
                "ano",
                "cnpj",
                "sigla",
                "razao_social",
                "tipo_ativo",
                "setor_atividade",
                "tipo_capital",
                "indicador_acordo_acionistas",
                "indicador_conselho_administracao",
                "indicador_conselho_fiscal",
                "quantidade_debentures",
            ],
        ),
        DESEMBOLSO_TABLE_ID: TableSpec(
            resource_id="d446ae3e-5ef8-47e4-bfe8-fbb51fbea20d",
            rename={
                "ano": "ano",
                "cnpj": "cnpj",
                "sigla": "sigla",
                "razao_social": "razao_social",
                "aberta_fechada": "tipo_capital",
                "tipo_de_ativo": "tipo_ativo",
                "setor": "setor_atividade",
                "objetivo_predominante": "objetivo_predominante",
                "descricao_resumida_da_operacao": "descricao_operacao",
                "valor_desembolsado_reais": "valor_desembolsado",
            },
            columns=[
                "ano",
                "cnpj",
                "sigla",
                "razao_social",
                "tipo_capital",
                "tipo_ativo",
                "setor_atividade",
                "objetivo_predominante",
                "descricao_operacao",
                "valor_desembolsado",
            ],
        ),
    }
