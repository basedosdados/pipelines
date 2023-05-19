# -*- coding: utf-8 -*-
"""
Constant values for the datasets projects
"""


###############################################################################
#
# Esse é um arquivo onde podem ser declaratas constantes que serão usadas
# pelo projeto br_cvm_fii.
#
# Por ser um arquivo opcional, pode ser removido sem prejuízo ao funcionamento
# do projeto, caos não esteja em uso.
#
# Para declarar constantes, basta fazer conforme o exemplo abaixo:
#
# ```
# class constants(Enum):
#     """
#     Constant values for the br_cvm_fii project
#     """
#     FOO = "bar"
# ```
#
# Para usá-las, basta fazer conforme o exemplo abaixo:
#
# ```py
# from pipelines.datasets.br_cvm_fii.constants import constants
# print(constants.FOO.value)
# ```
#
###############################################################################

from enum import Enum


class constants(Enum):  # pylint: disable=c0103
    """
    Constant values for the br_cvm_fii project
    """

    CDA_URL = "https://dados.cvm.gov.br/dados/FI/DOC/CDA/DADOS/"

    INFORME_DIARIO_URL = "https://dados.cvm.gov.br/dados/FI/DOC/INF_DIARIO/DADOS/"

    LISTA = [
        "indicador_emissor_ligado",
        "quantidade_vendas_negocios_mes",
        "quantidade_aquisicoes_negocios_mes",
        "quantidade_posicao_final",
        "indicador_titulo_pos_fixado",
        "indicador_emissor_pessoa_fisica_juridica",
        "indicador_codigo_identificacao_emissor_pessoa_fisica_juridica",
        "indicador_titulo_registrado_cetip",
        "indicador_titulo_possui_garantia_seguro",
        "indicador_investimento_coletivo",
        "indicador_gestao_carteira_influencia_gestor",
        "quantidade_ativos_exterior",
    ]

    COLUNAS_TOTAIS = [
        "id_fundo",
        "cnpj",
        "denominacao_social",
        "data_competencia",
        "tipo_aplicacao",
        "tipo_ativo",
        "indicador_emissor_ligado",
        "tipo_negociacao",
        "quantidade_vendas_negocios_mes",
        "valor_vendas_negocios_mes",
        "quantidade_aquisicoes_negocios_mes",
        "valor_aquisicoes_negocios_mes",
        "quantidade_posicao_final",
        "valor_mercado_posicao_final",
        "valor_custo_posicao_final",
        "prazo_confidencialidae_aplicacao",
        "codigo_ativo",
        "descricao_ativo",
        "codigo_isin",
        "data_inicio_vigencia",
        "data_fim_vigencia",
        "bloco",
        "tipo_titulo_publico",
        "codigo_selic",
        "data_emissao",
        "data_vencimento",
        "indicador_emissor_pessoa_fisica_juridica",
        "indicador_codigo_identificacao_emissor_pessoa_fisica_juridica",
        "nome_emissor",
        "indicador_titulo_pos_fixado",
        "codigo_indexador_pos_fixados",
        "descricao_indexador_pos_fixados",
        "porcentagem_indexador_pos_fixados",
        "porcentagem_cupom_pos_fixados",
        "porcentagem_taxa_concentrada_pre_fixados",
        "indicador_titulo_registrado_cetip",
        "indicador_titulo_possui_garantia_seguro",
        "cnpj_instituicao_financeira_coobrigacao",
        "codigo_swap",
        "descricao_tipo_ativo_swap",
        "cnpj_emissor",
        "indicador_emissor_possui_classificacao_risco",
        "nome_agencia_classificacao_risco",
        "data_classificacao_risco",
        "grau_risco_atribuido",
        "cnpj_fundo_investido",
        "denominacao_social_fundo_investido",
        "indicador_investimento_coletivo",
        "indicador_gestao_carteira_influencia_gestor",
        "codigo_pais",
        "nome_pais",
        "codigo_bolsa_mercado_balcao",
        "tipo_bolsa_mercado_balcao",
        "codigo_ativo_bolsa_mercado_balcao_local_aquisicao",
        "descricao_ativo_exterior",
        "quantidade_ativos_exterior",
        "valor_ativo_exterior",
        "ano",
        "mes",
    ]

    COLUNAS_FINAL = [
        "TP_FUNDO",
        "CNPJ_FUNDO",
        "DENOM_SOCIAL",
        "DT_COMPTC",
        "TP_APLIC",
        "TP_ATIVO",
        "EMISSOR_LIGADO",
        "TP_NEGOC",
        "QT_VENDA_NEGOC",
        "VL_VENDA_NEGOC",
        "QT_AQUIS_NEGOC",
        "VL_AQUIS_NEGOC",
        "QT_POS_FINAL",
        "VL_MERC_POS_FINAL",
        "VL_CUSTO_POS_FINAL",
        "DT_CONFID_APLIC",
        "TP_TITPUB",
        "CD_ISIN",
        "CD_SELIC",
        "DT_EMISSAO",
        "DT_VENC",
        "CNPJ_FUNDO_COTA",
        "NM_FUNDO_COTA",
        "CD_SWAP",
        "DS_SWAP",
        "CD_ATIVO",
        "DS_ATIVO",
        "DT_INI_VIGENCIA",
        "DT_FIM_VIGENCIA",
        "CNPJ_EMISSOR",
        "EMISSOR",
        "TITULO_POSFX",
        "CD_INDEXADOR_POSFX",
        "DS_INDEXADOR_POSFX",
        "PR_INDEXADOR_POSFX",
        "PR_CUPOM_POSFX",
        "PR_TAXA_PREFX",
        "RISCO_EMISSOR",
        "AG_RISCO",
        "DT_RISCO",
        "GRAU_RISCO",
        "PF_PJ_EMISSOR",
        "CPF_CNPJ_EMISSOR",
        "TITULO_CETIP",
        "TITULO_GARANTIA",
        "CNPJ_INSTITUICAO_FINANC_COOBR",
        "INVEST_COLETIVO",
        "INVEST_COLETIVO_GESTOR",
        "CD_PAIS",
        "PAIS",
        "CD_BV_MERC",
        "BV_MERC",
        "CD_ATIVO_BV_MERC",
        "DS_ATIVO_EXTERIOR",
        "QT_ATIVO_EXTERIOR",
        "VL_ATIVO_EXTERIOR",
    ]

    MAPEAMENTO = {"S": "1", "N": "0"}

    COLUNAS = [
        "EMISSOR_LIGADO",
        "TITULO_POSFX",
        "RISCO_EMISSOR",
        "PF_PJ_EMISSOR",
        "CPF_CNPJ_EMISSOR",
        "TITULO_CETIP",
        "TITULO_GARANTIA",
        "INVEST_COLETIVO",
        "INVEST_COLETIVO_GESTOR",
    ]

    COLUNAS_ASCI = [
        "denominacao_social",
        "tipo_aplicacao",
        "tipo_ativo",
        "descricao_ativo",
        "tipo_negociacao",
        "tipo_titulo_publico",
        "nome_emissor",
        "descricao_indexador_pos_fixados",
        "nome_agencia_classificacao_risco",
        "grau_risco_atribuido",
        "nome_pais",
        "tipo_bolsa_mercado_balcao",
        "codigo_ativo_bolsa_mercado_balcao_local_aquisicao",
    ]

    ARQUITETURA_URL = "https://docs.google.com/spreadsheets/d/1W739_mLZNBPYhBqGyjsuuWFBOqCrhrDl/edit#gid=1045172528"
