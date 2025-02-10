# -*- coding: utf-8 -*-
"""
Constant values for the datasets projects
"""

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

    COLUNAS_FINAL_INF = [
        "id_fundo",
        "cnpj",
        "data_competencia",
        "valor_total",
        "valor_cota",
        "valor_patrimonio_liquido",
        "captacao_dia",
        "regate_dia",
        "quantidade_cotistas",
        "ano",
        "mes",
    ]

    ARQUITETURA_URL_INF = "https://docs.google.com/spreadsheets/d/1W739_mLZNBPYhBqGyjsuuWFBOqCrhrDl/edit#gid=1045172528"

    COLUNAS_ASCI_EXT = [
        "denominacao_social",
        "condominio",
        "nome_mercado",
        "tipo_prazo",
        "prazo",
        "publico_alvo",
        "classificacao_anbima",
        "forma_distribuicao",
        "politica_investimento",
        "prazo_atualizacao_valor_cota",
        "cota_emissao",
        "patrimonio_liquido_cota",
        "tipo_prazo_pagamento_resgates",
        "parametro_taxa_performance",
        "metodo_calculo_taxa_performance",
        "informacoes_adicionais_taxa_performance",
        "finalidade_operacoes_derivativos",
    ]

    COLUNAS_TOTAIS_EXT = [
        "CNPJ_FUNDO",
        "DENOM_SOCIAL",
        "DT_COMPTC",
        "CONDOM",
        "NEGOC_MERC",
        "MERCADO",
        "TP_PRAZO",
        "PRAZO",
        "PUBLICO_ALVO",
        "REG_ANBIMA",
        "CLASSE_ANBIMA",
        "DISTRIB",
        "POLIT_INVEST",
        "APLIC_MAX_FUNDO_LIGADO",
        "RESULT_CART_INCORP_PL",
        "FUNDO_COTAS",
        "FUNDO_ESPELHO",
        "APLIC_MIN",
        "ATUALIZ_DIARIA_COTA",
        "PRAZO_ATUALIZ_COTA",
        "COTA_EMISSAO",
        "COTA_PL",
        "QT_DIA_CONVERSAO_COTA",
        "QT_DIA_PAGTO_COTA",
        "QT_DIA_RESGATE_COTAS",
        "QT_DIA_PAGTO_RESGATE",
        "TP_DIA_PAGTO_RESGATE",
        "TAXA_SAIDA_PAGTO_RESGATE",
        "TAXA_ADM",
        "TAXA_CUSTODIA_MAX",
        "EXISTE_TAXA_PERFM",
        "TAXA_PERFM",
        "PARAM_TAXA_PERFM",
        "PR_INDICE_REFER_TAXA_PERFM",
        "VL_CUPOM",
        "CALC_TAXA_PERFM",
        "INF_TAXA_PERFM",
        "EXISTE_TAXA_INGRESSO",
        "TAXA_INGRESSO_REAL",
        "TAXA_INGRESSO_PR",
        "EXISTE_TAXA_SAIDA",
        "TAXA_SAIDA_REAL",
        "TAXA_SAIDA_PR",
        "OPER_DERIV",
        "FINALIDADE_OPER_DERIV",
        "OPER_VL_SUPERIOR_PL",
        "FATOR_OPER_VL_SUPERIOR_PL",
        "CONTRAP_LIGADO",
        "INVEST_EXTERIOR",
        "APLIC_MAX_ATIVO_EXTERIOR",
        "ATIVO_CRED_PRIV",
        "APLIC_MAX_ATIVO_CRED_PRIV",
        "PR_INSTITUICAO_FINANC_MIN",
        "PR_INSTITUICAO_FINANC_MAX",
        "PR_CIA_MIN",
        "PR_CIA_MAX",
        "PR_FI_MIN",
        "PR_FI_MAX",
        "PR_UNIAO_MIN",
        "PR_UNIAO_MAX",
        "PR_ADMIN_GESTOR_MIN",
        "PR_ADMIN_GESTOR_MAX",
        "PR_EMISSOR_OUTRO_MIN",
        "PR_EMISSOR_OUTRO_MAX",
        "PR_COTA_FI_MIN",
        "PR_COTA_FI_MAX",
        "PR_COTA_FIC_MIN",
        "PR_COTA_FIC_MAX",
        "PR_COTA_FI_QUALIF_MIN",
        "PR_COTA_FI_QUALIF_MAX",
        "PR_COTA_FIC_QUALIF_MIN",
        "PR_COTA_FIC_QUALIF_MAX",
        "PR_COTA_FI_PROF_MIN",
        "PR_COTA_FI_PROF_MAX",
        "PR_COTA_FIC_PROF_MIN",
        "PR_COTA_FIC_PROF_MAX",
        "PR_COTA_FII_MIN",
        "PR_COTA_FII_MAX",
        "PR_COTA_FIDC_MIN",
        "PR_COTA_FIDC_MAX",
        "PR_COTA_FICFIDC_MIN",
        "PR_COTA_FICFIDC_MAX",
        "PR_COTA_FIDC_NP_MIN",
        "PR_COTA_FIDC_NP_MAX",
        "PR_COTA_FICFIDC_NP_MIN",
        "PR_COTA_FICFIDC_NP_MAX",
        "PR_COTA_ETF_MIN",
        "PR_COTA_ETF_MAX",
        "PR_CRI_MIN",
        "PR_CRI_MAX",
        "PR_TITPUB_MIN",
        "PR_TITPUB_MAX",
        "PR_OURO_MIN",
        "PR_OURO_MAX",
        "PR_TIT_INSTITUICAO_FINANC_BACEN_MIN",
        "PR_TIT_INSTITUICAO_FINANC_BACEN_MAX",
        "PR_VLMOB_MIN",
        "PR_VLMOB_MAX",
        "PR_ACAO_MIN",
        "PR_ACAO_MAX",
        "PR_DEBENTURE_MIN",
        "PR_DEBENTURE_MAX",
        "PR_NP_MIN",
        "PR_NP_MAX",
        "PR_COMPROM_MIN",
        "PR_COMPROM_MAX",
        "PR_DERIV_MIN",
        "PR_DERIV_MAX",
        "PR_ATIVO_OUTRO_MIN",
        "PR_ATIVO_OUTRO_MAX",
        "PR_COTA_FMIEE_MIN",
        "PR_COTA_FMIEE_MAX",
        "PR_COTA_FIP_MIN",
        "PR_COTA_FIP_MAX",
        "PR_COTA_FICFIP_MIN",
        "PR_COTA_FICFIP_MAX",
        "ano",
        "mes",
    ]

    COLUNAS_FINAIS_EXT = [
        "cnpj",
        "denominacao_social",
        "data_competencia",
        "condominio",
        "indicador_negociacao_mercado",
        "nome_mercado",
        "tipo_prazo",
        "prazo",
        "publico_alvo",
        "indicador_registro_anbima",
        "classificacao_anbima",
        "forma_distribuicao",
        "politica_investimento",
        "porcentagem_aplicacao_maximo_fundo_ligado",
        "indicador_resultados_carteira_incorporado_patrimonio_liquido",
        "indicador_fundo_cotas",
        "indicador_fundo_espelho",
        "aplicacao_minima",
        "indicador_atualizacao_diaria_cota",
        "prazo_atualizacao_valor_cota",
        "cota_emissao",
        "patrimonio_liquido_cota",
        "quantidade_dias_conversao_cota",
        "quantidade_dias_pagamento_cota",
        "quantidade_dias_carencia_resgate_cotas",
        "quantidade_dias_pagamento_resgates",
        "tipo_prazo_pagamento_resgates",
        "indicador_cobranca_taxa_saida_resgates",
        "taxa_administracao",
        "taxa_maxima_custodia",
        "indicador_taxa_performance",
        "taxa_performance",
        "parametro_taxa_performance",
        "porcentagem_indice_referencia_taxa_performance",
        "valor_cumpom",
        "metodo_calculo_taxa_performance",
        "informacoes_adicionais_taxa_performance",
        "indicador_taxa_ingresso",
        "taxa_ingresso_real",
        "porcentagem_taxa_ingresso",
        "indicador_cobranca_taxa_saida",
        "taxa_saida_real",
        "porcentagem_taxa_saida",
        "indicador_operacoes_derivativos",
        "finalidade_operacoes_derivativos",
        "indicador_operacoes_valor_superior_patrimonio_liquido",
        "fator_limite_total_operacoes_patrimonio_liquido",
        "indicador_contraparte_ligado",
        "indicador_investimentos_exterior",
        "aplicacao_maxima_ativo_exterior",
        "indicador_ativo_credito_privado",
        "aplicacao_maxima_ativo_credito_privado",
        "porcentagem_exposicao_minima_emissor_instituicao_financeira",
        "porcentagem_exposicao_maxima_emissor_instituicao_financeira",
        "porcentagem_exposicao_minima_emissor_companhias_abertas",
        "porcentagem_exposicao_maxima_emissor_companhias_abertas",
        "porcentagem_exposicao_minima_emissor_fundos_investimento",
        "porcentagem_exposicao_maxima_emissor_fundos_investimento",
        "porcentagem_exposicao_minima_emissor_uniao_federal",
        "porcentagem_exposicao_maxima_emissor_uniao_federal",
        "porcentagem_exposicao_minima_emissor_adm_gestor_pessoas_ligadas",
        "porcentagem_exposicao_maxima_emissor_adm_gestor_pessoas_ligadas",
        "porcentagem_exposicao_minima_emissor_outros",
        "porcentagem_exposicao_maxima_emissor_outros",
        "porcentagem_exposicao_minima_cotas_fi",
        "porcentagem_exposicao_maxima_cotas_fi",
        "porcentagem_exposicao_minima_cotas_fic",
        "porcentagem_exposicao_maxima_cotas_fic",
        "porcentagem_exposicao_minima_cotas_fi_qualificados",
        "porcentagem_exposicao_maxima_cotas_fi_qualificados",
        "porcentagem_exposicao_minima_cotas_fic_qualificados",
        "porcentagem_exposicao_maxima_cotas_fic_qualificados",
        "porcentagem_exposicao_minima_cotas_fi_profissionais",
        "porcentagem_exposicao_maxima_cotas_fi_profissionais",
        "porcentagem_exposicao_minima_cotas_fic_profissionais",
        "porcentagem_exposicao_maxima_cotas_fic_profissionais",
        "porcentagem_exposicao_minima_cotas_fii",
        "porcentagem_exposicao_maxima_cotas_fii",
        "porcentagem_exposicao_minima_cotas_fidc",
        "porcentagem_exposicao_maxima_cotas_fidc",
        "porcentagem_exposicao_minima_cotas_ficfidc",
        "porcentagem_exposicao_maxima_cotas_ficfidc",
        "porcentagem_exposicao_minima_cotas_fidic_np",
        "porcentagem_exposicao_maxima_cotas_fidic_np",
        "porcentagem_exposicao_minima_cotas_ficfidic_np",
        "porcentagem_exposicao_maxima_cotas_ficfidc_np",
        "porcentagem_exposicao_minima_cotas_etf",
        "porcentagem_exposicao_maxima_cotas_etf",
        "porcentagem_exposicao_minima_cota_cri",
        "porcentagem_exposicao_maxima_cota_cri",
        "porcentagem_exposicao_minima_titulos_publicos_operacoes_comprimessadas",
        "porcentagem_exposicao_maxima_titulos_publicos_operacoes_comprimessadas",
        "porcentagem_exposicao_minima_ouro",
        "porcentagem_exposicao_maxima_ouro",
        "porcentagem_exposicao_minima_titulos_instituicao_financeira_bacen",
        "porcentagem_exposicao_maxima_titulos_instituicao_financeira_bacen",
        "porcentagem_exposicao_minima_valores_mobiliarios",
        "porcentagem_exposicao_maxima_valores_mobiliarios",
        "porcentagem_exposicao_minima_acoes",
        "porcentagem_exposicao_maxima_acoes",
        "porcentagem_exposicao_minima_debenture",
        "porcentagem_exposicao_maxima_debenture",
        "porcentagem_exposicao_minima_notas_promissorias",
        "porcentagem_exposicao_maxima_notas_promissorias",
        "porcentagem_exposicao_minima_operacoes_compromissadas_titulos_credito_privado",
        "porcentagem_exposicao_maxima_operacoes_compromissadas_titulos_credito_privado",
        "porcentagem_exposicao_minima_derivativos",
        "porcentagem_exposicao_maxima_derivativos",
        "porcentagem_exposicao_minima_outros",
        "porcentagem_exposicao_maxima_outros",
        "porcentagem_exposicao_minima_cotas_fmiee",
        "porcentagem_exposicao_maxima_cotas_fmiee",
        "porcentagem_exposicao_minima_cotas_fip",
        "porcentagem_exposicao_maxima_cotas_fip",
        "porcentagem_exposicao_minima_cotas_ficfip",
        "porcentagem_exposicao_maxima_cotas_ficfip",
        "ano",
        "mes",
    ]

    COLUNAS_MAPEAMENTO_EXT = [
        "NEGOC_MERC",
        "REG_ANBIMA",
        "RESULT_CART_INCORP_PL",
        "FUNDO_COTAS",
        "FUNDO_ESPELHO",
        "ATUALIZ_DIARIA_COTA",
        "TAXA_SAIDA_PAGTO_RESGATE",
        "EXISTE_TAXA_PERFM",
        "EXISTE_TAXA_INGRESSO",
        "EXISTE_TAXA_SAIDA",
        "OPER_DERIV",
        "OPER_VL_SUPERIOR_PL",
        "CONTRAP_LIGADO",
        "INVEST_EXTERIOR",
        "ATIVO_CRED_PRIV",
    ]

    ARQUITETURA_URL_EXT = "https://docs.google.com/spreadsheets/d/1b94RdASfwXMgJuVMhFeY6Xbph4n4_MVt/edit#gid=1045172528"

    URL_EXT = "https://dados.cvm.gov.br/dados/FI/DOC/EXTRATO/DADOS/"

    FILE_EXT = "extrato_fi.csv"

    URL_PERFIL_MENSAL = "https://dados.cvm.gov.br/dados/FI/DOC/PERFIL_MENSAL/DADOS/"

    ARQUITETURA_URL_PERFIL_MENSAL = "https://docs.google.com/spreadsheets/d/1IN7enHe6K-StD_sDfCaqhJ7XfUNXe-W8/edit#gid=1045172528"

    CSV_LIST = [
        "https://dados.cvm.gov.br/dados/FI/DOC/EXTRATO/DADOS/",
        "https://dados.cvm.gov.br/dados/FI/DOC/PERFIL_MENSAL/DADOS/",
    ]

    COLUNAS_ASCI_PERFIL_MENSAL = [
        "cnpj",
        "denominacao_social",
        "versao",
        "resumo_voto_adminstrador_assembleia",
        "justificativa_voto_administrador_assembleia",
        "tipo_modelos_valor_em_risco",
        "resumo_deliberacoes_aprovadas_assembleia",
        "fator_primitivo_risco",
        "cenario_fator_primitivo_risco_ibovespa",
        "cenario_fator_primitivo_risco_juros",
        "cenario_fator_primitivo_cupom_cambial",
        "cenario_fator_primitivo_dolar",
        "cenario_fator_primitivo_outros",
        "fator_risco_outros",
        "fator_risco_nocional",
        "tipo_pessoa_comitente_1",
        "cpf_cnpj_comitente_1",
        "tipo_pessoa_comitente_2",
        "cpf_cnpj_comitente_2",
        "tipo_pessoa_comitente_3",
        "cpf_cnpj_comitente_3",
        "tipo_pessoa_emissor_1",
        "cpf_cnpj_emissor_1",
        "tipo_pessoa_emissor_2",
        "cpf_cnpj_emissor_2",
        "tipo_pessoa_emissor_3",
        "cpf_cnpj_emissor_3",
    ]

    URL_INFO_CADASTRAL = "https://dados.cvm.gov.br/dados/FI/CAD/DADOS/"

    CAD_FILE = "cad_fi.csv"

    ARQUITETURA_URL_CAD = "https://docs.google.com/spreadsheets/d/1OdPdDRnZ9sh3tEdSUo64wsSxVuf5atHP/edit#gid=1045172528"

    COLUNAS_ASCI_CAD = [
        "id_fundo",
        "cnpj",
        "denominacao_social",
        "codigo_cvm",
        "situacao",
        "classe",
        "tipo_rentabilidade",
        "tipo_condominio",
        "informacoes_adicionais_taxa_performance",
        "informacoes_adicionais_taxa_administracao",
        "nome_diretor",
        "cnpj_administrador",
        "nome_administrador",
        "indicador_pessoa_fisica_ou_juridica",
        "cpf_cnpj_gestor",
        "nome_gestor",
        "cnpj_auditor",
        "nome_auditor",
        "cnpj_custodiante",
        "nome_custodiante",
        "cnpj_controlador",
        "nome_controlador",
    ]

    URL_BALANCETE = "https://dados.cvm.gov.br/dados/FI/DOC/BALANCETE/DADOS/HIST/"

    ARQUITETURA_URL_BALANCETE = "https://docs.google.com/spreadsheets/d/1eIMo_hYHy89oh6kHRN9Kh0NytUZzr8__/edit#gid=1045172528"

    DICIONARO_DOCUMENTOS_BALANCETE = {
        'TP_FUNDO_CLASSE': 'tipo_fundo',
        'CNPJ_FUNDO': 'cnpj',
        'DT_COMPTC' : 'data_competencia',
        'CNPJ_FUNDO_CLASSE': 'cnpj',
        'PLANO_CONTA_BALCTE' : 'plano_contabil_balancete',
        'CD_CONTA_BALCTE': 'codigo_conta',
        'VL_SALDO_BALCTE': 'valor_saldo',
    }


    ARQUITETURA_URL_CDA = "https://docs.google.com/spreadsheets/d/1V2XHBXBB_biC0cLoMZ3FxtbC7CPLxQXZhIY7iJDtsSw/edit#gid=0"
