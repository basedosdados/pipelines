"""
Constantes de br_bcb_sicor, um Enum por tabela (`constants_<tabela>`). O
contexto da fonte e as decisões de modelagem estão no README do conjunto.
"""

from enum import Enum


class constants_operacao(Enum):
    """Config da tabela operacao."""

    DATASET_ID = "br_bcb_sicor"
    TABLE_ID = "operacao"
    URL = "https://www.bcb.gov.br/estabilidadefinanceira/tabelas-credito-rural-proagro"
    OUTPUT_FOLDER = "output/br_bcb_sicor"
    TABLE_RAW_NAME = "OPERACAO_BASICA_ESTADO"
    TABLE_SCHEMA = {
        "#REF_BACEN": "id_referencia_bacen",
        "NU_ORDEM": "numero_ordem",
        "CD_ESTADO": "sigla_uf",
        "CD_CATEG_EMITENTE": "id_categoria_emitente",
        "CD_EMPREENDIMENTO": "id_empreendimento",
        "CD_FASE_CICLO_PRODUCAO": "id_fase_ciclo_producao",
        "CD_FONTE_RECURSO": "id_fonte_recurso",
        "CD_INST_CREDITO": "id_instrumento_credito",
        "CD_PROGRAMA": "id_programa",
        "CD_REF_BACEN_INVESTIMENTO": "id_referencia_bacen_investimento",
        "CD_SUBPROGRAMA": "id_subprograma",
        "CD_TIPO_AGRICULTURA": "id_tipo_agricultura",
        "CD_TIPO_CULTIVO": "id_tipo_cultivo",
        "CD_TIPO_SOLO": "id_tipo_solo",
        "CD_CICLO_CULTIVAR": "id_tipo_ciclo_cultivar",
        "CD_TIPO_ENCARG_FINANC": "id_tipo_encargo_financeiro",
        "CD_TIPO_GRAO_SEMENTE": "id_tipo_grao_semente",
        "CD_TIPO_INTGR_CONSOR": "id_tipo_integracao_consorcio",
        "CD_TIPO_IRRIGACAO": "id_tipo_irrigacao",
        "CD_TIPO_SEGURO": "id_tipo_seguro",
        "CNPJ_AGENTE_INVEST": "cnpj_agente_investimento",
        "CNPJ_IF": "cnpj_basico_instituicao_financeira",
        "DT_EMISSAO": "data_emissao",
        "PC_BONUS_CAR": "percentual_bonus_car",
        "DT_VENCIMENTO": "data_vencimento",
        "DT_FIM_COLHEITA": "data_fim_colheita",
        "DT_FIM_PLANTIO": "data_fim_plantio",
        "DT_INIC_COLHEITA": "data_inicio_colheita",
        "DT_INIC_PLANTIO": "data_inicio_plantio",
        "VL_AREA_FINANC": "area_financiada",
        "VL_ALIQ_PROAGRO": "valor_aliquota_proagro",
        "VL_PARC_CREDITO": "valor_parcela_credito",
        "VL_PRESTACAO_INVESTIMENTO": "valor_prestacao_investimento",
        "VL_REC_PROPRIO": "valor_recurso_proprio",
        "VL_RECEITA_BRUTA_ESPERADA": "valor_receita_bruta_esperada",
        "VL_REC_PROPRIO_SRV": "valor_recurso_proprio_srv",
        "VL_QUANTIDADE": "valor_quantidade_itens_financiados",
        "VL_PRODUTIV_OBTIDA": "valor_produtividade_obtida",
        "VL_PREV_PROD": "valor_previsao_producao",
        "VL_JUROS": "taxa_juro",
        "VL_AREA_INFORMADA": "area_informada",
        "VL_JUROS_ENC_FINAN_POSFIX": "taxa_juro_encargo_financeiro_posfixado",
        "VL_PERC_RISCO_STN": "valor_percentual_risco_stn",
        "VL_PERC_CUSTO_EFET_TOTAL": "valor_percentual_custo_efetivo_total",
        "VL_PERC_RISCO_FUNDO_CONST": "valor_percentual_risco_fundo_constitucional",
        "CD_CONTRATO_STN": "id_contrato_sistema_tesouro_nacional",
        "CD_CNPJ_CADASTRANTE": "cnpj_cadastrante",
    }


class constants_saldo(Enum):
    """Config da tabela saldo."""

    DATASET_ID = "br_bcb_sicor"
    TABLE_ID = "saldo"
    URL = "https://www.bcb.gov.br/estabilidadefinanceira/tabelas-credito-rural-proagro"
    OUTPUT_FOLDER = "output/br_bcb_sicor"
    TABLE_RAW_NAME = "SALDOS"
    TABLE_SCHEMA = {
        "#REF_BACEN": "id_referencia_bacen",
        "NU_ORDEM": "numero_ordem",
        "CD_SITUACAO_OPERACAO": "id_situacao_operacao",
        "ANO_BASE": "ano_base",
        "MES_BASE": "mes_base",
        "VL_MEDIO_DIARIO": "valor_medio_diario",
        "VL_MEDIO_DIARIO_VINCENDO": "valor_medio_diario_vincendo",
        "VL_ULTIMO_DIA": "valor_ultimo_dia",
        "IB_RENEGOCIADA": "indicador_renegociacao",
    }


class constants_liberacao(Enum):
    """Config da tabela liberacao."""

    DATASET_ID = "br_bcb_sicor"
    TABLE_ID = "liberacao"
    URL = "https://www.bcb.gov.br/estabilidadefinanceira/tabelas-credito-rural-proagro"
    OUTPUT_FOLDER = "output/br_bcb_sicor"
    TABLE_RAW_NAME = "LIBERACAO_RECURSOS"
    TABLE_SCHEMA = {
        "#LIR_DT_LIBERACAO": "data_liberacao",
        "LIR_VL_LIBERADO": "valor_liberado",
        "REF_BACEN": "id_referencia_bacen",
        "NU_ORDEM": "numero_ordem",
    }


class constants_recurso_publico_complemento_operacao(Enum):
    """Config da tabela recurso_publico_complemento_operacao."""

    DATASET_ID = "br_bcb_sicor"
    TABLE_ID = "recurso_publico_complemento_operacao"
    URL = "https://www.bcb.gov.br/estabilidadefinanceira/tabelas-credito-rural-proagro"
    OUTPUT_FOLDER = "output/br_bcb_sicor"
    TABLE_RAW_NAME = "SICOR_COMPLEMENTO_OPERACAO_BASICA"
    TABLE_SCHEMA = {
        "#REF_BACEN": "id_referencia_bacen",
        "NU_ORDEM": "numero_ordem",
        "CD_IBGE_MUNICIPIO": "id_municipio",
        "REF_BACEN_EFETIVO": "id_referencia_bacen_efetivo",
        "AGENCIA_IF": "id_agencia",
        "NUM_CEDULA_IF": "numero_cedula",
    }


class constants_recurso_publico_cooperado(Enum):
    """Config da tabela recurso_publico_cooperado."""

    DATASET_ID = "br_bcb_sicor"
    TABLE_ID = "recurso_publico_cooperado"
    URL = "https://www.bcb.gov.br/estabilidadefinanceira/tabelas-credito-rural-proagro"
    OUTPUT_FOLDER = "output/br_bcb_sicor"
    TABLE_RAW_NAME = "SICOR_LISTA_COOPERADOS"
    TABLE_SCHEMA = {
        "#REF_BACEN": "id_referencia_bacen",
        "NU_ORDEM": "numero_ordem",
        "CD_PROGRAMA": "id_programa",
        "CPF_CNPJ": "tipo_cpf_cnpj",
        "TIPO_PESSOA": "tipo_pessoa",
        "VL_PARCELA": "valor_parcela",
    }


class constants_recurso_publico_gleba(Enum):
    """Config da tabela recurso_publico_gleba."""

    DATASET_ID = "br_bcb_sicor"
    TABLE_ID = "recurso_publico_gleba"
    URL = "https://www.bcb.gov.br/estabilidadefinanceira/tabelas-credito-rural-proagro"
    OUTPUT_FOLDER = "output/br_bcb_sicor"
    TABLE_RAW_NAME = "sicor_glebas_wkt"
    TABLE_SCHEMA = {
        "#REF_BACEN": "id_referencia_bacen",
        "NU_ORDEM": "numero_ordem",
        "NU_INDICE": "indice_gleba",
        "GT_GEOMETRIA": "geometria",
    }


class constants_recurso_publico_mutuario(Enum):
    """Config da tabela recurso_publico_mutuario."""

    DATASET_ID = "br_bcb_sicor"
    TABLE_ID = "recurso_publico_mutuario"
    URL = "https://www.bcb.gov.br/estabilidadefinanceira/tabelas-credito-rural-proagro"
    OUTPUT_FOLDER = "output/br_bcb_sicor"
    TABLE_RAW_NAME = "SICOR_MUTUARIOS"
    TABLE_SCHEMA = {
        "REF_BACEN": "id_referencia_bacen",
        "CD_PRIMEIRO": "primeiro_mutuario",
        "#CD_SEXO": "sexo",
        "CD_CPF_CNPJ": "tipo_cpf_cnpj",
        "CD_TIPO_BENEFICIARIO": "tipo_beneficiario",
        "CD_DAP": "id_dap",
    }


class constants_recurso_publico_propriedade(Enum):
    """Config da tabela recurso_publico_propriedade."""

    DATASET_ID = "br_bcb_sicor"
    TABLE_ID = "recurso_publico_propriedade"
    URL = "https://www.bcb.gov.br/estabilidadefinanceira/tabelas-credito-rural-proagro"
    OUTPUT_FOLDER = "output/br_bcb_sicor"
    TABLE_RAW_NAME = "SICOR_PROPRIEDADES"
    TABLE_SCHEMA = {
        "#REF_BACEN": "id_referencia_bacen",
        "NU_ORDEM": "numero_ordem",
        "CD_CNPJ_CPF": "tipo_cpf_cnpj",
        "CD_SNCR": "id_sncr",
        "CD_CIB": "id_cib",
        "CD_CAR": "id_car",
    }


class constants_operacoes_desclassificadas(Enum):
    """Config da tabela operacoes_desclassificadas."""

    DATASET_ID = "br_bcb_sicor"
    TABLE_ID = "operacoes_desclassificadas"
    URL = "https://www.bcb.gov.br/estabilidadefinanceira/tabelas-credito-rural-proagro"
    OUTPUT_FOLDER = "output/br_bcb_sicor"
    TABLE_RAW_NAME = "SICOR_DESCLASSIFICACAO"
    TABLE_SCHEMA = {
        "#REF_BACEN": "id_referencia_bacen",
        "NU_ORDEM": "numero_ordem",
        "DT_DESC": "data_desclassificacao",
        "CD_MOTIVO_DESC": "id_motivo_desclassificacao",
        "VL_DESC": "valor_desclassificado",
        "TIPO_DESC": "tipo_desclassificacao",
    }


class constants_proagro_cop(Enum):
    """Config da tabela proagro_cop."""

    DATASET_ID = "br_bcb_sicor"
    TABLE_ID = "proagro_cop"
    URL = "https://www.bcb.gov.br/estabilidadefinanceira/tabelas-credito-rural-proagro"
    OUTPUT_FOLDER = "output/br_bcb_sicor"
    TABLE_RAW_NAME = "SICOR_COP_BASICO"
    TABLE_SCHEMA = {
        "#REF_BACEN": "id_referencia_bacen",
        "NU_ORDEM": "numero_ordem",
        "DT_COMUNICACAO": "data_comunicacao",
        "DT_FIM_COLHEITA": "data_fim_colheita",
        "DT_FIM_PLANTIO": "data_fim_plantio",
        "DT_INICIO_COLHEITA": "data_inicio_colheita",
        "DT_INICIO_PLANTIO": "data_inicio_plantio",
        "CD_STATUS": "id_status",
        "CD_CICLO_CULTIVAR": "id_tipo_ciclo_cultivar",
        "CD_TIPO_SOLO": "id_tipo_solo",
        "CD_EVENTO": "id_evento",
    }


class constants_proagro_complemento_cop(Enum):
    """Config da tabela proagro_complemento_cop."""

    DATASET_ID = "br_bcb_sicor"
    TABLE_ID = "proagro_complemento_cop"
    URL = "https://www.bcb.gov.br/estabilidadefinanceira/tabelas-credito-rural-proagro"
    OUTPUT_FOLDER = "output/br_bcb_sicor"
    TABLE_RAW_NAME = "SICOR_COMPLEMENTO_COP"
    TABLE_SCHEMA = {
        "#REF_BACEN": "id_referencia_bacen",
        "NU_ORDEM": "numero_ordem",
        "CD_CPF_CNPJ_PERICIADORA": "tipo_cpf_cnpj_periciadora",
        "CD_EVENTO": "id_evento",
        "CD_CPF_PERITO": "cpf_perito",
    }


class constants_proagro_rcp(Enum):
    """Config da tabela proagro_rcp."""

    DATASET_ID = "br_bcb_sicor"
    TABLE_ID = "proagro_rcp"
    URL = "https://www.bcb.gov.br/estabilidadefinanceira/tabelas-credito-rural-proagro"
    OUTPUT_FOLDER = "output/br_bcb_sicor"
    TABLE_RAW_NAME = "SICOR_RCP_BASICO"
    TABLE_SCHEMA = {
        "#REF_BACEN": "id_referencia_bacen",
        "NU_ORDEM": "numero_ordem",
        "DT_ENTREGA": "data_entrega",
        "DT_FIM_COLHEITA": "data_fim_colheita",
        "DT_FIM_EVENTO": "data_fim_evento",
        "DT_FIM_PLANTIO": "data_fim_plantio",
        "DT_INICIO_COLHEITA": "data_inicio_colheita",
        "DT_INICIO_EVENTO": "data_inicio_evento",
        "DT_INICIO_PLANTIO": "data_inicio_plantio",
        "VL_AREA": "area",
        "VL_PREV_PROD": "valor_previsao_producao",
        "VL_REC_PREV": "valor_receita_prevista",
        "CD_STATUS": "id_status",
        "CD_TIPO": "id_tipo",
        "CD_EVENTO": "id_evento",
        "DT_VISITA": "data_visita",
        "NU_DIAS_CICLO_CULTIVAR": "quantidade_dias_ciclo_cultivar",
    }


class constants_proagro_complemento_rcp(Enum):
    """Config da tabela proagro_complemento_rcp."""

    DATASET_ID = "br_bcb_sicor"
    TABLE_ID = "proagro_complemento_rcp"
    URL = "https://www.bcb.gov.br/estabilidadefinanceira/tabelas-credito-rural-proagro"
    OUTPUT_FOLDER = "output/br_bcb_sicor"
    TABLE_RAW_NAME = "SICOR_COMPLEMENTO_RCP"
    TABLE_SCHEMA = {
        "#REF_BACEN": "id_referencia_bacen",
        "NU_ORDEM": "numero_ordem",
        "CD_CPF_CNPJ_PERICIADORA": "tipo_cpf_cnpj_periciadora",
        "CD_CPF_PERITO": "cpf_perito",
    }


class constants_proagro_rcp_gleba(Enum):
    """Config da tabela proagro_rcp_gleba."""

    DATASET_ID = "br_bcb_sicor"
    TABLE_ID = "proagro_rcp_gleba"
    URL = "https://www.bcb.gov.br/estabilidadefinanceira/tabelas-credito-rural-proagro"
    OUTPUT_FOLDER = "output/br_bcb_sicor"
    TABLE_RAW_NAME = "SICOR_RCP_GLEBAS"
    TABLE_SCHEMA = {
        "#REF_BACEN": "id_referencia_bacen",
        "NU_ORDEM": "numero_ordem",
        "NU_INDICE": "indice_gleba",
        "GT_GEOMETRIA": "geometria",
    }


class constants_proagro_parcela(Enum):
    """Config da tabela proagro_parcela."""

    DATASET_ID = "br_bcb_sicor"
    TABLE_ID = "proagro_parcela"
    URL = "https://www.bcb.gov.br/estabilidadefinanceira/tabelas-credito-rural-proagro"
    OUTPUT_FOLDER = "output/br_bcb_sicor"
    TABLE_RAW_NAME = "SICOR_PARCELAS_PROAGRO"
    TABLE_SCHEMA = {
        "#REF_BACEN": "id_referencia_bacen",
        "NU_ORDEM": "numero_ordem",
        "DT_BASE": "data_base",
        "DT_PAGAMENTO": "data_pagamento",
        "CD_INSTANCIA": "id_instancia",
        "CD_STATUS": "id_status",
        "CD_NATUREZA_PARCELA": "id_natureza_parcela",
        "DT_ATUALIZACAO": "data_atualizacao",
        "VL_ATUAL": "valor_atual",
        "VL_BASE": "valor_base",
        "VL_PAGO": "valor_pago",
        "VL_IMPOSTO": "valor_imposto",
        "DT_REMESSA": "data_remessa",
    }


class constants_proagro_sumula_julgamento(Enum):
    """Config da tabela proagro_sumula_julgamento."""

    DATASET_ID = "br_bcb_sicor"
    TABLE_ID = "proagro_sumula_julgamento"
    URL = "https://www.bcb.gov.br/estabilidadefinanceira/tabelas-credito-rural-proagro"
    OUTPUT_FOLDER = "output/br_bcb_sicor"
    TABLE_RAW_NAME = "SICOR_SUMULA_JULGAMENTO"
    TABLE_SCHEMA = {
        "#REF_BACEN": "id_referencia_bacen",
        "NU_ORDEM": "numero_ordem",
        "VL_COB_ANT_PARCELA_INVEST_PROAGRO_MAIS": "valor_cobertura_anterior_parcela_investimento_proagro_mais",
        "VL_REMU_ENCARR_COMPROV_PERDAS": "valor_remuneracao_encarregado_comprovacao_perdas",
        "CD_STATUS": "id_status",
        "VL_RECEITAS_CONSIDERADAS": "valor_receitas_consideradas",
        "VL_COBERTURA_ANT_REC_PROPRIOS": "valor_cobertura_anterior_recurso_proprio",
        "VL_DEMAIS_DESPESAS_COMPROV_PERD": "valor_demais_despesas_comprovacao_perdas",
        "VL_CRED_CUSTEIO_USADO": "valor_credito_custeio_utilizado",
        "VL_DEMAIS_DESP_ANT_COMP_PER": "valor_demais_despesas_anteriores_comprovacao_perdas",
        "VL_REC_PROP_USADO": "valor_recurso_proprio_utilizado",
        "VL_COB_ANT_GARANTIA_RENDA_MIN": "valor_cobertura_anterior_garantia_renda_minima",
        "CD_INSTANCIA": "id_instancia",
        "VL_PERDAS_NAO_AMPARADAS": "valor_perdas_nao_amparadas",
        "VL_ENCARGOS_SOB_CREDITO": "valor_encargos_credito_utilizado",
        "DT_INCLUSAO": "data_inclusao",
        "VL_PERC_REDUTOR_COBERTURA": "percentual_redutor_cobertura",
        "VL_REMU_ANT_ENCARG_COMP_PERDAS": "valor_remuneracao_anterior_encarregado_comprovacao_perdas",
        "CD_DECISAO": "id_decisao",
        "DT_DECISAO": "data_decisao",
        "DT_BASE": "data_base",
        "VL_COBERTURA_ANT_CREDITO_CUSTEIO": "valor_cobertura_anterior_credito_custeio",
        "VL_BONUS_PGPAF": "valor_bonus_pgpaf",
        "VL_DEDUCOES_LEGAIS": "valor_deducoes_legais",
        "VL_ORCAMENTO_ENQUADRADO": "valor_orcamento_enquadrado",
        "NU_DIAS_UTEIS_ATRASO_PERITO": "quantidade_dias_uteis_atraso_perito",
        "IB_SEGUNDA_VISTORIA": "indicador_segunda_vistoria",
    }


class constants_instituicao_financeira(Enum):
    """Config da tabela instituicao_financeira."""

    DATASET_ID = "br_bcb_sicor"
    TABLE_ID = "instituicao_financeira"
    URL = "https://www.bcb.gov.br/estabilidadefinanceira/tabelas-credito-rural-proagro"
    OUTPUT_FOLDER = "output/br_bcb_sicor"
    TABLE_RAW_NAME = "SICOR_LISTA_IFS"
    TABLE_SCHEMA = {
        "#CNPJ_IF": "cnpj_basico",
        "NOME_IF": "nome",
        "SEGMENTO_IF": "segmento",
    }


class constants_fonte_recurso(Enum):
    """Config da tabela fonte_recurso."""

    DATASET_ID = "br_bcb_sicor"
    TABLE_ID = "fonte_recurso"
    URL = "https://www.bcb.gov.br/estabilidadefinanceira/tabelas-credito-rural-proagro"
    OUTPUT_FOLDER = "output/br_bcb_sicor"
    SOURCE_URLS = [
        "https://www.bcb.gov.br/htms/sicor/FonteRecursos.csv",
        "https://www.bcb.gov.br/htms/sicor/FonteRecursosPublicos.csv",
    ]
    TABLE_RAW_NAME = "FonteRecursos"
    TABLE_SCHEMA = {
        "#CODIGO": "id_fonte_recurso",
        "DESCRICAO": "descricao",
        "DATA_INICIO": "data_inicio",
        "DATA_FIM": "data_fim",
    }
    TABLE_SCHEMA_RECURSO_PUBLICO = {
        "#CODIGO": "id_fonte_recurso",
        "DESCRICAO": "descricao",
    }


class constants_empreendimento(Enum):
    """Config da tabela empreendimento."""

    DATASET_ID = "br_bcb_sicor"
    TABLE_ID = "empreendimento"
    URL = "https://www.bcb.gov.br/estabilidadefinanceira/tabelas-credito-rural-proagro"
    OUTPUT_FOLDER = "output/br_bcb_sicor"
    SOURCE_URLS = ["https://www.bcb.gov.br/htms/sicor/Empreendimento.csv"]
    TABLE_RAW_NAME = "Empreendimento"
    TABLE_SCHEMA = {
        "#CODIGO": "id_empreendimento",
        "DATA_INICIO": "data_inicio",
        "DATA_FIM": "data_fim",
        "FINALIDADE": "finalidade",
        "ATIVIDADE": "atividade",
        "MODALIDADE": "modalidade",
        "PRODUTO": "produto",
        "VARIEDADE": "variedade",
        "CESTA": "cesta_safra",
        "ZONEAMENTO": "zoneamento",
        "UNIDADE_MEDIDA": "unidade_medida",
        "UNIDADE_MEDIDA_PREVISAO": "unidade_medida_previsao_producao",
        "CONSORCIO": "consorcio",
        "CEDULA_MAE": "cedula_mae",
        "CD_TIPO_CULTURA": "id_tipo_cultura",
    }


class constants_dicionario(Enum):
    """Config da tabela dicionario."""

    DATASET_ID = "br_bcb_sicor"
    TABLE_ID = "dicionario"
    OUTPUT_FOLDER = "output/br_bcb_sicor"
    COLUMN_SOURCES = [
        {
            "id_tabela": "operacao",
            "nome_coluna": "id_categoria_emitente",
            "url": "https://www.bcb.gov.br/htms/sicor/CategoriaEmitente.csv",
            "colunas": {"#CODIGO": "chave", "DESCRICAO": "valor"},
            "sep": ";",
        },
        {
            "id_tabela": "operacao",
            "nome_coluna": "id_fase_ciclo_producao",
            "url": "https://www.bcb.gov.br/htms/sicor/FaseCicloProducao.csv",
            "colunas": {"#CODIGO": "chave", "DESCRICAO": "valor"},
            "sep": ";",
        },
        {
            "id_tabela": "operacao",
            "nome_coluna": "id_fonte_recurso",
            "url": "https://www.bcb.gov.br/htms/sicor/FonteRecursos.csv",
            "colunas": {"#CODIGO": "chave", "DESCRICAO": "valor"},
            "sep": ";",
        },
        {
            "id_tabela": "operacao",
            "nome_coluna": "id_instrumento_credito",
            "url": "https://www.bcb.gov.br/htms/sicor/InstrumentoCredito.csv",
            "colunas": {"#CODIGO": "chave", "DESCRICAO": "valor"},
            "sep": ";",
        },
        {
            "id_tabela": "operacao",
            "nome_coluna": "id_programa",
            "url": "https://www.bcb.gov.br/htms/sicor/Programa.csv",
            "colunas": {"#CODIGO": "chave", "DESCRICAO": "valor"},
            "sep": ";",
        },
        {
            "id_tabela": "operacao",
            "nome_coluna": "id_subprograma",
            "url": "https://www.bcb.gov.br/htms/sicor/Subprogramas.csv",
            "colunas": {
                "#CODIGO_SUBPROGRAMA": "chave",
                "DESCRICAO_SUBPROGRAMA": "valor",
            },
            "sep": ";",
        },
        {
            "id_tabela": "operacao",
            "nome_coluna": "id_tipo_agricultura",
            "url": "https://www.bcb.gov.br/htms/sicor/TipoAgropecuaria.csv",
            "colunas": {"#CODIGO": "chave", "DESCRICAO": "valor"},
            "sep": ";",
        },
        {
            "id_tabela": "operacao",
            "nome_coluna": "id_tipo_cultivo",
            "url": "https://www.bcb.gov.br/htms/sicor/TipoCultivo.csv",
            "colunas": {"#CODIGO": "chave", "DESCRICAO": "valor"},
            "sep": ";",
        },
        {
            "id_tabela": "operacao",
            "nome_coluna": "id_tipo_solo",
            "url": "https://www.bcb.gov.br/htms/sicor/TipoSoloProagro.csv",
            "colunas": {
                "#CODIGO_TIPO_SOLO": "chave",
                "DESCRICAO_TIPO_SOLO": "valor",
            },
            "sep": ";",
        },
        {
            "id_tabela": "operacao",
            "nome_coluna": "id_tipo_ciclo_cultivar",
            "url": "https://www.bcb.gov.br/htms/sicor/CicloCultivarProagro.csv",
            "colunas": {"#CODIGO_CICLO": "chave", "DESCRICAO_CICLO": "valor"},
            "sep": ";",
        },
        {
            "id_tabela": "operacao",
            "nome_coluna": "id_tipo_encargo_financeiro",
            "url": "https://www.bcb.gov.br/htms/sicor/EncargosFinanceirosComplementares.csv",
            "colunas": {"#CODIGO": "chave", "DESCRICAO": "valor"},
            "sep": ",",
        },
        {
            "id_tabela": "operacao",
            "nome_coluna": "id_tipo_grao_semente",
            "url": "https://www.bcb.gov.br/htms/sicor/GraoSemente.csv",
            "colunas": {"#CODIGO": "chave", "DESCRICAO": "valor"},
            "sep": ",",
        },
        {
            "id_tabela": "operacao",
            "nome_coluna": "id_tipo_integracao_consorcio",
            "url": "https://www.bcb.gov.br/htms/sicor/TipoIntegracao.csv",
            "colunas": {"#CODIGO": "chave", "DESCRICAO": "valor"},
            "sep": ";",
        },
        {
            "id_tabela": "operacao",
            "nome_coluna": "id_tipo_irrigacao",
            "url": "https://www.bcb.gov.br/htms/sicor/TipoIrrigacao.csv",
            "colunas": {"#CODIGO": "chave", "DESCRICAO": "valor"},
            "sep": ";",
        },
        {
            "id_tabela": "operacao",
            "nome_coluna": "id_tipo_seguro",
            "url": "https://www.bcb.gov.br/htms/sicor/TipoGarantiaEmpreendimento.csv",
            "colunas": {"#CODIGO": "chave", "DESCRICAO": "valor"},
            "sep": ",",
        },
        {
            "id_tabela": "proagro_cop",
            "nome_coluna": "id_status",
            "url": "https://www.bcb.gov.br/htms/sicor/StatusCOPProagro.csv",
            "colunas": {"#CODIGO": "chave", "DESCRICAO": "valor"},
            "sep": ";",
        },
        {
            "id_tabela": "proagro_cop",
            "nome_coluna": "id_tipo_ciclo_cultivar",
            "url": "https://www.bcb.gov.br/htms/sicor/CicloCultivarProagro.csv",
            "colunas": {"#CODIGO_CICLO": "chave", "DESCRICAO_CICLO": "valor"},
            "sep": ";",
        },
        {
            "id_tabela": "proagro_cop",
            "nome_coluna": "id_tipo_solo",
            "url": "https://www.bcb.gov.br/htms/sicor/TipoSoloProagro.csv",
            "colunas": {
                "#CODIGO_TIPO_SOLO": "chave",
                "DESCRICAO_TIPO_SOLO": "valor",
            },
            "sep": ";",
        },
        {
            "id_tabela": "proagro_cop",
            "nome_coluna": "id_evento",
            "url": "https://www.bcb.gov.br/htms/sicor/EventoProagro.csv",
            "colunas": {"#CODIGO_EVENTO": "chave", "NOME_EVENTO": "valor"},
            "sep": ";",
        },
        {
            "id_tabela": "proagro_complemento_cop",
            "nome_coluna": "id_evento",
            "url": "https://www.bcb.gov.br/htms/sicor/EventoProagro.csv",
            "colunas": {"#CODIGO_EVENTO": "chave", "NOME_EVENTO": "valor"},
            "sep": ";",
        },
        {
            "id_tabela": "proagro_rcp",
            "nome_coluna": "id_evento",
            "url": "https://www.bcb.gov.br/htms/sicor/EventoProagro.csv",
            "colunas": {"#CODIGO_EVENTO": "chave", "NOME_EVENTO": "valor"},
            "sep": ";",
        },
        {
            "id_tabela": "proagro_parcela",
            "nome_coluna": "id_instancia",
            "url": "https://www.bcb.gov.br/htms/sicor/InstanciaProagro.csv",
            "colunas": {"#CODIGO": "chave", "DESCRICAO": "valor"},
            "sep": ";",
        },
        {
            "id_tabela": "proagro_parcela",
            "nome_coluna": "id_status",
            "url": "https://www.bcb.gov.br/htms/sicor/StatusParcelaProagro.csv",
            "colunas": {"#CODIGO": "chave", "DESCRICAO": "valor"},
            "sep": ";",
        },
        {
            "id_tabela": "proagro_parcela",
            "nome_coluna": "id_natureza_parcela",
            "url": "https://www.bcb.gov.br/htms/sicor/NaturezaProagro.csv",
            "colunas": {
                "#CODIGO_NATUREZA": "chave",
                "DESCRICAO_NATUREZA": "valor",
            },
            "sep": ";",
        },
        {
            "id_tabela": "proagro_sumula_julgamento",
            "nome_coluna": "id_instancia",
            "url": "https://www.bcb.gov.br/htms/sicor/InstanciaProagro.csv",
            "colunas": {"#CODIGO": "chave", "DESCRICAO": "valor"},
            "sep": ";",
        },
        {
            "id_tabela": "saldo",
            "nome_coluna": "id_situacao_operacao",
            "url": "https://www.bcb.gov.br/htms/sicor/SituacaoOperacao.csv",
            "colunas": {"#CODIGO": "chave", "DESCRICAO": "valor"},
            "sep": ",",
        },
        {
            "id_tabela": "empreendimento",
            "nome_coluna": "id_tipo_cultura",
            "url": "https://www.bcb.gov.br/htms/sicor/TipodeCultura.csv",
            "colunas": {"#CODIGO": "chave", "DESCRICAO": "valor"},
            "sep": ",",
        },
        {
            "id_tabela": "operacoes_desclassificadas",
            "nome_coluna": "id_motivo_desclassificacao",
            "url": "https://www.bcb.gov.br/htms/sicor/motivoDesclassificacao.csv",
            "colunas": {"#CODIGO": "chave", "DESCRICAO": "valor"},
            "sep": ",",
        },
        {
            "id_tabela": "recurso_publico_mutuario",
            "nome_coluna": "tipo_beneficiario",
            "url": "https://www.bcb.gov.br/htms/sicor/motivoDesclassificacao.csv",
            "colunas": {"#CODIGO": "chave", "DESCRICAO": "valor"},
            "sep": ",",
        },
        {
            "id_tabela": "recurso_publico_cooperado",
            "nome_coluna": "id_programa",
            "url": "https://www.bcb.gov.br/htms/sicor/Programa.csv",
            "colunas": {"#CODIGO": "chave", "DESCRICAO": "valor"},
            "sep": ";",
        },
    ]


TABLES_CONSTANTS = {
    "operacao": constants_operacao,
    "saldo": constants_saldo,
    "liberacao": constants_liberacao,
    "recurso_publico_complemento_operacao": constants_recurso_publico_complemento_operacao,
    "recurso_publico_cooperado": constants_recurso_publico_cooperado,
    "recurso_publico_gleba": constants_recurso_publico_gleba,
    "recurso_publico_mutuario": constants_recurso_publico_mutuario,
    "recurso_publico_propriedade": constants_recurso_publico_propriedade,
    "operacoes_desclassificadas": constants_operacoes_desclassificadas,
    "proagro_cop": constants_proagro_cop,
    "proagro_complemento_cop": constants_proagro_complemento_cop,
    "proagro_rcp": constants_proagro_rcp,
    "proagro_complemento_rcp": constants_proagro_complemento_rcp,
    "proagro_rcp_gleba": constants_proagro_rcp_gleba,
    "proagro_parcela": constants_proagro_parcela,
    "proagro_sumula_julgamento": constants_proagro_sumula_julgamento,
    "instituicao_financeira": constants_instituicao_financeira,
    "fonte_recurso": constants_fonte_recurso,
    "empreendimento": constants_empreendimento,
}
