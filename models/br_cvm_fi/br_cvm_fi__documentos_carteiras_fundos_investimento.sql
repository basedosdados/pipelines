{{
    config(
        alias="documentos_carteiras_fundos_investimento",
        schema="br_cvm_fi",
        materialized="table",
        partition_by={
            "field": "ano",
            "data_type": "int64",
            "range": {"start": 2005, "end": 2025, "interval": 1},
        },
        cluster_by=["mes", "id_fundo"],
    )
}}
select
    safe_cast(ano as int64) ano,
    safe_cast(mes as int64) mes,
    safe_cast(id_fundo as string) id_fundo,
    safe_cast(bloco as string) bloco,
    regexp_replace(cnpj, r'[^0-9]', '') as cnpj,
    substr(regexp_replace(cnpj, r'[^0-9]', ''), 1, 8) as cnpj_basico,
    safe_cast(denominacao_social as string) denominacao_social,
    safe_cast(data_competencia as date) data_competencia,
    safe_cast(tipo_aplicacao as string) tipo_aplicacao,
    safe_cast(tipo_ativo as string) tipo_ativo,
    safe_cast(
        replace(indicador_emissor_ligado, '.0', '') as int64
    ) indicador_emissor_ligado,
    safe_cast(tipo_negociacao as string) tipo_negociacao,
    safe_cast(
        replace(quantidade_vendas_negocios_mes, '.0', '') as int64
    ) quantidade_vendas_negocios_mes,
    safe_cast(valor_vendas_negocios_mes as float64) valor_vendas_negocios_mes,
    safe_cast(
        replace(quantidade_aquisicoes_negocios_mes, '.0', '') as int64
    ) quantidade_aquisicoes_negocios_mes,
    safe_cast(valor_aquisicoes_negocios_mes as float64) valor_aquisicoes_negocios_mes,
    safe_cast(
        replace(quantidade_posicao_final, '.0', '') as int64
    ) quantidade_posicao_final,
    safe_cast(valor_mercado_posicao_final as float64) valor_mercado_posicao_final,
    safe_cast(valor_custo_posicao_final as float64) valor_custo_posicao_final,
    safe_cast(
        prazo_confidencialidae_aplicacao as string
    ) prazo_confidencialidade_aplicacao,
    safe_cast(tipo_titulo_publico as string) tipo_titulo_publico,
    safe_cast(codigo_isin as string) codigo_isin,
    safe_cast(codigo_selic as string) codigo_selic,
    safe_cast(data_emissao as date) data_emissao,
    safe_cast(data_vencimento as date) data_vencimento,
    regexp_replace(cnpj_fundo_investido, r'[^0-9]', '') as cnpj_fundo_investido,
    substr(
        regexp_replace(cnpj_fundo_investido, r'[^0-9]', ''), 1, 8
    ) as cnpj_basico_fundo_investido,
    safe_cast(
        denominacao_social_fundo_investido as string
    ) denominacao_social_fundo_investido,
    safe_cast(codigo_swap as string) codigo_swap,
    safe_cast(descricao_tipo_ativo_swap as string) descricao_tipo_ativo_swap,
    safe_cast(codigo_ativo as string) codigo_ativo,
    safe_cast(descricao_ativo as string) descricao_ativo,
    safe_cast(data_inicio_vigencia as date) data_inicio_vigencia,
    safe_cast(data_fim_vigencia as date) data_fim_vigencia,
    regexp_replace(cnpj_emissor, r'[^0-9]', '') as cnpj_emissor,
    substr(regexp_replace(cnpj_emissor, r'[^0-9]', ''), 1, 8) as cnpj_basico_emissor,
    safe_cast(nome_emissor as string) nome_emissor,
    safe_cast(
        replace(indicador_titulo_pos_fixado, '.0', '') as int64
    ) indicador_titulo_pos_fixado,
    safe_cast(codigo_indexador_pos_fixados as string) codigo_indexador_pos_fixados,
    safe_cast(
        descricao_indexador_pos_fixados as string
    ) descricao_indexador_pos_fixados,
    safe_cast(
        porcentagem_indexador_pos_fixados as float64
    ) porcentagem_indexador_pos_fixados,
    safe_cast(porcentagem_cupom_pos_fixados as float64) porcentagem_cupom_pos_fixados,
    safe_cast(
        porcentagem_taxa_concentrada_pre_fixados as float64
    ) porcentagem_taxa_concentrada_pre_fixados,
    safe_cast(
        indicador_emissor_possui_classificacao_risco as string
    ) indicador_emissor_possui_classificacao_risco,
    safe_cast(
        nome_agencia_classificacao_risco as string
    ) nome_agencia_classificacao_risco,
    safe_cast(data_classificacao_risco as date) data_classificacao_risco,
    safe_cast(grau_risco_atribuido as string) grau_risco_atribuido,
    safe_cast(
        replace(indicador_emissor_pessoa_fisica_juridica, '.0', '') as int64
    ) indicador_emissor_pessoa_fisica_juridica,
    safe_cast(
        replace(
            indicador_codigo_identificacao_emissor_pessoa_fisica_juridica, '.0', ''
        ) as int64
    ) indicador_codigo_identificacao_emissor_pessoa_fisica_juridica,
    safe_cast(
        replace(indicador_titulo_registrado_cetip, '.0', '') as int64
    ) indicador_titulo_registrado_cetip,
    safe_cast(
        replace(indicador_titulo_possui_garantia_seguro, '.0', '') as int64
    ) indicador_titulo_possui_garantia_seguro,
    regexp_replace(
        cnpj_instituicao_financeira_coobrigacao, r'[^0-9]', ''
    ) as cnpj_instituicao_financeira_coobrigacao,
    substr(
        regexp_replace(cnpj_instituicao_financeira_coobrigacao, r'[^0-9]', ''), 1, 8
    ) as cnpj_basico_instituicao_financeira_coobrigacao,
    safe_cast(
        replace(indicador_investimento_coletivo, '.0', '') as int64
    ) indicador_investimento_coletivo,
    safe_cast(
        replace(indicador_gestao_carteira_influencia_gestor, '.0', '') as int64
    ) indicador_gestao_carteira_influencia_gestor,
    safe_cast(codigo_pais as string) codigo_pais,
    safe_cast(nome_pais as string) nome_pais,
    safe_cast(codigo_bolsa_mercado_balcao as string) codigo_bolsa_mercado_balcao,
    safe_cast(tipo_bolsa_mercado_balcao as string) tipo_bolsa_mercado_balcao,
    safe_cast(
        codigo_ativo_bolsa_mercado_balcao_local_aquisicao as string
    ) codigo_ativo_bolsa_mercado_balcao_local_aquisicao,
    safe_cast(descricao_ativo_exterior as string) descricao_ativo_exterior,
    safe_cast(
        replace(quantidade_ativos_exterior, '.0', '') as int64
    ) quantidade_ativos_exterior,
    safe_cast(valor_ativo_exterior as float64) valor_ativo_exterior,
from
    {{
        set_datalake_project(
            "br_cvm_fi_staging.documentos_carteiras_fundos_investimento"
        )
    }} t
