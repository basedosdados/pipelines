{{
    config(
        schema="br_cvm_fi",
        alias="documentos_extratos_informacoes",
        materialized="table",
        partition_by={
            "field": "ano",
            "data_type": "int64",
            "range": {"start": 2015, "end": 2025, "interval": 1},
        },
        cluster_by=["mes", "data_competencia"],
    )
}}
select
    safe_cast(ano as int64) ano,
    safe_cast(mes as int64) mes,
    regexp_replace(cnpj, r'[^0-9]', '') as cnpj,
    substr(regexp_replace(cnpj, r'[^0-9]', ''), 1, 8) as cnpj_basico,
    safe_cast(denominacao_social as string) denominacao_social,
    safe_cast(data_competencia as date) data_competencia,
    safe_cast(condominio as string) condominio,
    safe_cast(indicador_negociacao_mercado as int64) indicador_negociacao_mercado,
    safe_cast(nome_mercado as string) nome_mercado,
    safe_cast(tipo_prazo as string) tipo_prazo,
    safe_cast(prazo as string) prazo,
    safe_cast(publico_alvo as string) publico_alvo,
    safe_cast(indicador_registro_anbima as int64) indicador_registro_anbima,
    safe_cast(classificacao_anbima as string) classificacao_anbima,
    safe_cast(forma_distribuicao as string) forma_distribuicao,
    safe_cast(politica_investimento as string) politica_investimento,
    safe_cast(
        porcentagem_aplicacao_maximo_fundo_ligado as float64
    ) porcentagem_aplicacao_maximo_fundo_ligado,
    safe_cast(
        indicador_resultados_carteira_incorporado_patrimonio_liquido as int64
    ) indicador_resultados_carteira_incorporado_patrimonio_liquido,
    safe_cast(indicador_fundo_cotas as int64) indicador_fundo_cotas,
    safe_cast(indicador_fundo_espelho as int64) indicador_fundo_espelho,
    safe_cast(aplicacao_minima as float64) aplicacao_minima,
    safe_cast(
        indicador_atualizacao_diaria_cota as int64
    ) indicador_atualizacao_diaria_cota,
    safe_cast(prazo_atualizacao_valor_cota as string) prazo_atualizacao_valor_cota,
    safe_cast(cota_emissao as string) cota_emissao,
    safe_cast(patrimonio_liquido_cota as string) patrimonio_liquido_cota,
    safe_cast(quantidade_dias_conversao_cota as int64) quantidade_dias_conversao_cota,
    safe_cast(quantidade_dias_pagamento_cota as int64) quantidade_dias_pagamento_cota,
    safe_cast(
        quantidade_dias_carencia_resgate_cotas as int64
    ) quantidade_dias_carencia_resgate_cotas,
    safe_cast(
        quantidade_dias_pagamento_resgates as int64
    ) quantidade_dias_pagamento_resgates,
    safe_cast(tipo_prazo_pagamento_resgates as string) tipo_prazo_pagamento_resgates,
    safe_cast(
        indicador_cobranca_taxa_saida_resgates as int64
    ) indicador_cobranca_taxa_saida_resgates,
    safe_cast(taxa_administracao as float64) taxa_administracao,
    safe_cast(taxa_maxima_custodia as float64) taxa_maxima_custodia,
    safe_cast(indicador_taxa_performance as int64) indicador_taxa_performance,
    safe_cast(taxa_performance as float64) taxa_performance,
    safe_cast(parametro_taxa_performance as string) parametro_taxa_performance,
    safe_cast(
        porcentagem_indice_referencia_taxa_performance as float64
    ) porcentagem_indice_referencia_taxa_performance,
    safe_cast(valor_cumpom as float64) valor_cumpom,
    safe_cast(
        metodo_calculo_taxa_performance as string
    ) metodo_calculo_taxa_performance,
    safe_cast(
        informacoes_adicionais_taxa_performance as string
    ) informacoes_adicionais_taxa_performance,
    safe_cast(indicador_taxa_ingresso as int64) indicador_taxa_ingresso,
    safe_cast(taxa_ingresso_real as float64) taxa_ingresso_real,
    safe_cast(porcentagem_taxa_ingresso as float64) porcentagem_taxa_ingresso,
    safe_cast(indicador_cobranca_taxa_saida as int64) indicador_cobranca_taxa_saida,
    safe_cast(taxa_saida_real as float64) taxa_saida_real,
    safe_cast(porcentagem_taxa_saida as float64) porcentagem_taxa_saida,
    safe_cast(indicador_operacoes_derivativos as int64) indicador_operacoes_derivativos,
    safe_cast(
        finalidade_operacoes_derivativos as string
    ) finalidade_operacoes_derivativos,
    safe_cast(
        indicador_operacoes_valor_superior_patrimonio_liquido as int64
    ) indicador_operacoes_valor_superior_patrimonio_liquido,
    safe_cast(
        fator_limite_total_operacoes_patrimonio_liquido as float64
    ) fator_limite_total_operacoes_patrimonio_liquido,
    safe_cast(indicador_contraparte_ligado as int64) indicador_contraparte_ligado,
    safe_cast(
        indicador_investimentos_exterior as int64
    ) indicador_investimentos_exterior,
    safe_cast(
        aplicacao_maxima_ativo_exterior as float64
    ) aplicacao_maxima_ativo_exterior,
    safe_cast(indicador_ativo_credito_privado as int64) indicador_ativo_credito_privado,
    safe_cast(
        aplicacao_maxima_ativo_credito_privado as float64
    ) aplicacao_maxima_ativo_credito_privado,
    safe_cast(
        porcentagem_exposicao_minima_emissor_instituicao_financeira as float64
    ) porcentagem_exposicao_minima_emissor_instituicao_financeira,
    safe_cast(
        porcentagem_exposicao_maxima_emissor_instituicao_financeira as float64
    ) porcentagem_exposicao_maxima_emissor_instituicao_financeira,
    safe_cast(
        porcentagem_exposicao_minima_emissor_companhias_abertas as float64
    ) porcentagem_exposicao_minima_emissor_companhias_abertas,
    safe_cast(
        porcentagem_exposicao_maxima_emissor_companhias_abertas as float64
    ) porcentagem_exposicao_maxima_emissor_companhias_abertas,
    safe_cast(
        porcentagem_exposicao_minima_emissor_fundos_investimento as float64
    ) porcentagem_exposicao_minima_emissor_fundos_investimento,
    safe_cast(
        porcentagem_exposicao_maxima_emissor_fundos_investimento as float64
    ) porcentagem_exposicao_maxima_emissor_fundos_investimento,
    safe_cast(
        porcentagem_exposicao_minima_emissor_uniao_federal as float64
    ) porcentagem_exposicao_minima_emissor_uniao_federal,
    safe_cast(
        porcentagem_exposicao_maxima_emissor_uniao_federal as float64
    ) porcentagem_exposicao_maxima_emissor_uniao_federal,
    safe_cast(
        porcentagem_exposicao_minima_emissor_adm_gestor_pessoas_ligadas as float64
    ) porcentagem_exposicao_minima_emissor_adm_gestor_pessoas_ligadas,
    safe_cast(
        porcentagem_exposicao_maxima_emissor_adm_gestor_pessoas_ligadas as float64
    ) porcentagem_exposicao_maxima_emissor_adm_gestor_pessoas_ligadas,
    safe_cast(
        porcentagem_exposicao_minima_emissor_outros as float64
    ) porcentagem_exposicao_minima_emissor_outros,
    safe_cast(
        porcentagem_exposicao_maxima_emissor_outros as float64
    ) porcentagem_exposicao_maxima_emissor_outros,
    safe_cast(
        porcentagem_exposicao_minima_cotas_fi as float64
    ) porcentagem_exposicao_minima_cotas_fi,
    safe_cast(
        porcentagem_exposicao_maxima_cotas_fi as float64
    ) porcentagem_exposicao_maxima_cotas_fi,
    safe_cast(
        porcentagem_exposicao_minima_cotas_fic as float64
    ) porcentagem_exposicao_minima_cotas_fic,
    safe_cast(
        porcentagem_exposicao_maxima_cotas_fic as float64
    ) porcentagem_exposicao_maxima_cotas_fic,
    safe_cast(
        porcentagem_exposicao_minima_cotas_fi_qualificados as float64
    ) porcentagem_exposicao_minima_cotas_fi_qualificados,
    safe_cast(
        porcentagem_exposicao_maxima_cotas_fi_qualificados as float64
    ) porcentagem_exposicao_maxima_cotas_fi_qualificados,
    safe_cast(
        porcentagem_exposicao_minima_cotas_fic_qualificados as float64
    ) porcentagem_exposicao_minima_cotas_fic_qualificados,
    safe_cast(
        porcentagem_exposicao_maxima_cotas_fic_qualificados as float64
    ) porcentagem_exposicao_maxima_cotas_fic_qualificados,
    safe_cast(
        porcentagem_exposicao_minima_cotas_fi_profissionais as float64
    ) porcentagem_exposicao_minima_cotas_fi_profissionais,
    safe_cast(
        porcentagem_exposicao_maxima_cotas_fi_profissionais as float64
    ) porcentagem_exposicao_maxima_cotas_fi_profissionais,
    safe_cast(
        porcentagem_exposicao_minima_cotas_fic_profissionais as float64
    ) porcentagem_exposicao_minima_cotas_fic_profissionais,
    safe_cast(
        porcentagem_exposicao_maxima_cotas_fic_profissionais as float64
    ) porcentagem_exposicao_maxima_cotas_fic_profissionais,
    safe_cast(
        porcentagem_exposicao_minima_cotas_fii as float64
    ) porcentagem_exposicao_minima_cotas_fii,
    safe_cast(
        porcentagem_exposicao_maxima_cotas_fii as float64
    ) porcentagem_exposicao_maxima_cotas_fii,
    safe_cast(
        porcentagem_exposicao_minima_cotas_fidc as float64
    ) porcentagem_exposicao_minima_cotas_fidc,
    safe_cast(
        porcentagem_exposicao_maxima_cotas_fidc as float64
    ) porcentagem_exposicao_maxima_cotas_fidc,
    safe_cast(
        porcentagem_exposicao_minima_cotas_ficfidc as float64
    ) porcentagem_exposicao_minima_cotas_ficfidc,
    safe_cast(
        porcentagem_exposicao_maxima_cotas_ficfidc as float64
    ) porcentagem_exposicao_maxima_cotas_ficfidc,
    safe_cast(
        porcentagem_exposicao_minima_cotas_fidic_np as float64
    ) porcentagem_exposicao_minima_cotas_fidic_np,
    safe_cast(
        porcentagem_exposicao_maxima_cotas_fidic_np as float64
    ) porcentagem_exposicao_maxima_cotas_fidic_np,
    safe_cast(
        porcentagem_exposicao_minima_cotas_ficfidic_np as float64
    ) porcentagem_exposicao_minima_cotas_ficfidic_np,
    safe_cast(
        porcentagem_exposicao_maxima_cotas_ficfidc_np as float64
    ) porcentagem_exposicao_maxima_cotas_ficfidc_np,
    safe_cast(
        porcentagem_exposicao_minima_cotas_etf as float64
    ) porcentagem_exposicao_minima_cotas_etf,
    safe_cast(
        porcentagem_exposicao_maxima_cotas_etf as float64
    ) porcentagem_exposicao_maxima_cotas_etf,
    safe_cast(
        porcentagem_exposicao_minima_cota_cri as float64
    ) porcentagem_exposicao_minima_cota_cri,
    safe_cast(
        porcentagem_exposicao_maxima_cota_cri as float64
    ) porcentagem_exposicao_maxima_cota_cri,
    safe_cast(
        porcentagem_exposicao_minima_titulos_publicos_operacoes_comprimessadas
        as float64
    ) porcentagem_exposicao_minima_titulos_publicos_operacoes_comprimessadas,
    safe_cast(
        porcentagem_exposicao_maxima_titulos_publicos_operacoes_comprimessadas
        as float64
    ) porcentagem_exposicao_maxima_titulos_publicos_operacoes_comprimessadas,
    safe_cast(
        porcentagem_exposicao_minima_ouro as float64
    ) porcentagem_exposicao_minima_ouro,
    safe_cast(
        porcentagem_exposicao_maxima_ouro as float64
    ) porcentagem_exposicao_maxima_ouro,
    safe_cast(
        porcentagem_exposicao_minima_titulos_instituicao_financeira_bacen as float64
    ) porcentagem_exposicao_minima_titulos_instituicao_financeira_bacen,
    safe_cast(
        porcentagem_exposicao_maxima_titulos_instituicao_financeira_bacen as float64
    ) porcentagem_exposicao_maxima_titulos_instituicao_financeira_bacen,
    safe_cast(
        porcentagem_exposicao_minima_valores_mobiliarios as float64
    ) porcentagem_exposicao_minima_valores_mobiliarios,
    safe_cast(
        porcentagem_exposicao_maxima_valores_mobiliarios as float64
    ) porcentagem_exposicao_maxima_valores_mobiliarios,
    safe_cast(
        porcentagem_exposicao_minima_acoes as float64
    ) porcentagem_exposicao_minima_acoes,
    safe_cast(
        porcentagem_exposicao_maxima_acoes as float64
    ) porcentagem_exposicao_maxima_acoes,
    safe_cast(
        porcentagem_exposicao_minima_debenture as float64
    ) porcentagem_exposicao_minima_debenture,
    safe_cast(
        porcentagem_exposicao_maxima_debenture as float64
    ) porcentagem_exposicao_maxima_debenture,
    safe_cast(
        porcentagem_exposicao_minima_notas_promissorias as float64
    ) porcentagem_exposicao_minima_notas_promissorias,
    safe_cast(
        porcentagem_exposicao_maxima_notas_promissorias as float64
    ) porcentagem_exposicao_maxima_notas_promissorias,
    safe_cast(
        porcentagem_exposicao_minima_operacoes_compromissadas_titulos_credito_privado
        as float64
    ) porcentagem_exposicao_minima_operacoes_compromissadas_titulos_credito_privado,
    safe_cast(
        porcentagem_exposicao_maxima_operacoes_compromissadas_titulos_credito_privado
        as float64
    ) porcentagem_exposicao_maxima_operacoes_compromissadas_titulos_credito_privado,
    safe_cast(
        porcentagem_exposicao_minima_derivativos as float64
    ) porcentagem_exposicao_minima_derivativos,
    safe_cast(
        porcentagem_exposicao_maxima_derivativos as float64
    ) porcentagem_exposicao_maxima_derivativos,
    safe_cast(
        porcentagem_exposicao_minima_outros as float64
    ) porcentagem_exposicao_minima_outros,
    safe_cast(
        porcentagem_exposicao_maxima_outros as float64
    ) porcentagem_exposicao_maxima_outros,
    safe_cast(
        porcentagem_exposicao_minima_cotas_fmiee as float64
    ) porcentagem_exposicao_minima_cotas_fmiee,
    safe_cast(
        porcentagem_exposicao_maxima_cotas_fmiee as float64
    ) porcentagem_exposicao_maxima_cotas_fmiee,
    safe_cast(
        porcentagem_exposicao_minima_cotas_fip as float64
    ) porcentagem_exposicao_minima_cotas_fip,
    safe_cast(
        porcentagem_exposicao_maxima_cotas_fip as float64
    ) porcentagem_exposicao_maxima_cotas_fip,
    safe_cast(
        porcentagem_exposicao_minima_cotas_ficfip as float64
    ) porcentagem_exposicao_minima_cotas_ficfip,
    safe_cast(
        porcentagem_exposicao_maxima_cotas_ficfip as float64
    ) porcentagem_exposicao_maxima_cotas_ficfip,
from
    {{ set_datalake_project("br_cvm_fi_staging.documentos_extratos_informacoes") }} as t
