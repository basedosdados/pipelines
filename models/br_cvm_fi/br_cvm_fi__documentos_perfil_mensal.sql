{{
    config(
        schema="br_cvm_fi",
        alias="documentos_perfil_mensal",
        materialized="table",
        partition_by={
            "field": "ano",
            "data_type": "int64",
            "range": {"start": 2019, "end": 2025, "interval": 1},
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
    safe_cast(versao as string) versao,
    safe_cast(
        quantidade_clientes_cotitstas_pessoa_fisica_private_banking as int64
    ) quantidade_clientes_cotitstas_pessoa_fisica_private_banking,
    safe_cast(
        quantidade_clientes_cotitstas_pessoa_fisica_varejo as int64
    ) quantidade_clientes_cotitstas_pessoa_fisica_varejo,
    safe_cast(
        quantidade_clientes_cotistas_pessoa_juridica_nao_financeira_private_banking
        as int64
    ) quantidade_clientes_cotistas_pessoa_juridica_nao_financeira_private_banking,
    safe_cast(
        quantidade_clientes_cotistas_pessoa_juridica_nao_financeira_varejo as int64
    ) quantidade_clientes_cotistas_pessoa_juridica_nao_financeira_varejo,
    safe_cast(
        quantidade_clientes_cotitstas_banco_comercial as int64
    ) quantidade_clientes_cotitstas_banco_comercial,
    safe_cast(
        quantidade_clientes_cotistas_corretora_distribuidora as int64
    ) quantidade_clientes_cotistas_corretora_distribuidora,
    safe_cast(
        quantidade_clientes_cotistas_outras_pessoas_juridicas_financeiras as int64
    ) quantidade_clientes_cotistas_outras_pessoas_juridicas_financeiras,
    safe_cast(
        quantidade_clientes_cotistas_investidores_nao_residentes as int64
    ) quantidade_clientes_cotistas_investidores_nao_residentes,
    safe_cast(
        quantidade_clientes_cotistas_entidade_aberta_previdencia_complementar as int64
    ) quantidade_clientes_cotistas_entidade_aberta_previdencia_complementar,
    safe_cast(
        quantidade_clientes_cotistas_entidade_fechada_previdencia_complementar as int64
    ) quantidade_clientes_cotistas_entidade_fechada_previdencia_complementar,
    safe_cast(
        quantidade_clientes_cotistas_regime_proprio_previdencia_servidores_publicos
        as int64
    ) quantidade_clientes_cotistas_regime_proprio_previdencia_servidores_publicos,
    safe_cast(
        quantidade_clientes_cotistas_sociedade_seguradora_resseguradora as int64
    ) quantidade_clientes_cotistas_sociedade_seguradora_resseguradora,
    safe_cast(
        quantidade_clientes_cotistas_sociedade_capitalizacao_arrendamento_mercantil
        as int64
    ) quantidade_clientes_cotistas_sociedade_capitalizacao_arrendamento_mercantil,
    safe_cast(
        quantidade_clientes_cotistas_fundos_clubes_investimento as int64
    ) quantidade_clientes_cotistas_fundos_clubes_investimento,
    safe_cast(
        quantidade_clientes_cotistas_distribuidores_fundo as int64
    ) quantidade_clientes_cotistas_distribuidores_fundo,
    safe_cast(
        quantidade_clientes_cotistas_outros_tipos as int64
    ) quantidade_clientes_cotistas_outros_tipos,
    safe_cast(
        porcentagem_patrimonio_liquido_detido_cotitstas_pessoa_fisica_private_banking
        as float64
    ) porcentagem_patrimonio_liquido_detido_cotitstas_pessoa_fisica_private_banking,
    safe_cast(
        porcentagem_patrimonio_liquido_detido_cotitstas_pessoa_fisica_varejo as float64
    ) porcentagem_patrimonio_liquido_detido_cotitstas_pessoa_fisica_varejo,
    safe_cast(
        porcentagem_patrimonio_liquido_detido_cotistas_pessoa_juridica_nao_financeira_private_banking
        as float64
    ) porcentagem_patrimonio_liquido_detido_cotistas_pessoa_juridica_nao_financeira_private_banking,
    safe_cast(
        porcentagem_patrimonio_liquido_detido_cotistas_pessoa_juridica_nao_financeira_varejo
        as float64
    ) porcentagem_patrimonio_liquido_detido_cotistas_pessoa_juridica_nao_financeira_varejo,
    safe_cast(
        porcentagem_patrimonio_liquido_detido_cotitstas_banco_comercial as float64
    ) porcentagem_patrimonio_liquido_detido_cotitstas_banco_comercial,
    safe_cast(
        porcentagem_patrimonio_liquido_detido_cotistas_corretora_distribuidora
        as float64
    ) porcentagem_patrimonio_liquido_detido_cotistas_corretora_distribuidora,
    safe_cast(
        porcentagem_patrimonio_liquido_detido_cotistas_outras_pessoas_juridicas_financeiras
        as float64
    ) porcentagem_patrimonio_liquido_detido_cotistas_outras_pessoas_juridicas_financeiras,
    safe_cast(
        porcentagem_patrimonio_liquido_detido_cotistas_investidores_nao_residentes
        as float64
    ) porcentagem_patrimonio_liquido_detido_cotistas_investidores_nao_residentes,
    safe_cast(
        porcentagem_patrimonio_liquido_detido_cotistas_entidade_aberta_previdencia_complementar
        as float64
    ) porcentagem_patrimonio_liquido_detido_cotistas_entidade_aberta_previdencia_complementar,
    safe_cast(
        porcentagem_patrimonio_liquido_detido_cotistas_entidade_fechada_previdencia_complementar
        as float64
    ) porcentagem_patrimonio_liquido_detido_cotistas_entidade_fechada_previdencia_complementar,
    safe_cast(
        porcentagem_patrimonio_liquido_detido_cotistas_regime_proprio_previdencia_servidores_publicos
        as float64
    ) porcentagem_patrimonio_liquido_detido_cotistas_regime_proprio_previdencia_servidores_publicos,
    safe_cast(
        porcentagem_patrimonio_liquido_detido_cotistas_sociedade_seguradora_resseguradora
        as float64
    ) porcentagem_patrimonio_liquido_detido_cotistas_sociedade_seguradora_resseguradora,
    safe_cast(
        porcentagem_patrimonio_liquido_detido_cotistas_sociedade_capitalizacao_arrendamento_mercantil
        as float64
    ) porcentagem_patrimonio_liquido_detido_cotistas_sociedade_capitalizacao_arrendamento_mercantil,
    safe_cast(
        porcentagem_patrimonio_liquido_cotistas_fundos_clubes_investimento as float64
    ) porcentagem_patrimonio_liquido_cotistas_fundos_clubes_investimento,
    safe_cast(
        porcentagem_patrimonio_liquido_cotistas_distribuidores_fundo as float64
    ) porcentagem_patrimonio_liquido_cotistas_distribuidores_fundo,
    safe_cast(
        porcentagem_patrimonio_liquido_cotistas_outros_tipos as float64
    ) porcentagem_patrimonio_liquido_cotistas_outros_tipos,
    safe_cast(
        resumo_voto_adminstrador_assembleia as string
    ) resumo_voto_adminstrador_assembleia,
    safe_cast(
        justificativa_voto_administrador_assembleia as string
    ) justificativa_voto_administrador_assembleia,
    safe_cast(
        porcentagem_valor_em_risco_carteria as float64
    ) porcentagem_valor_em_risco_carteria,
    safe_cast(tipo_modelos_valor_em_risco as string) tipo_modelos_valor_em_risco,
    safe_cast(prazo_carteira_titulos as float64) prazo_carteira_titulos,
    safe_cast(
        resumo_deliberacoes_aprovadas_assembleia as string
    ) resumo_deliberacoes_aprovadas_assembleia,
    safe_cast(
        valor_total_contrato_compra_dolar as float64
    ) valor_total_contrato_compra_dolar,
    safe_cast(
        valor_total_contrato_venda_dolar as float64
    ) valor_total_contrato_venda_dolar,
    safe_cast(
        porcentagem_variacao_diaria_valor_cota as float64
    ) porcentagem_variacao_diaria_valor_cota,
    safe_cast(fator_primitivo_risco as string) fator_primitivo_risco,
    safe_cast(
        cenario_fator_primitivo_risco_ibovespa as string
    ) cenario_fator_primitivo_risco_ibovespa,
    safe_cast(
        cenario_fator_primitivo_risco_juros as string
    ) cenario_fator_primitivo_risco_juros,
    safe_cast(
        cenario_fator_primitivo_cupom_cambial as string
    ) cenario_fator_primitivo_cupom_cambial,
    safe_cast(cenario_fator_primitivo_dolar as string) cenario_fator_primitivo_dolar,
    safe_cast(cenario_fator_primitivo_outros as string) cenario_fator_primitivo_outros,
    safe_cast(
        porcentagem_variacao_diaria_cota_estresse as float64
    ) porcentagem_variacao_diaria_cota_estresse,
    safe_cast(
        porcentagem_variacao_diaria_patrimonio_liquido_taxa_anual_juros as float64
    ) porcentagem_variacao_diaria_patrimonio_liquido_taxa_anual_juros,
    safe_cast(
        porcentagem_variacao_diaria_patrimonio_liquido_taxa_cambio as float64
    ) porcentagem_variacao_diaria_patrimonio_liquido_taxa_cambio,
    safe_cast(
        porcentagem_variacao_diaria_patrimonio_liquido_ibovespa as float64
    ) porcentagem_variacao_diaria_patrimonio_liquido_ibovespa,
    safe_cast(fator_risco_outros as string) fator_risco_outros,
    safe_cast(
        porcentagem_variacao_diaria_patrimonio_liquido_outros as float64
    ) porcentagem_variacao_diaria_patrimonio_liquido_outros,
    safe_cast(
        porcentagem_valor_colateral_garantia_derivativos as float64
    ) porcentagem_valor_colateral_garantia_derivativos,
    safe_cast(fator_risco_nocional as string) fator_risco_nocional,
    safe_cast(
        valor_fator_risco_nocional_long_ibovespa as float64
    ) valor_fator_risco_nocional_long_ibovespa,
    safe_cast(
        valor_fator_risco_nocional_long_juros as float64
    ) valor_fator_risco_nocional_long_juros,
    safe_cast(
        valor_fator_risco_nocional_long_cupom as float64
    ) valor_fator_risco_nocional_long_cupom,
    safe_cast(
        valor_fator_risco_nocional_long_dolar as float64
    ) valor_fator_risco_nocional_long_dolar,
    safe_cast(
        valor_fator_risco_nocional_long_outro as float64
    ) valor_fator_risco_nocional_long_outro,
    safe_cast(
        valor_fator_risco_nocional_short_ibovespa as float64
    ) valor_fator_risco_nocional_short_ibovespa,
    safe_cast(
        valor_fator_risco_nocional_short_juros as float64
    ) valor_fator_risco_nocional_short_juros,
    safe_cast(
        valor_fator_risco_nocional_short_cupom as float64
    ) valor_fator_risco_nocional_short_cupom,
    safe_cast(
        valor_fator_risco_nocional_short_dolar as float64
    ) valor_fator_risco_nocional_short_dolar,
    safe_cast(
        valor_fator_risco_nocional_short_outro as float64
    ) valor_fator_risco_nocional_short_outro,
    safe_cast(tipo_pessoa_comitente_1 as string) tipo_pessoa_comitente_1,
    safe_cast(cpf_cnpj_comitente_1 as string) cpf_cnpj_comitente_1,
    safe_cast(
        indicador_comitente_relacionada_administrador_gestor_1 as int64
    ) indicador_comitente_relacionada_administrador_gestor_1,
    safe_cast(
        porcentagem_valor_parte_comitente_1 as float64
    ) porcentagem_valor_parte_comitente_1,
    safe_cast(tipo_pessoa_comitente_2 as string) tipo_pessoa_comitente_2,
    safe_cast(cpf_cnpj_comitente_2 as string) cpf_cnpj_comitente_2,
    safe_cast(
        indicador_comitente_relacionada_administrador_gestor_2 as int64
    ) indicador_comitente_relacionada_administrador_gestor_2,
    safe_cast(
        porcentagem_valor_parte_comitente_2 as float64
    ) porcentagem_valor_parte_comitente_2,
    safe_cast(tipo_pessoa_comitente_3 as string) tipo_pessoa_comitente_3,
    safe_cast(cpf_cnpj_comitente_3 as string) cpf_cnpj_comitente_3,
    safe_cast(
        indicador_comitente_relacionada_administrador_gestor_3 as int64
    ) indicador_comitente_relacionada_administrador_gestor_3,
    safe_cast(
        porcentagem_valor_parte_comitente_3 as float64
    ) porcentagem_valor_parte_comitente_3,
    safe_cast(
        porcentagem_valor_total_ativos_emissao_partes_relacionadas as float64
    ) porcentagem_valor_total_ativos_emissao_partes_relacionadas,
    safe_cast(tipo_pessoa_emissor_1 as string) tipo_pessoa_emissor_1,
    safe_cast(cpf_cnpj_emissor_1 as string) cpf_cnpj_emissor_1,
    safe_cast(
        indicador_emissor_relacionada_administrador_gestor_1 as int64
    ) indicador_emissor_relacionada_administrador_gestor_1,
    safe_cast(
        porcentagem_valor_parte_emissor_1 as float64
    ) porcentagem_valor_parte_emissor_1,
    safe_cast(tipo_pessoa_emissor_2 as string) tipo_pessoa_emissor_2,
    safe_cast(cpf_cnpj_emissor_2 as string) cpf_cnpj_emissor_2,
    safe_cast(
        indicador_emissor_relacionada_administrador_gestor_2 as int64
    ) indicador_emissor_relacionada_administrador_gestor_2,
    safe_cast(
        porcentagem_valor_parte_emissor_2 as float64
    ) porcentagem_valor_parte_emissor_2,
    safe_cast(tipo_pessoa_emissor_3 as string) tipo_pessoa_emissor_3,
    safe_cast(cpf_cnpj_emissor_3 as string) cpf_cnpj_emissor_3,
    safe_cast(
        indicador_emissor_relacionada_administrador_gestor_3 as int64
    ) indicador_emissor_relacionada_administrador_gestor_3,
    safe_cast(
        porcentagem_valor_parte_emissor_3 as float64
    ) porcentagem_valor_parte_emissor_3,
    safe_cast(
        porcentagem_valor_total_ativos_credito_privado as float64
    ) porcentagem_valor_total_ativos_credito_privado,
    safe_cast(
        indicador_vedada_cobranca_taxa_performance as int64
    ) indicador_vedada_cobranca_taxa_performance,
    safe_cast(
        data_cota_fundo_ultima_cobranca_taxa_performance as date
    ) data_cota_fundo_ultima_cobranca_taxa_performance,
    safe_cast(
        valor_cota_fundo_ultima_cobranca_taxa_performance as float64
    ) valor_cota_fundo_ultima_cobranca_taxa_performance,
    safe_cast(
        valor_distribuido_direito_dividendos_juros_capital_proprio_outros as float64
    ) valor_distribuido_direito_dividendos_juros_capital_proprio_outros,
    safe_cast(
        quantidade_cotistas_entidade_previdencia_complementar as int64
    ) quantidade_cotistas_entidade_previdencia_complementar,
    safe_cast(
        porcentagem_cotistas_entidade_previdencia_complementar as float64
    ) porcentagem_cotistas_entidade_previdencia_complementar,
    safe_cast(
        porcentagem_patrimonio_liquido_maior_cotista as float64
    ) porcentagem_patrimonio_liquido_maior_cotista,
    safe_cast(
        quantidade_dias_cinquenta_percentual as int64
    ) quantidade_dias_cinquenta_percentual,
    safe_cast(quantidade_dias_cem_percentual as int64) quantidade_dias_cem_percentual,
    safe_cast(indicador_liquidez as int64) indicador_liquidez,
    safe_cast(
        porcentagem_patrimonio_liquido_convertido_caixa as float64
    ) porcentagem_patrimonio_liquido_convertido_caixa,
from {{ set_datalake_project("br_cvm_fi_staging.documentos_perfil_mensal") }} as t
