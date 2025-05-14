{{ config(alias="municipio", schema="br_ipea_avs") }}
select
    safe_cast(ano as int64) ano,
    safe_cast(sigla_uf as string) sigla_uf,
    safe_cast(id_municipio as string) id_municipio,
    safe_cast(raca_cor as string) raca_cor,
    safe_cast(sexo as string) sexo,
    safe_cast(localizacao as string) localizacao,
    safe_cast(ivs as float64) ivs,
    safe_cast(ivs_infraestrutura_urbana as float64) ivs_infraestrutura_urbana,
    safe_cast(ivs_capital_humano as float64) ivs_capital_humano,
    safe_cast(ivs_renda_trabalho as float64) ivs_renda_trabalho,
    safe_cast(udh as string) udh,
    safe_cast(nome_udh as string) nome_udh,
    safe_cast(idhm as float64) idhm,
    safe_cast(idhm_l as float64) idhm_l,
    safe_cast(idhm_e as float64) idhm_e,
    safe_cast(idhm_r as float64) idhm_r,
    safe_cast(idhm_subescolaridade as float64) idhm_subescolaridade,
    safe_cast(idhm_subfrequencia as float64) idhm_subfrequencia,
    safe_cast(prosperidade_social as string) prosperidade_social,
    safe_cast(
        proporcao_vulnerabilidade_socioeconomica as float64
    ) proporcao_vulnerabilidade_socioeconomica,
    safe_cast(propocao_energia_eletrica as float64) propocao_energia_eletrica,
    safe_cast(proporcao_domicilio_densidade as float64) proporcao_domicilio_densidade,
    safe_cast(proporcao_sem_agua_esgoto as float64) proporcao_sem_agua_esgoto,
    safe_cast(proporcao_sem_coleta as float64) proporcao_sem_coleta,
    safe_cast(renda_per_capita as float64) renda_per_capita,
    safe_cast(renda_media_18_mais as float64) renda_media_18_mais,
    safe_cast(proporcao_sem_renda_18_mais as float64) proporcao_sem_renda_18_mais,
    safe_cast(renda_trabalho as float64) renda_trabalho,
    safe_cast(renda_per_capita_vulneravel as float64) renda_per_capita_vulneravel,
    safe_cast(proporcao_vulneravel as float64) proporcao_vulneravel,
    safe_cast(populacao_vulneravel_15_24 as float64) populacao_vulneravel_15_24,
    safe_cast(
        proporcao_vulneravel_dependente_idoso as float64
    ) proporcao_vulneravel_dependente_idoso,
    safe_cast(populacao_vulneravel_e_idoso as float64) populacao_vulneravel_e_idoso,
    safe_cast(razao_dependencia as float64) razao_dependencia,
    safe_cast(fecundidade_total as float64) fecundidade_total,
    safe_cast(taxa_envelhecimento as float64) taxa_envelhecimento,
    safe_cast(mortalidade_1_menos as float64) mortalidade_1_menos,
    safe_cast(proporcao_mortalidade_5_menos as float64) proporcao_mortalidade_5_menos,
    safe_cast(
        proporcao_crianca_fora_escola_0_5 as float64
    ) proporcao_crianca_fora_escola_0_5,
    safe_cast(proporcao_escola_5_6 as float64) proporcao_escola_5_6,
    safe_cast(
        proporcao_crianca_fora_escola_6_14 as float64
    ) proporcao_crianca_fora_escola_6_14,
    safe_cast(proporcao_maternidade_10_17 as float64) proporcao_maternidade_10_17,
    safe_cast(maternidade_crianca_15_menos as float64) maternidade_crianca_15_menos,
    safe_cast(
        proporcao_maternidade_fundamental_incompleto_crianca_15_menos as float64
    ) proporcao_maternidade_fundamental_incompleto_crianca_15_menos,
    safe_cast(
        proporcao_analfabetismo_15_mais as float64
    ) proporcao_analfabetismo_15_mais,
    safe_cast(
        proporcao_analfabetismo_18_mais as float64
    ) proporcao_analfabetismo_18_mais,
    safe_cast(
        proporcao_analfabetismo_25_mais as float64
    ) proporcao_analfabetismo_25_mais,
    safe_cast(
        proporcao_responsavel_fundamental_incompleto as float64
    ) proporcao_responsavel_fundamental_incompleto,
    safe_cast(proporcao_anos_finais_11_13 as float64) proporcao_anos_finais_11_13,
    safe_cast(
        proporcao_fundamental_completo_15_17 as float64
    ) proporcao_fundamental_completo_15_17,
    safe_cast(
        proporcao_fundamental_completo_18_mais as float64
    ) proporcao_fundamental_completo_18_mais,
    safe_cast(proporcao_medio_completo_18_20 as float64) proporcao_medio_completo_18_20,
    safe_cast(populacao_ocupada_trabalho as float64) populacao_ocupada_trabalho,
    safe_cast(
        proporcao_ocupado_fundamental_18_mais as float64
    ) proporcao_ocupado_fundamental_18_mais,
    safe_cast(
        proporcao_ocupado_medio_18_mais as float64
    ) proporcao_ocupado_medio_18_mais,
    safe_cast(
        proporcao_ocupado_superior_18_mais as float64
    ) proporcao_ocupado_superior_18_mais,
    safe_cast(proporcao_desocupado_15_24 as float64) proporcao_desocupado_15_24,
    safe_cast(proporcao_desocupado_18_mais as float64) proporcao_desocupado_18_mais,
    safe_cast(
        proporcao_ocupados_informal_18_mais as float64
    ) proporcao_ocupados_informal_18_mais,
    safe_cast(proporcao_carteira_18_mais as float64) proporcao_carteira_18_mais,
    safe_cast(proporcao_sem_carteira_18_mais as float64) proporcao_sem_carteira_18_mais,
    safe_cast(
        proporcao_setor_publico_18_mais as float64
    ) proporcao_setor_publico_18_mais,
    safe_cast(proporcao_empreendedor_18_mais as float64) proporcao_empreendedor_18_mais,
    safe_cast(proporcao_empregador_18_mais as float64) proporcao_empregador_18_mais,
    safe_cast(
        proporcao_mercado_formal_18_mais as float64
    ) proporcao_mercado_formal_18_mais,
    safe_cast(proporcao_atividade_10_14 as float64) proporcao_atividade_10_14,
    safe_cast(pea_10_mais as int64) pea_10_mais,
    safe_cast(pea_10_14 as int64) pea_10_14,
    safe_cast(pea_15_17 as int64) pea_15_17,
    safe_cast(pea_18_mais as int64) pea_18_mais,
    safe_cast(expectativa_vida as float64) expectativa_vida,
    safe_cast(indice_gini as float64) indice_gini,
    safe_cast(populacao_0_1 as int64) populacao_0_1,
    safe_cast(populacao_1_3 as int64) populacao_1_3,
    safe_cast(populacao_4 as int64) populacao_4,
    safe_cast(populacao_5 as int64) populacao_5,
    safe_cast(populacao_6 as int64) populacao_6,
    safe_cast(populacao_6_10 as int64) populacao_6_10,
    safe_cast(populacao_6_17 as int64) populacao_6_17,
    safe_cast(populacao_11_13 as int64) populacao_11_13,
    safe_cast(populacao_11_14 as int64) populacao_11_14,
    safe_cast(populacao_12_14 as int64) populacao_12_14,
    safe_cast(populacao_15_mais as int64) populacao_15_mais,
    safe_cast(populacao_15_17 as int64) populacao_15_17,
    safe_cast(populacao_15_24 as int64) populacao_15_24,
    safe_cast(populacao_16_18 as int64) populacao_16_18,
    safe_cast(populacao_18_mais as int64) populacao_18_mais,
    safe_cast(populacao_18_20 as int64) populacao_18_20,
    safe_cast(populacao_18_24 as int64) populacao_18_24,
    safe_cast(populacao_19_21 as int64) populacao_19_21,
    safe_cast(populacao_25_mais as int64) populacao_25_mais,
    safe_cast(populacao_65_mais as int64) populacao_65_mais,
from {{ set_datalake_project("br_ipea_avs_staging.municipio") }} as t
