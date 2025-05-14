{{ config(alias="prestador_agua_esgoto", schema="br_mdr_snis") }}
select
    safe_cast(ano as int64) ano,
    safe_cast(id_municipio as string) id_municipio,
    safe_cast(sigla_uf as string) sigla_uf,
    safe_cast(id_prestador as string) id_prestador,
    safe_cast(prestador as string) prestador,
    safe_cast(sigla_prestador as string) sigla_prestador,
    safe_cast(abrangencia as string) abrangencia,
    safe_cast(tipo_servico as string) tipo_servico,
    safe_cast(natureza_juridica as string) natureza_juridica,
    safe_cast(populacao_atendida_agua as int64) populacao_atendida_agua,
    safe_cast(populacao_atentida_esgoto as int64) populacao_atentida_esgoto,
    safe_cast(populacao_urbana as int64) populacao_urbana,
    safe_cast(populacao_urbana_atendida_agua as int64) populacao_urbana_atendida_agua,
    safe_cast(
        populacao_urbana_atendida_esgoto as int64
    ) populacao_urbana_atendida_esgoto,
    safe_cast(extensao_rede_agua as float64) extensao_rede_agua,
    safe_cast(extensao_rede_esgoto as float64) extensao_rede_esgoto,
    -- safe_cast(local_atendimento_agua as int64) local_atendimento_agua,
    -- safe_cast(local_atendimento_esgoto as int64) local_atendimento_esgoto,
    safe_cast(ano_vencimento_delegacao_agua as int64) ano_vencimento_delegacao_agua,
    safe_cast(ano_vencimento_delegacao_esgoto as int64) ano_vencimento_delegacao_esgoto,
    safe_cast(
        quantidade_municipios_agua_delegacao_vigor as int64
    ) quantidade_municipios_agua_delegacao_vigor,
    safe_cast(
        quantidade_municipios_esgoto_delegacao_vigor as int64
    ) quantidade_municipios_esgoto_delegacao_vigor,
    safe_cast(
        quantidade_municipios_agua_delegacao_vencida as int64
    ) quantidade_municipios_agua_delegacao_vencida,
    safe_cast(
        quantidade_municipios_esgoto_delegacao_vencida as int64
    ) quantidade_municipios_esgoto_delegacao_vencida,
    safe_cast(
        quantidade_municipios_agua_sem_delegacao as int64
    ) quantidade_municipios_agua_sem_delegacao,
    safe_cast(
        quantidade_municipios_esgoto_sem_delegacao as int64
    ) quantidade_municipios_esgoto_sem_delegacao,
    safe_cast(
        quantidade_municipios_sem_esgoto_sem_delegacao as int64
    ) quantidade_municipios_sem_esgoto_sem_delegacao,
    safe_cast(quantidade_sede_municipal_agua as int64) quantidade_sede_municipal_agua,
    safe_cast(
        quantidade_sede_municipal_esgoto as int64
    ) quantidade_sede_municipal_esgoto,
    safe_cast(quantidade_localidade_agua as int64) quantidade_localidade_agua,
    safe_cast(quantidade_localidade_esgoto as int64) quantidade_localidade_esgoto,
    safe_cast(quantidade_ligacao_total_agua as int64) quantidade_ligacao_total_agua,
    safe_cast(quantidade_ligacao_total_esgoto as int64) quantidade_ligacao_total_esgoto,
    safe_cast(quantidade_ligacao_ativa_agua as int64) quantidade_ligacao_ativa_agua,
    safe_cast(quantidade_ligacao_ativa_esgoto as int64) quantidade_ligacao_ativa_esgoto,
    safe_cast(quantidade_economia_ativa_agua as int64) quantidade_economia_ativa_agua,
    safe_cast(
        quantidade_economia_ativa_esgoto as int64
    ) quantidade_economia_ativa_esgoto,
    safe_cast(
        quantidade_ligacao_micromedia_ativa_agua as int64
    ) quantidade_ligacao_micromedia_ativa_agua,
    safe_cast(
        quantidade_economia_residencial_ativa_agua as int64
    ) quantidade_economia_residencial_ativa_agua,
    safe_cast(
        quantidade_economia_micromedida_ativa_agua as int64
    ) quantidade_economia_micromedida_ativa_agua,
    safe_cast(
        quantidade_economia_residencial_micromedida_ativa_agua as int64
    ) quantidade_economia_residencial_micromedida_ativa_agua,
    safe_cast(
        quantidade_economia_residencial_ativa_esgoto as int64
    ) quantidade_economia_residencial_ativa_esgoto,
    safe_cast(volume_agua_produzido as float64) volume_agua_produzido,
    safe_cast(volume_agua_tratada_eta as float64) volume_agua_tratada_eta,
    safe_cast(volume_agua_micromedido as float64) volume_agua_micromedido,
    safe_cast(volume_agua_consumido as float64) volume_agua_consumido,
    safe_cast(volume_agua_faturado as float64) volume_agua_faturado,
    safe_cast(volume_agua_macromedido as float64) volume_agua_macromedido,
    safe_cast(
        volume_agua_tratada_desinfeccao as float64
    ) volume_agua_tratada_desinfeccao,
    safe_cast(volume_agua_bruta_exportado as float64) volume_agua_bruta_exportado,
    safe_cast(volume_agua_tratada_importado as float64) volume_agua_tratada_importado,
    safe_cast(volume_agua_tratada_exportado as float64) volume_agua_tratada_exportado,
    safe_cast(
        volume_agua_micromedido_economia_ativa as float64
    ) volume_agua_micromedido_economia_ativa,
    safe_cast(volume_servico_agua as float64) volume_servico_agua,
    safe_cast(volume_agua_fluoretada as float64) volume_agua_fluoretada,
    safe_cast(consumo_eletrico_sistemas_agua as float64) consumo_eletrico_sistemas_agua,
    safe_cast(volume_esgoto_coletado as float64) volume_esgoto_coletado,
    safe_cast(volume_esgoto_tratado as float64) volume_esgoto_tratado,
    safe_cast(volume_esgoto_faturado as float64) volume_esgoto_faturado,
    safe_cast(volume_esgoto_bruto_exportado as float64) volume_esgoto_bruto_exportado,
    safe_cast(volume_esgoto_bruto_importado as float64) volume_esgoto_bruto_importado,
    safe_cast(volume_esgoto_importado as float64) volume_esgoto_importado,
    safe_cast(
        volume_esgoto_bruto_exportado_tratamento_importador as float64
    ) volume_esgoto_bruto_exportado_tratamento_importador,
    safe_cast(
        consumo_eletrico_sistemas_esgoto as float64
    ) consumo_eletrico_sistemas_esgoto,
    safe_cast(indice_agua_ligacao as float64) indice_agua_ligacao,
    safe_cast(indice_hidrometracao as float64) indice_hidrometracao,
    safe_cast(
        indice_macromedicao_volume_disponibilizado as float64
    ) indice_macromedicao_volume_disponibilizado,
    safe_cast(indice_macromedicao as float64) indice_macromedicao,
    safe_cast(indice_perda_faturamento as float64) indice_perda_faturamento,
    safe_cast(indice_micromedido_economia as float64) indice_micromedido_economia,
    safe_cast(indice_coleta_esgoto as float64) indice_coleta_esgoto,
    safe_cast(indice_tratamento_esgoto as float64) indice_tratamento_esgoto,
    safe_cast(indice_consumo_agua_faturado as float64) indice_consumo_agua_faturado,
    safe_cast(indice_extensao_agua_ligacao as float64) indice_extensao_agua_ligacao,
    safe_cast(indice_extensao_esgoto_ligacao as float64) indice_extensao_esgoto_ligacao,
    safe_cast(indice_consumo_agua_per_capita as float64) indice_consumo_agua_per_capita,
    safe_cast(indice_atendimento_urbano_agua as float64) indice_atendimento_urbano_agua,
    safe_cast(indice_atendimento_agua_esgoto as float64) indice_atendimento_agua_esgoto,
    safe_cast(indice_volume_agua_economia as float64) indice_volume_agua_economia,
    safe_cast(indice_faturamento_agua as float64) indice_faturamento_agua,
    safe_cast(
        indice_participacao_economia_agua as float64
    ) indice_participacao_economia_agua,
    safe_cast(
        indice_micromedicao_relativo_agua as float64
    ) indice_micromedicao_relativo_agua,
    safe_cast(
        indice_esgotamento_agua_consumida as float64
    ) indice_esgotamento_agua_consumida,
    safe_cast(
        indice_atendimento_esgoto_esgoto as float64
    ) indice_atendimento_esgoto_esgoto,
    safe_cast(indice_perda_distribuicao_agua as float64) indice_perda_distribuicao_agua,
    safe_cast(indice_perda_linear_agua as float64) indice_perda_linear_agua,
    safe_cast(indice_perda_ligacao_agua as float64) indice_perda_ligacao_agua,
    safe_cast(indice_consumo_agua as float64) indice_consumo_agua,
    safe_cast(indice_consumo_medio_agua as float64) indice_consumo_medio_agua,
    safe_cast(indice_atendimento_total_agua as float64) indice_atendimento_total_agua,
    safe_cast(indice_atendimento_esgoto_agua as float64) indice_atendimento_esgoto_agua,
    safe_cast(indice_fluoretacao_agua as float64) indice_fluoretacao_agua,
    safe_cast(indice_consumo_energia_agua as float64) indice_consumo_energia_agua,
    safe_cast(indice_consumo_energia_esgoto as float64) indice_consumo_energia_esgoto,
    safe_cast(receita_operacional_direta as float64) receita_operacional_direta,
    safe_cast(
        receita_operacional_direta_agua as float64
    ) receita_operacional_direta_agua,
    safe_cast(
        receita_operacional_direta_esgoto as float64
    ) receita_operacional_direta_esgoto,
    safe_cast(receita_operacional_indireta as float64) receita_operacional_indireta,
    safe_cast(
        receita_operacional_direta_agua_exportada as float64
    ) receita_operacional_direta_agua_exportada,
    safe_cast(receita_operacional as float64) receita_operacional,
    safe_cast(
        receita_operacional_direta_esgoto_importado as float64
    ) receita_operacional_direta_esgoto_importado,
    safe_cast(arrecadacao_total as float64) arrecadacao_total,
    safe_cast(credito_areceber as float64) credito_areceber,
    safe_cast(despesa_pessoal as float64) despesa_pessoal,
    safe_cast(quantidade_empregado as int64) quantidade_empregado,
    safe_cast(despesa_produto_quimico as float64) despesa_produto_quimico,
    safe_cast(despesa_energia as float64) despesa_energia,
    safe_cast(despesa_servico_terceiro as float64) despesa_servico_terceiro,
    safe_cast(despesa_exploracao as float64) despesa_exploracao,
    safe_cast(despesas_juros_divida as float64) despesas_juros_divida,
    safe_cast(despesa_total_servico as float64) despesa_total_servico,
    safe_cast(despesa_ativo as float64) despesa_ativo,
    safe_cast(despesa_agua_importada as float64) despesa_agua_importada,
    safe_cast(despesa_fiscal as float64) despesa_fiscal,
    safe_cast(despesa_fiscal_nao_computada as float64) despesa_fiscal_nao_computada,
    safe_cast(despesa_exploracao_outro as float64) despesa_exploracao_outro,
    safe_cast(despesa_servico_outro as float64) despesa_servico_outro,
    safe_cast(despesa_amortizacao_divida as float64) despesa_amortizacao_divida,
    safe_cast(despesas_juros_divida_excecao as float64) despesas_juros_divida_excecao,
    safe_cast(despesa_divida_variacao as float64) despesa_divida_variacao,
    safe_cast(despesa_divida_total as float64) despesa_divida_total,
    safe_cast(despesa_esgoto_exportado as float64) despesa_esgoto_exportado,
    safe_cast(
        despesa_capitalizavel_municipio as float64
    ) despesa_capitalizavel_municipio,
    safe_cast(despesa_capitalizavel_estado as float64) despesa_capitalizavel_estado,
    safe_cast(
        despesa_capitalizavel_prestador as float64
    ) despesa_capitalizavel_prestador,
    safe_cast(investimento_agua_prestador as float64) investimento_agua_prestador,
    safe_cast(investimento_esgoto_prestador as float64) investimento_esgoto_prestador,
    safe_cast(investimento_outro_prestador as float64) investimento_outro_prestador,
    safe_cast(
        investimento_recurso_proprio_prestador as float64
    ) investimento_recurso_proprio_prestador,
    safe_cast(
        investimento_recurso_oneroso_prestador as float64
    ) investimento_recurso_oneroso_prestador,
    safe_cast(
        investimento_recurso_nao_oneroso_prestador as float64
    ) investimento_recurso_nao_oneroso_prestador,
    safe_cast(investimento_total_prestador as float64) investimento_total_prestador,
    safe_cast(investimento_agua_municipio as float64) investimento_agua_municipio,
    safe_cast(investimento_esgoto_municipio as float64) investimento_esgoto_municipio,
    safe_cast(investimento_outro_municipio as float64) investimento_outro_municipio,
    safe_cast(
        investimento_recurso_proprio_municipio as float64
    ) investimento_recurso_proprio_municipio,
    safe_cast(
        investimento_recurso_oneroso_municipio as float64
    ) investimento_recurso_oneroso_municipio,
    safe_cast(
        investimento_recurso_nao_oneroso_municipio as float64
    ) investimento_recurso_nao_oneroso_municipio,
    safe_cast(investimento_total_municipio as float64) investimento_total_municipio,
    safe_cast(investimento_agua_estado as float64) investimento_agua_estado,
    safe_cast(investimento_esgoto_estado as float64) investimento_esgoto_estado,
    safe_cast(investimento_outro_estado as float64) investimento_outro_estado,
    safe_cast(
        investimento_recurso_proprio_estado as float64
    ) investimento_recurso_proprio_estado,
    safe_cast(
        investimento_recurso_oneroso_estado as float64
    ) investimento_recurso_oneroso_estado,
    safe_cast(
        investimento_recurso_nao_oneroso_estado as float64
    ) investimento_recurso_nao_oneroso_estado,
    safe_cast(investimento_total_estado as float64) investimento_total_estado,
from {{ set_datalake_project("br_mdr_snis_staging.prestador_agua_esgoto") }} as t
