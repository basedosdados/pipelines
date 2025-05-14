{{
    config(
        alias="microdados_operacao",
        schema="br_bcb_sicor",
        materialized="table",
        partition_by={
            "field": "ano",
            "data_type": "int64",
            "range": {"start": 2013, "end": 2024, "interval": 1},
        },
        cluster_by=["sigla_uf", "plano_safra_emissao"],
    )
}}

select
    safe_cast(ano as int64) ano,
    safe_cast(mes as int64) mes,
    safe_cast(data_emissao as date) data_emissao,
    safe_cast(ano_vencimento as int64) ano_vencimento,
    safe_cast(mes_vencimento as int64) mes_vencimento,
    safe_cast(data_vencimento as date) data_vencimento,
    safe_cast(plano_safra_emissao as string) plano_safra_emissao,
    safe_cast(plano_safra_vencimento as string) plano_safra_vencimento,
    safe_cast(sigla_uf as string) sigla_uf,
    safe_cast(id_referencia_bacen as string) id_referencia_bacen,
    safe_cast(numero_ordem as string) numero_ordem,
    safe_cast(id_categoria_emitente as string) id_categoria_emitente,
    safe_cast(id_empreendimento as string) id_empreendimento,
    safe_cast(id_fase_ciclo_producao as string) id_fase_ciclo_producao,
    safe_cast(id_fonte_recurso as string) id_fonte_recurso,
    safe_cast(id_instrumento_credito as string) id_instrumento_credito,
    safe_cast(id_programa as string) id_programa,
    safe_cast(
        id_referencia_bacen_investimento as string
    ) id_referencia_bacen_investimento,
    safe_cast(id_subprograma as string) id_subprograma,
    safe_cast(id_tipo_agricultura as string) id_tipo_agricultura,
    safe_cast(id_tipo_cultivo as string) id_tipo_cultivo,
    safe_cast(id_tipo_encargo_financeiro as string) id_tipo_encargo_financeiro,
    safe_cast(id_tipo_grao_semente as string) id_tipo_grao_semente,
    safe_cast(id_tipo_integracao_consorcio as string) id_tipo_integracao_consorcio,
    safe_cast(id_tipo_irrigacao as string) id_tipo_irrigacao,
    safe_cast(id_tipo_seguro as string) id_tipo_seguro,
    safe_cast(cnpj_agente_investimento as string) cnpj_agente_investimento,
    safe_cast(
        cnpj_basico_instituicao_financeira as string
    ) cnpj_basico_instituicao_financeira,
    safe_cast(
        id_contrato_sistema_tesouro_nacional as string
    ) id_contrato_sistema_tesouro_nacional,
    safe_cast(cnpj_cadastrante as string) cnpj_cadastrante,
    safe_cast(data_fim_colheita as date) data_fim_colheita,
    safe_cast(data_fim_plantio as date) data_fim_plantio,
    safe_cast(data_inicio_colheita as date) data_inicio_colheita,
    safe_cast(data_inicio_plantio as date) data_inicio_plantio,
    safe_cast(area_financiada as float64) area_financiada,
    safe_cast(valor_aliquota_proagro as float64) valor_aliquota_proagro,
    safe_cast(valor_parcela_credito as float64) valor_parcela_credito,
    safe_cast(valor_prestacao_investimento as float64) valor_prestacao_investimento,
    safe_cast(valor_recurso_proprio as float64) valor_recurso_proprio,
    safe_cast(valor_receita_bruta_esperada as float64) valor_receita_bruta_esperada,
    safe_cast(valor_recurso_proprio_srv as float64) valor_recurso_proprio_srv,
    safe_cast(
        valor_quantidade_itens_financiados as float64
    ) valor_quantidade_itens_financiados,
    safe_cast(valor_produtividade_obtida as float64) valor_produtividade_obtida,
    safe_cast(valor_previsao_producao as float64) valor_previsao_producao,
    safe_cast(taxa_juro as float64) taxa_juro,
    safe_cast(
        taxa_juro_encargo_financeiro_posfixado as float64
    ) taxa_juro_encargo_financeiro_posfixado,
    safe_cast(
        valor_percentual_custo_efetivo_total as float64
    ) valor_percentual_custo_efetivo_total,
    safe_cast(
        valor_percentual_risco_fundo_constitucional as float64
    ) valor_percentual_risco_fundo_constitucional,
    safe_cast(valor_percentual_risco_stn as float64) valor_percentual_risco_stn
from {{ set_datalake_project("br_bcb_sicor_staging.microdados_operacao") }} as t
