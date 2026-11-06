{{
    config(
        alias="proagro_sumula_julgamento",
        schema="br_bcb_sicor",
        materialized="table",
        partition_by={
            "field": "ano_emissao",
            "data_type": "int64",
            "range": {"start": 2013, "end": 2031, "interval": 1},
        },
        pre_hook="             BEGIN                 DROP ALL ROW ACCESS POLICIES ON {{ this }};             EXCEPTION WHEN ERROR THEN                 SELECT 1;              END;         ",
    )
}}


select
    safe_cast(ano_emissao as int64) ano_emissao,
    safe_cast(mes_emissao as int64) mes_emissao,
    safe_cast(t.id_referencia_bacen as string) id_referencia_bacen,
    safe_cast(t.numero_ordem as string) numero_ordem,
    safe_cast(ltrim(id_decisao, '0') as string) id_decisao,
    safe_cast(ltrim(id_instancia, '0') as string) id_instancia,
    safe_cast(ltrim(id_status, '0') as string) id_status,
    safe_cast(indicador_segunda_vistoria as string) indicador_segunda_vistoria,
    safe_cast(parse_date("%d/%m/%Y", data_base) as date) data_base,
    safe_cast(parse_date("%d/%m/%Y", data_inclusao) as date) data_inclusao,
    safe_cast(parse_date("%d/%m/%Y", data_decisao) as date) data_decisao,
    safe_cast(valor_orcamento_enquadrado as float64) valor_orcamento_enquadrado,
    safe_cast(
        valor_credito_custeio_utilizado as float64
    ) valor_credito_custeio_utilizado,
    safe_cast(
        valor_recurso_proprio_utilizado as float64
    ) valor_recurso_proprio_utilizado,
    safe_cast(valor_receitas_consideradas as float64) valor_receitas_consideradas,
    safe_cast(
        valor_encargos_credito_utilizado as float64
    ) valor_encargos_credito_utilizado,
    safe_cast(
        valor_cobertura_anterior_credito_custeio as float64
    ) valor_cobertura_anterior_credito_custeio,
    safe_cast(
        valor_cobertura_anterior_recurso_proprio as float64
    ) valor_cobertura_anterior_recurso_proprio,
    safe_cast(
        valor_cobertura_anterior_garantia_renda_minima as float64
    ) valor_cobertura_anterior_garantia_renda_minima,
    safe_cast(
        valor_cobertura_anterior_parcela_investimento_proagro_mais as float64
    ) valor_cobertura_anterior_parcela_investimento_proagro_mais,
    safe_cast(
        valor_remuneracao_encarregado_comprovacao_perdas as float64
    ) valor_remuneracao_encarregado_comprovacao_perdas,
    safe_cast(
        valor_remuneracao_anterior_encarregado_comprovacao_perdas as float64
    ) valor_remuneracao_anterior_encarregado_comprovacao_perdas,
    safe_cast(
        valor_demais_despesas_comprovacao_perdas as float64
    ) valor_demais_despesas_comprovacao_perdas,
    safe_cast(
        valor_demais_despesas_anteriores_comprovacao_perdas as float64
    ) valor_demais_despesas_anteriores_comprovacao_perdas,
    safe_cast(valor_perdas_nao_amparadas as float64) valor_perdas_nao_amparadas,
    safe_cast(valor_bonus_pgpaf as float64) valor_bonus_pgpaf,
    safe_cast(valor_deducoes_legais as float64) valor_deducoes_legais,
    safe_cast(percentual_redutor_cobertura as float64) percentual_redutor_cobertura,
    safe_cast(
        quantidade_dias_uteis_atraso_perito as int64
    ) quantidade_dias_uteis_atraso_perito
from
    {{ set_datalake_project("br_bcb_sicor_staging.proagro_sumula_julgamento") }}
    as t {{ add_ano_mes_operacao_data(["id_referencia_bacen", "numero_ordem"]) }}
