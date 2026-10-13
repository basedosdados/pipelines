{{
    config(
        alias="proagro_rcp",
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
    safe_cast(ltrim(id_evento, '0') as string) id_evento,
    safe_cast(ltrim(id_status, '0') as string) id_status,
    safe_cast(ltrim(id_tipo, '0') as string) id_tipo,
    safe.parse_date("%d/%m/%Y", data_entrega) data_entrega,
    safe.parse_date("%d/%m/%Y", data_visita) data_visita,
    {{ parse_data_agronomica_sicor("data_inicio_evento") }} data_inicio_evento,
    {{ parse_data_agronomica_sicor("data_fim_evento") }} data_fim_evento,
    {{ parse_data_agronomica_sicor("data_inicio_plantio") }} data_inicio_plantio,
    {{ parse_data_agronomica_sicor("data_fim_plantio") }} data_fim_plantio,
    {{ parse_data_agronomica_sicor("data_inicio_colheita") }} data_inicio_colheita,
    {{ parse_data_agronomica_sicor("data_fim_colheita") }} data_fim_colheita,
    safe_cast(area as float64) area,
    safe_cast(valor_previsao_producao as float64) valor_previsao_producao,
    safe_cast(valor_receita_prevista as float64) valor_receita_prevista,
    safe_cast(quantidade_dias_ciclo_cultivar as int64) quantidade_dias_ciclo_cultivar
from
    {{ set_datalake_project("br_bcb_sicor_staging.proagro_rcp") }}
    as t {{ add_ano_mes_operacao_data(["id_referencia_bacen", "numero_ordem"]) }}
