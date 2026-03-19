{{
    config(
        alias="liberacao",
        schema="br_bcb_sicor",
        materialized="table",
        partition_by={
            "field": "ano",
            "data_type": "int64",
            "range": {"start": 2013, "end": 2026, "interval": 1},
        },
        cluster_by=["mes"],
        pre_hook="             BEGIN                 DROP ALL ROW ACCESS POLICIES ON {{ this }};             EXCEPTION WHEN ERROR THEN                 SELECT 1;              END;         ",
    )
}}


select
    safe_cast(ano_emissao as int64) ano_emissao,
    safe_cast(mes_emissao as int64) mes_emissao,
    safe_cast(
        extract(year from parse_date("%d/%m/%Y", data_liberacao)) as int64
    ) as ano,
    safe_cast(extract(month from parse_date("%d/%m/%Y", data_liberacao)) as int64) mes,
    safe_cast(parse_date("%d/%m/%Y", data_liberacao) as date) as data_liberacao,
    safe_cast(id_referencia_bacen as string) id_referencia_bacen,
    safe_cast(numero_ordem as string) numero_ordem,
    safe_cast(valor_liberado as float64) valor_liberado
from
    {{ set_datalake_project("br_bcb_sicor_staging.liberacao") }} as t
    {{ add_ano_mes_operacao_data(["id_referencia_bacen", "numero_ordem"]) }}
where
    safe_cast(extract(month from parse_date("%d/%m/%Y", data_liberacao)) as int64)
    not in (1905, 201, 2012, 2000, 2011)
