{{
    config(
        alias="saldo",
        schema="br_bcb_sicor",
        materialized="incremental",
        incremental_strategy="insert_overwrite",
        partition_by={
            "field": "ano",
            "data_type": "int64",
            "range": {"start": 2013, "end": 2026, "interval": 1},
        },
        cluster_by=["mes"],
        pre_hook="             BEGIN                 DROP ALL ROW ACCESS POLICIES ON {{ this }};             EXCEPTION WHEN ERROR THEN                 SELECT 1;              END;         ",
    )
}}


select distinct
    safe_cast(ano_emissao as int64) ano_emissao,
    safe_cast(mes_emissao as int64) mes_emissao,
    safe_cast(ano_base as int64) ano,
    safe_cast(mes_base as int64) mes,
    safe_cast(id_referencia_bacen as string) id_referencia_bacen,
    safe_cast(numero_ordem as string) numero_ordem,
    safe_cast(id_situacao_operacao as string) id_situacao_operacao,
    safe_cast(valor_medio_diario as float64) valor_medio_diario,
    safe_cast(valor_medio_diario_vincendo as float64) valor_medio_diario_vincendo,
    safe_cast(valor_ultimo_dia as float64) valor_ultimo_dia
from
    {{ set_datalake_project("br_bcb_sicor_staging.saldo") }} as t
    {{ add_ano_mes_operacao_data(["id_referencia_bacen", "numero_ordem"]) }}
where
    ano_emissao is not null
    {% if is_incremental() %}
        -- a pipeline é settada para atualizar sempre um arquivo de ano; logo, precisa
        -- de um insert_overwrite que sobrescreva o ano atual com os dados mais
        -- recentes;
        and cast(ano_base as int64) = (select max(ano_emissao) from {{ this }})
    {% endif %}
