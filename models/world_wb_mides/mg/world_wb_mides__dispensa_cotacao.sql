-- Minas Gerais only. Generated from the pinned source header contract
-- (`code/mg_source_headers.json`) plus the validated key definitions; see
-- `ARCHITECTURE_MG.md`. MG is the only state publishing this stream, so the
-- table's Coverage records sigla_uf = MG rather than implying national scope.
{{
    config(
        alias="dispensa_cotacao",
        schema="world_wb_mides",
        materialized="table",
        partition_by={
            "field": "ano",
            "data_type": "int64",
            "range": {"start": 2014, "end": 2031, "interval": 1},
        },
        cluster_by=["id_municipio"],
        labels={"tema": "economia"},
    )
}}
with
    -- The parent's `_bd` key is NOT in the staging mirror -- it is built by
    -- `world_wb_mides__dispensa_item`. Reading it from `ref()` rather than
    -- re-deriving the concat here means the two can never drift: one
    -- definition, one place. The inline copy this replaces had already drifted
    -- from the parent's key, so the foreign key it published matched no parent
    -- row.
    --
    -- The join is scoped by municipality and exercise because `seq_item_dispensa`
    -- recurs across them; within a scope it determines the parent key
    -- (2,155,777 groups, 0 ambiguous, measured on the parquet 2026-09-24),
    -- so the join cannot fan out.
    p_dispensa_item as (
        select
            id_municipio,
            ano,
            id_item_dispensa,
            min(id_dispensa_item_bd) as id_dispensa_item_bd
        from {{ ref("world_wb_mides__dispensa_item") }}
        group by 1, 2, 3
    )
select
    safe_cast(t.ano as int64) as ano,
    safe_cast(t.mes as int64) as mes,
    'MG' as sigla_uf,
    safe_cast(t.id_municipio as string) as id_municipio,
    safe_cast(
        concat(
            p_dispensa_item.id_dispensa_item_bd,
            ' ',
            ifnull(t.num_quant_item, ''),
            ' ',
            ifnull(t.valor_preco_unit, '')
        ) as string
    ) as id_dispensa_cotacao_bd,
    safe_cast(p_dispensa_item.id_dispensa_item_bd as string) as id_dispensa_item_bd,
    safe_cast(t.seq_cot_dispensa as string) as id_cot_dispensa,
    safe_cast(t.seq_item_dispensa as string) as id_item_dispensa,
    safe_cast(t.seq_dispensa as string) as id_dispensa,
    safe_cast(t.orgao as string) as orgao,
    safe_cast(t.valor_preco_unit as float64) as valor_unitario,
    safe_cast(t.num_quant_item as string) as numero_quant_item
from {{ set_datalake_project("world_wb_mides_staging.raw_dispensa_cotacao_mg") }} as t
left join
    p_dispensa_item
    on t.id_municipio = p_dispensa_item.id_municipio
    and safe_cast(t.ano as int64) = p_dispensa_item.ano
    and t.seq_item_dispensa = p_dispensa_item.id_item_dispensa
