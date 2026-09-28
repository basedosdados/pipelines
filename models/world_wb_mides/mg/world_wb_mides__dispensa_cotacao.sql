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
    -- The join is scoped by municipality and sequence, NOT by exercise. A
    -- child routinely cites a parent recorded in an earlier exercise, so
    -- matching on the child's own `ano` discards those references.
    -- Here it left `_bd` NULL for 4 of 1,867,194 rows (0.00%),
    -- against 0 (0.00%) once the exercise is out of the join.
    --
    -- Dropping it cannot fan out: `(id_municipio, seq_item_dispensa)` spans more than
    -- one exercise in 801 of the parent's groups, and the `min` below
    -- collapses each such group to one key deterministically.
    -- Measured on the staging parquet with DuckDB, 2026-09-28.
    p_dispensa_item as (
        select
            id_municipio,
            id_item_dispensa,
            min(id_dispensa_item_bd) as id_dispensa_item_bd
        from {{ ref("world_wb_mides__dispensa_item") }}
        group by 1, 2
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
    and t.seq_item_dispensa = p_dispensa_item.id_item_dispensa
