-- Minas Gerais only. Generated from the pinned source header contract
-- (`code/mg_source_headers.json`) plus the validated key definitions; see
-- `ARCHITECTURE_MG.md`. MG is the only state publishing this stream, so the
-- table's Coverage records sigla_uf = MG rather than implying national scope.
{{
    config(
        alias="registro_preco_adesao_cotacao",
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
    -- `world_wb_mides__registro_preco_adesao_item`. Reading it from `ref()` rather than
    -- re-deriving the concat here means the two can never drift: one
    -- definition, one place. The inline copy this replaces had already drifted
    -- from the parent's key, so the foreign key it published matched no parent
    -- row.
    --
    -- The join is scoped by municipality and sequence, NOT by exercise. A
    -- child routinely cites a parent recorded in an earlier exercise, so
    -- matching on the child's own `ano` discards those references.
    -- Here it left `_bd` NULL for 48,909 of 1,259,477 rows (3.88%),
    -- against 3,783 (0.30%) once the exercise is out of the join.
    --
    -- Dropping it cannot fan out: `(id_municipio, seq_item_reg_adesao)` spans more than
    -- one exercise in 137 of the parent's groups, and the `min` below
    -- collapses each such group to one key deterministically.
    -- Measured on the staging parquet with DuckDB, 2026-09-28.
    p_registro_preco_adesao_item as (
        select
            id_municipio,
            id_item_reg_adesao,
            min(id_registro_preco_adesao_item_bd) as id_registro_preco_adesao_item_bd
        from {{ ref("world_wb_mides__registro_preco_adesao_item") }}
        group by 1, 2
    )
select
    safe_cast(t.ano as int64) as ano,
    safe_cast(t.mes as int64) as mes,
    'MG' as sigla_uf,
    safe_cast(t.id_municipio as string) as id_municipio,
    -- No stable column makes this key unique: colliding source rows differ
    -- only in a measure, or are identical apart from the portal's own
    -- sequence. `seq_cot_reg_adesao` is appended so the key identifies a row, at the
    -- cost of churning between extractions -- see
    -- `reference_tce_mg_seq_empenho_unstable`. Decided 2026-09-24.
    safe_cast(
        concat(
            p_registro_preco_adesao_item.id_registro_preco_adesao_item_bd,
            ' ',
            ifnull(t.data_cotacao, ''),
            ' ',
            ifnull(t.num_quant_item, ''),
            ' ',
            ifnull(t.valor_unitario, ''),
            ' ',
            ifnull(t.seq_cot_reg_adesao, '')
        ) as string
    ) as id_registro_preco_adesao_cotacao_bd,
    safe_cast(
        p_registro_preco_adesao_item.id_registro_preco_adesao_item_bd as string
    ) as id_registro_preco_adesao_item_bd,
    safe_cast(t.seq_cot_reg_adesao as string) as id_cot_reg_adesao,
    safe_cast(t.seq_item_reg_adesao as string) as id_item_reg_adesao,
    safe_cast(t.seq_reg_adesao as string) as id_reg_adesao,
    safe_cast(t.orgao as string) as orgao,
    safe_cast(t.data_cotacao as date) as data_cotacao,
    safe_cast(t.valor_unitario as float64) as valor_unitario,
    safe_cast(t.num_quant_item as string) as numero_quant_item
from
    {{
        set_datalake_project(
            "world_wb_mides_staging.raw_registro_preco_adesao_cotacao_mg"
        )
    }} as t
left join
    p_registro_preco_adesao_item
    on t.id_municipio = p_registro_preco_adesao_item.id_municipio
    and t.seq_item_reg_adesao = p_registro_preco_adesao_item.id_item_reg_adesao
