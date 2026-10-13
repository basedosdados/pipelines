-- Minas Gerais only. Generated from the pinned source header contract
-- (`code/mg_source_headers.json`) plus the validated key definitions; see
-- `ARCHITECTURE_MG.md`. MG is the only state publishing this stream, so the
-- table's Coverage records sigla_uf = MG rather than implying national scope.
{{
    config(
        alias="restos_pagar_movimentacao_credor",
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
    -- `world_wb_mides__restos_pagar_movimentacao`. Reading it from `ref()` rather than
    -- re-deriving the concat here means the two can never drift: one
    -- definition, one place. The inline copy this replaces had already drifted
    -- from the parent's key, so the foreign key it published matched no parent
    -- row.
    --
    -- The join is scoped by municipality and sequence, NOT by exercise. A
    -- child routinely cites a parent recorded in an earlier exercise, so
    -- matching on the child's own `ano` discards those references.
    -- Here it changes nothing measurable (0 of 265,669 rows,
    -- 0.00%, unresolved either way), but the join is wrong in the same
    -- way and is corrected for consistency with its siblings.
    --
    -- Dropping it cannot fan out: `(id_municipio, seq_mov_rsp)` spans more than
    -- one exercise in 0 of the parent's groups, and the `min` below would
    -- collapse any future ambiguity to one key deterministically.
    -- Measured on the staging parquet with DuckDB, 2026-09-28.
    p_restos_pagar_movimentacao as (
        select
            id_municipio,
            id_mov_rsp,
            min(id_restos_pagar_movimentacao_bd) as id_restos_pagar_movimentacao_bd
        from {{ ref("world_wb_mides__restos_pagar_movimentacao") }}
        group by 1, 2
    )
select
    safe_cast(t.ano as int64) as ano,
    safe_cast(t.mes as int64) as mes,
    'MG' as sigla_uf,
    safe_cast(t.id_municipio as string) as id_municipio,
    safe_cast(
        concat(
            p_restos_pagar_movimentacao.id_restos_pagar_movimentacao_bd,
            ' ',
            ifnull(t.num_doc_credor, '')
        ) as string
    ) as id_restos_pagar_movimentacao_credor_bd,
    safe_cast(
        p_restos_pagar_movimentacao.id_restos_pagar_movimentacao_bd as string
    ) as id_restos_pagar_movimentacao_bd,
    safe_cast(t.seq_cred_mov_rsp as string) as id_cred_mov_rsp,
    safe_cast(t.seq_mov_rsp as string) as id_mov_rsp,
    safe_cast(t.orgao as string) as orgao,
    safe_cast(t.num_doc_credor as string) as numero_doc_credor,
    safe_cast(t.nom_credor as string) as nome_credor
from
    {{
        set_datalake_project(
            "world_wb_mides_staging.raw_restos_pagar_movimentacao_credor_mg"
        )
    }} as t
left join
    p_restos_pagar_movimentacao
    on t.id_municipio = p_restos_pagar_movimentacao.id_municipio
    and t.seq_mov_rsp = p_restos_pagar_movimentacao.id_mov_rsp
