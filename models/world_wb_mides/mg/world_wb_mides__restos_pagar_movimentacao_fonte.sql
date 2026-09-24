-- Minas Gerais only. Generated from the pinned source header contract
-- (`code/mg_source_headers.json`) plus the validated key definitions; see
-- `ARCHITECTURE_MG.md`. MG is the only state publishing this stream, so the
-- table's Coverage records sigla_uf = MG rather than implying national scope.
{{
    config(
        alias="restos_pagar_movimentacao_fonte",
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
    -- The join is scoped by municipality and exercise because `seq_mov_rsp`
    -- recurs across them; within a scope it determines the parent key
    -- (2,624,515 groups, 0 ambiguous, measured on the parquet 2026-09-24),
    -- so the join cannot fan out.
    p_restos_pagar_movimentacao as (
        select
            id_municipio,
            ano,
            id_mov_rsp,
            min(id_restos_pagar_movimentacao_bd) as id_restos_pagar_movimentacao_bd
        from {{ ref("world_wb_mides__restos_pagar_movimentacao") }}
        group by 1, 2, 3
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
            ifnull(t.dsc_fonte_recurso, ''),
            ' ',
            ifnull(t.dsc_cod_orcamentario, '')
        ) as string
    ) as id_restos_pagar_movimentacao_fonte_bd,
    safe_cast(
        p_restos_pagar_movimentacao.id_restos_pagar_movimentacao_bd as string
    ) as id_restos_pagar_movimentacao_bd,
    safe_cast(t.seq_mov_rsp_fonte as string) as id_mov_rsp_fonte,
    safe_cast(t.seq_mov_rsp as string) as id_mov_rsp,
    safe_cast(t.orgao as string) as orgao,
    safe_cast(t.dsc_fonte_recurso as string) as fonte_recurso,
    safe_cast(t.dsc_cod_orcamentario as string) as cod_orcamentario,
    safe_cast(t.valor_mov_fonte as float64) as valor_mov_fonte
from
    {{
        set_datalake_project(
            "world_wb_mides_staging.raw_restos_pagar_movimentacao_fonte_mg"
        )
    }} as t
left join
    p_restos_pagar_movimentacao
    on t.id_municipio = p_restos_pagar_movimentacao.id_municipio
    and safe_cast(t.ano as int64) = p_restos_pagar_movimentacao.ano
    and t.seq_mov_rsp = p_restos_pagar_movimentacao.id_mov_rsp
