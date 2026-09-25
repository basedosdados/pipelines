-- Minas Gerais only. Generated from the pinned source header contract
-- (`code/mg_source_headers.json`) plus the validated key definitions; see
-- `ARCHITECTURE_MG.md`. MG is the only state publishing this stream, so the
-- table's Coverage records sigla_uf = MG rather than implying national scope.
{{
    config(
        alias="contrato_contabilizacao",
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
    -- `world_wb_mides__contrato`. Reading it from `ref()` rather than
    -- re-deriving the concat here means the two can never drift: one
    -- definition, one place. The inline copies this replaces had already
    -- drifted -- they predated the `seq_contrato`/`seq_dispensa` tie-breakers,
    -- so the `id_contrato_bd` they published matched no row of the parent.
    --
    -- The join is scoped by municipality and exercise because `seq_contrato`
    -- recurs across them. Within a scope, 1,983 of 1,274,719 groups (0.16%)
    -- carry two contracts differing only in `seq_dispensa`: the parent key
    -- separates them, a child row cannot, so `min` attaches the child to one
    -- of them deterministically rather than fanning it out. Measured on the
    -- parquet, 2026-09-24.
    p_contrato as (
        select id_municipio, ano, id_contrato, min(id_contrato_bd) as id_contrato_bd
        from {{ ref("world_wb_mides__contrato") }}
        group by 1, 2, 3
    )
select
    safe_cast(t.ano as int64) as ano,
    safe_cast(t.mes as int64) as mes,
    'MG' as sigla_uf,
    safe_cast(t.id_municipio as string) as id_municipio,
    safe_cast(
        concat(
            p_contrato.id_contrato_bd,
            ' ',
            ifnull(t.num_doc_representante, ''),
            ' ',
            ifnull(t.num_doc_credor, '')
        ) as string
    ) as id_contrato_contabilizacao_bd,
    safe_cast(p_contrato.id_contrato_bd as string) as id_contrato_bd,
    safe_cast(t.seq_contratado as string) as id_contratado,
    safe_cast(t.seq_contrato as string) as id_contrato,
    safe_cast(t.orgao as string) as orgao,
    safe_cast(t.num_doc_representante as string) as numero_doc_representante,
    safe_cast(t.dsc_nome_representante as string) as nome_representante,
    safe_cast(t.num_doc_credor as string) as numero_doc_credor,
    safe_cast(t.dsc_nome_credor as string) as nome_credor
from
    {{ set_datalake_project("world_wb_mides_staging.raw_contrato_contabilizacao_mg") }}
    as t
left join
    p_contrato
    on t.id_municipio = p_contrato.id_municipio
    and safe_cast(t.ano as int64) = p_contrato.ano
    and t.seq_contrato = p_contrato.id_contrato
