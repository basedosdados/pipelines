-- Minas Gerais only. Generated from the pinned source header contract
-- (`code/mg_source_headers.json`) plus the validated key definitions; see
-- `ARCHITECTURE_MG.md`. MG is the only state publishing this stream, so the
-- table's Coverage records sigla_uf = MG rather than implying national scope.
{{
    config(
        alias="empenho_credor",
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
    -- The parent's `_bd` key is NOT in the staging mirror -- it is built by the
    -- state model. Reading it from `ref()` rather than re-deriving the concat
    -- here means the two can never drift: one definition, one place.
    -- `seq_empenho`/`seq_liquidacao`/`seq_pagamento` are unambiguous across
    -- municipalities within one extraction (0 shared values over 120
    -- municipalities x 2016/2020/2024, measured 2026-09-24), so the join needs
    -- no further scoping.
    p_empenho as (
        select distinct id_empenho_bd, id_empenho
        from {{ ref("world_wb_mides__empenho_mg") }}
    )
select
    safe_cast(t.ano as int64) as ano,
    safe_cast(t.mes as int64) as mes,
    'MG' as sigla_uf,
    safe_cast(t.id_municipio as string) as id_municipio,
    -- A residual handful of source rows repeat the key above; `seq_emp_credor`
    -- separates them. See the note on the other seq-bearing keys.
    safe_cast(
        concat(
            p_empenho.id_empenho_bd,
            ' ',
            ifnull(t.num_doc_credor, ''),
            ' ',
            ifnull(t.seq_emp_credor, '')
        ) as string
    ) as id_empenho_credor_bd,
    safe_cast(p_empenho.id_empenho_bd as string) as id_empenho_bd,
    safe_cast(t.seq_emp_credor as string) as id_emp_credor,
    safe_cast(t.seq_empenho as string) as id_empenho,
    safe_cast(t.orgao as string) as orgao,
    safe_cast(t.id_unidade_gestora as string) as id_unidade_gestora,
    safe_cast(t.num_doc_credor as string) as numero_doc_credor,
    safe_cast(t.nom_credor as string) as nome_credor
from {{ set_datalake_project("world_wb_mides_staging.raw_empenho_credor_mg") }} as t
left join p_empenho on t.seq_empenho = p_empenho.id_empenho
