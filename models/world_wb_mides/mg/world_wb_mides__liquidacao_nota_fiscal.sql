-- Minas Gerais only. Generated from the pinned source header contract
-- (`code/mg_source_headers.json`) plus the validated key definitions; see
-- `ARCHITECTURE_MG.md`. MG is the only state publishing this stream, so the
-- table's Coverage records sigla_uf = MG rather than implying national scope.
{{
    config(
        alias="liquidacao_nota_fiscal",
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
    p_liquidacao as (
        select distinct id_liquidacao_bd, id_liquidacao
        from {{ ref("world_wb_mides__liquidacao_mg") }}
    )
select
    safe_cast(t.ano as int64) as ano,
    safe_cast(t.mes as int64) as mes,
    'MG' as sigla_uf,
    safe_cast(t.id_municipio as string) as id_municipio,
    safe_cast(
        concat(
            p_liquidacao.id_liquidacao_bd, ' ', ifnull(t.seq_nota_fiscal, '')
        ) as string
    ) as id_liquidacao_nota_fiscal_bd,
    safe_cast(p_liquidacao.id_liquidacao_bd as string) as id_liquidacao_bd,
    safe_cast(t.seq_liq_nota_fiscal as string) as id_liq_nota_fiscal,
    safe_cast(t.seq_nota_fiscal as string) as id_nota_fiscal,
    safe_cast(t.orgao as string) as orgao,
    safe_cast(t.seq_empenho as string) as id_empenho,
    safe_cast(t.seq_rsp as string) as id_rsp,
    safe_cast(t.seq_liquidacao as string) as id_liquidacao,
    safe_cast(t.num_doc_emitente as string) as numero_doc_emitente,
    safe_cast(t.nom_emitente as string) as nome_emitente
from
    {{ set_datalake_project("world_wb_mides_staging.raw_liquidacao_nota_fiscal_mg") }}
    as t
left join p_liquidacao on t.seq_liquidacao = p_liquidacao.id_liquidacao
