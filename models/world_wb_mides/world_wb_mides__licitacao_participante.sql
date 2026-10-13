{{
    config(
        alias="licitacao_participante",
        schema="world_wb_mides",
        materialized="table",
        partition_by={
            "field": "ano",
            "data_type": "int64",
            "range": {"start": 2009, "end": 2026, "interval": 1},
        },
        cluster_by=["sigla_uf"],
        labels={"tema": "economia"},
    )
}}

with
    -- (municipality, exercise) pairs the current TCE-MG harvest covers. Taken
    -- from the raw mirrors, not from `ref()` of the MG model: the model is
    -- ephemeral, so referencing it here would inline its whole query a second
    -- time and double the scan. Coverage is a property of the HARVEST, not of
    -- one stream -- a municipality-exercise we re-harvested is governed by the
    -- new vintage even in a stream where it happens to have no rows.
    mg_cobertura as (
        select distinct id_municipio, safe_cast(ano as int64) as ano
        from {{ set_datalake_project("world_wb_mides_staging.raw_licitacao_mg") }}
        union distinct
        select distinct id_municipio, safe_cast(ano as int64) as ano
        from {{ set_datalake_project("world_wb_mides_staging.raw_dispensa_mg") }}
    )
select
    safe_cast(ano as int64) ano,
    safe_cast(sigla_uf as string) sigla_uf,
    safe_cast(id_municipio as string) id_municipio,
    safe_cast(orgao as string) orgao,
    safe_cast(id_unidade_gestora as string) id_unidade_gestora,
    safe_cast(id_licitacao_bd as string) id_licitacao_bd,
    safe_cast(id_licitacao as string) id_licitacao,
    safe_cast(id_dispensa as string) id_dispensa,
    safe_cast(razao_social as string) razao_social,
    safe_cast(documento as string) documento,
    safe_cast(habilitado as int64) habilitado,
    safe_cast(classificado as int64) classificado,
    safe_cast(vencedor as int64) vencedor,
    safe_cast(endereco as string) endereco,
    safe_cast(cep as string) cep,
    safe_cast(municipio_participante as string) municipio_participante,
    safe_cast(tipo as string) tipo
from {{ set_datalake_project("world_wb_mides_staging.licitacao_participante") }} as t
-- MG is supplied by its own state model, which rebuilds MG 2014-2026 from the
-- current TCE-MG source. It is excluded from this arm so pre-2022 MG rows are
-- not duplicated -- EXCEPT for the municipality-exercises the portal has since
-- withdrawn. TCE-MG retroactively removed 187 municipalities from exercise 2017
-- and 101 from 2018, so a straight rebuild drops 3.4% of the published MG rows
-- (621,646 local vs 643,442 published over 2014-2021, measured 2026-09-24).
-- Those rows still exist in this monolithic table, so MG is kept here for
-- exactly the (municipality, exercise) pairs the state model does not cover.
-- This is the same per-municipality union the spend mirrors get from the GCS
-- overlay in `code/upload_mg.py`; procurement has no such mirror, so it is done
-- in SQL instead.
where
    t.sigla_uf != 'MG'
    or not exists (
        select 1
        from mg_cobertura as c
        where c.id_municipio = t.id_municipio and c.ano = safe_cast(t.ano as int64)
    )

union all

select *
from {{ ref("world_wb_mides__licitacao_participante_mg") }} as t
