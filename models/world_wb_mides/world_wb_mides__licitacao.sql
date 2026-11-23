{{
    config(
        alias="licitacao",
        schema="world_wb_mides",
        materialized="table",
        partition_by={
            "field": "ano",
            "data_type": "int64",
            "range": {"start": 2009, "end": 2026, "interval": 1},
        },
        cluster_by=["mes", "sigla_uf"],
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
    safe_cast(mes as int64) mes,
    safe_cast(sigla_uf as string) sigla_uf,
    safe_cast(id_municipio as string) id_municipio,
    safe_cast(orgao as string) orgao,
    safe_cast(id_unidade_gestora as string) id_unidade_gestora,
    safe_cast(id_licitacao_bd as string) id_licitacao_bd,
    safe_cast(id_licitacao as string) id_licitacao,
    safe_cast(id_dispensa as string) id_dispensa,
    safe_cast(ano_processo as int64) ano_processo,
    safe_cast(data_abertura as date) data_abertura,
    safe_cast(data_edital as date) data_edital,
    safe_cast(data_homologacao as date) data_homologacao,
    safe_cast(data_publicacao_dispensa as date) data_publicacao_dispensa,
    safe_cast(descricao_objeto as string) descricao_objeto,
    safe_cast(natureza_objeto as string) natureza_objeto,
    safe_cast(modalidade as string) modalidade,
    safe_cast(natureza_processo as string) natureza_processo,
    safe_cast(tipo as string) tipo,
    safe_cast(forma_pagamento as string) forma_pagamento,
    safe_cast(valor_orcamento as float64) valor_orcamento,
    safe_cast(valor as float64) valor,
    safe_cast(valor_corrigido as float64) valor_corrigido,
    safe_cast(situacao as string) situacao,
    safe_cast(estagio as string) estagio,
    safe_cast(preferencia_micro_pequena as string) preferencia_micro_pequena,
    safe_cast(exclusiva_micro_pequena as string) exclusiva_micro_pequena,
    safe_cast(contratacao as string) contratacao,
    safe_cast(quantidade_convidados as int64) quantidade_convidados,
    safe_cast(tipo_cadastro as string) tipo_cadastro,
    safe_cast(carona as string) carona,
    safe_cast(covid_19 as string) covid_19
from {{ set_datalake_project("world_wb_mides_staging.licitacao") }} as t
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
from {{ ref("world_wb_mides__licitacao_mg") }}
