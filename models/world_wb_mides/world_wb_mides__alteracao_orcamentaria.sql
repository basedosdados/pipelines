-- Minas Gerais only. Generated from the pinned source header contract
-- (`code/mg_source_headers.json`) plus the validated key definitions; see
-- `ARCHITECTURE_MG.md`. MG is the only state publishing this stream, so the
-- table's Coverage records sigla_uf = MG rather than implying national scope.
{{
    config(
        alias="alteracao_orcamentaria",
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
    p_decreto as (
        select distinct
            t.seq_decreto_alt,
            concat(
                t.orgao,
                ' ',
                ifnull(t.num_decreto, ''),
                ' ',
                ifnull(t.data_ass_decreto, ''),
                ' ',
                ifnull(t.dsc_tipo_decreto, ''),
                ' ',
                t.id_municipio,
                ' ',
                t.ano
            ) as id_decreto_bd
        from {{ set_datalake_project("world_wb_mides_staging.raw_decreto_mg") }} as t
    )
select
    safe_cast(t.ano as int64) as ano,
    safe_cast(t.mes as int64) as mes,
    'MG' as sigla_uf,
    safe_cast(t.id_municipio as string) as id_municipio,
    -- No stable column makes this key unique: colliding source rows differ
    -- only in a measure, or are identical apart from the portal's own
    -- sequence. `seq_alteracao` is appended so the key identifies a row, at the
    -- cost of churning between extractions -- see
    -- `reference_tce_mg_seq_empenho_unstable`. Decided 2026-09-24.
    safe_cast(
        concat(
            p_decreto.id_decreto_bd,
            ' ',
            ifnull(t.dsc_dotacao, ''),
            ' ',
            ifnull(t.dsc_tipo_alteracao, ''),
            ' ',
            ifnull(t.seq_alteracao, '')
        ) as string
    ) as id_alteracao_orcamentaria_bd,
    safe_cast(p_decreto.id_decreto_bd as string) as id_decreto_bd,
    safe_cast(t.seq_alteracao as string) as id_alteracao,
    safe_cast(t.seq_decreto_alt as string) as id_decreto_alt,
    safe_cast(t.orgao as string) as orgao,
    safe_cast(t.dsc_dotacao as string) as dotacao,
    safe_cast(t.dsc_tipo_alteracao as string) as tipo_alteracao,
    safe_cast(t.valor_acres_red as float64) as valor_acres_red
from
    {{ set_datalake_project("world_wb_mides_staging.raw_alteracao_orcamentaria_mg") }}
    as t
left join p_decreto on t.seq_decreto_alt = p_decreto.seq_decreto_alt
