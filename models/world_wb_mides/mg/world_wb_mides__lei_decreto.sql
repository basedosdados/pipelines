-- Minas Gerais only. Generated from the pinned source header contract
-- (`code/mg_source_headers.json`) plus the validated key definitions; see
-- `ARCHITECTURE_MG.md`. MG is the only state publishing this stream, so the
-- table's Coverage records sigla_uf = MG rather than implying national scope.
{{
    config(
        alias="lei_decreto",
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
    safe_cast(
        concat(p_decreto.id_decreto_bd, ' ', ifnull(t.num_lei, '')) as string
    ) as id_lei_decreto_bd,
    safe_cast(p_decreto.id_decreto_bd as string) as id_decreto_bd,
    safe_cast(t.seq_lei_alteracao as string) as id_lei_alteracao,
    safe_cast(t.seq_decreto_alt as string) as id_decreto_alt,
    safe_cast(t.orgao as string) as orgao,
    safe_cast(t.num_lei as string) as numero_lei,
    safe_cast(t.data_lei as date) as data_lei,
    safe_cast(t.data_pub_lei as date) as data_pub_lei,
    safe_cast(t.num_lei_alt as string) as numero_lei_alt,
    safe_cast(t.data_lei_alt as date) as data_lei_alt,
    safe_cast(t.data_pub_lei_alt as date) as data_pub_lei_alt,
    safe_cast(t.valor_aberto_lei as float64) as valor_aberto_lei
from {{ set_datalake_project("world_wb_mides_staging.raw_lei_decreto_mg") }} as t
left join p_decreto on t.seq_decreto_alt = p_decreto.seq_decreto_alt
