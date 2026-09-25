-- Minas Gerais only. Generated from the pinned source header contract
-- (`code/mg_source_headers.json`) plus the validated key definitions; see
-- `ARCHITECTURE_MG.md`. MG is the only state publishing this stream, so the
-- table's Coverage records sigla_uf = MG rather than implying national scope.
{{
    config(
        alias="decreto",
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
select
    safe_cast(t.ano as int64) as ano,
    safe_cast(t.mes as int64) as mes,
    'MG' as sigla_uf,
    safe_cast(t.id_municipio as string) as id_municipio,
    safe_cast(
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
        ) as string
    ) as id_decreto_bd,
    safe_cast(t.seq_decreto_alt as string) as id_decreto_alt,
    safe_cast(t.orgao as string) as orgao,
    safe_cast(t.num_decreto as string) as numero_decreto,
    safe_cast(t.data_ass_decreto as date) as data_ass_decreto,
    safe_cast(t.dsc_tipo_decreto as string) as tipo_decreto,
    safe_cast(t.valor_aberto as float64) as valor_aberto
from {{ set_datalake_project("world_wb_mides_staging.raw_decreto_mg") }} as t
