{{
    config(
        alias="setor_censitario_2010",
        schema="br_geobr_mapas",
        materialized="table",
        partition_by={
            "field": "sigla_uf",
            "data_type": "string",
        },
    )
}}

select
    safe_cast(id_uf as string) id_uf,
    safe_cast(estado_abrev as string) sigla_uf,
    safe_cast(
        safe_cast(safe_cast(id_municipio as float64) as int64) as string
    ) id_municipio,  -- corrige ponto decimal
    safe_cast(nome_municipio as string) nome_municipio,
    safe_cast(id_distrito as string) id_distrito,
    safe_cast(nome_distrito as string) nome_distrito,
    safe_cast(id_subdistrito as string) id_subdistrito,
    safe_cast(nome_subdistrito as string) nome_subdistrito,
    safe_cast(id_vizinhanca as string) nome_vizinhanca,  -- invertida com nome_vizinhanca
    safe_cast(
        safe_cast(safe_cast(nome_vizinhanca as float64) as int64) as string
    ) id_vizinhanca,  -- invertida com id_vizinhanca e corrige ponto decimal
    safe_cast(id_setor_censitario as string) id_setor_censitario,
    safe_cast(zona as string) zona,
    safe.st_geogfromtext(geometria) geometria
from {{ set_datalake_project("br_geobr_mapas_staging.setor_censitario_2010") }} as t
