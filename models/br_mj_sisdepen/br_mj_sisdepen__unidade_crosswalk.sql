{{
    config(
        schema="br_mj_sisdepen",
        alias="unidade_crosswalk",
        materialized="table",
        partition_by={
            "field": "ano",
            "data_type": "int64",
            "range": {"start": 2016, "end": 2030, "interval": 1},
        },
    )
}}


select
    safe_cast(ano as int64) ano,
    safe_cast(semestre as int64) semestre,
    safe_cast(sigla_uf as string) sigla_uf,
    safe_cast(id_municipio as string) id_municipio,
    safe_cast(id_unidade as string) id_unidade,
    safe_cast(ciclo as string) ciclo,
    safe_cast(nome_unidade_original as string) nome_unidade_original,
    safe_cast(score_pareamento as float64) score_pareamento,
    safe_cast(score_rival as float64) score_rival,
    safe_cast(pareamento_ambiguo as string) pareamento_ambiguo,
    safe_cast(metodo_pareamento as string) metodo_pareamento
from {{ set_datalake_project("br_mj_sisdepen_staging.unidade_crosswalk") }} as t
