{{
    config(
        alias="etapa_ensino_serie",
        schema="br_inep_sinopse_estatistica_educacao_basica",
        materialized="table",
        partition_by={
            "field": "ano",
            "data_type": "int64",
            "range": {"start": 2007, "end": 2023, "interval": 1},
        },
        cluster_by="sigla_uf",
    )
}}
select
    safe_cast(ano as int64) ano,
    safe_cast(sigla_uf as string) sigla_uf,
    safe_cast(id_municipio as string) id_municipio,
    safe_cast(rede as string) rede,
    safe_cast(etapa_ensino as string) etapa_ensino,
    safe_cast(serie as string) serie,
    safe_cast(quantidade_matricula as int64) quantidade_matricula,
from
    {{
        set_datalake_project(
            "br_inep_sinopse_estatistica_educacao_basica_staging.etapa_ensino_serie"
        )
    }} as t
