{{
    config(
        alias="docente_etapa_ensino",
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
    safe_cast(replace(id_municipio, ".0", "") as string) id_municipio,
    safe_cast(tipo_classe as string) tipo_classe,
    safe_cast(etapa_ensino as string) etapa_ensino,
    safe_cast(replace(quantidade_docentes, ".0", "") as int64) quantidade_docente,
from
    {{ project_path("br_inep_sinopse_estatistica_educacao_basica_staging.docente_etapa_ensino") }}
    as t
