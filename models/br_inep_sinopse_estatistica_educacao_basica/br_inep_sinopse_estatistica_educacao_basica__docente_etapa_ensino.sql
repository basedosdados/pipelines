{{
    config(
        alias="docente_etapa_ensino",
        schema="br_inep_sinopse_estatistica_educacao_basica",
        materialized="table",
        partition_by={
            "field": "ano",
            "data_type": "int64",
            "range": {"start": 2007, "end": 2025, "interval": 1},
        },
        cluster_by="sigla_uf",
    )
}}

with
    tabela_1 as (
        select
            safe_cast(ano as int64) ano,
            safe_cast(sigla_uf as string) sigla_uf,
            safe_cast(replace(id_municipio, ".0", "") as string) id_municipio,
            case
                when tipo_classe = "Educacao Basica"
                then "Educação Básica"
                when tipo_classe = "Educacao Infantil"
                then "Educação Infantil"
                when tipo_classe = "Educacao Especial - Classes Exclusivas"
                then "Educação Especial - Classes Exclusivas"
                when tipo_classe = "Educacao Indigena"
                then "Educação Indígena"
                when tipo_classe = "Educacao Especial - Classes Comuns"
                then "Educação Especial - Classes Comuns"
                when tipo_classe = "Educacao Especial"
                then "Educação Especial"
                else tipo_classe
            end etapa_ensino,
            etapa_ensino as tipo_classe,
            safe_cast(
                replace(quantidade_docentes, ".0", "") as int64
            ) quantidade_docente,
        from
            {{
                set_datalake_project(
                    "br_inep_sinopse_estatistica_educacao_basica_staging.docente_etapa_ensino"
                )
            }}
    )

select
    ano,
    sigla_uf,
    id_municipio,
    etapa_ensino,
    trim(regexp_extract(tipo_classe, r'^(.*)-[^-]*$')) as tipo_classe,
    trim(regexp_extract(tipo_classe, r'-([^-]*)$')) as rede,
    quantidade_docente
from tabela_1
