{{
    config(
        alias="docente_escolaridade",
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
    safe_cast(escolaridade as string) escolaridade,
    safe_cast(replace(quantidade_docente, ".0", "") as int64) quantidade_docente,
from
    {{
        set_datalake_project(
            "br_inep_sinopse_estatistica_educacao_basica_staging.docente_escolaridade"
        )
    }}
