{{
    config(
        alias="estudantes_indigenas_deficiencia_grupo_idade",
        schema="br_ibge_censo_2022",
        materialized="table",
    )
}}
select
    safe_cast(ano as int64) ano,
    safe_cast(id_municipio as string) id_municipio,
    safe_cast(grupo_idade as string) grupo_idade,
    safe_cast(pessoas_deficiencia as int64) pessoas_deficiencia,
from
    {{
        set_datalake_project(
            "br_ibge_censo_2022_staging.estudantes_indigenas_deficiencia_grupo_idade"
        )
    }} as t
