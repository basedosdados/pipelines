{{
    config(
        alias="populacao_tea_sexo_grupo_idade",
        schema="br_ibge_censo_2022",
        materialized="table",
    )
}}
select
    safe_cast(ano as int64) ano,
    safe_cast(id_municipio as string) id_municipio,
    safe_cast(grupo_idade as string) grupo_idade,
    safe_cast(sexo as string) sexo,
    safe_cast(populacao_tea as int64) populacao_tea,
from
    {{
        set_datalake_project(
            "br_ibge_censo_2022_staging.populacao_tea_sexo_grupo_idade"
        )
    }} as t
