{{
    config(
        alias="estudantes_tea_grupo_idade_nivel_escolaridade",
        schema="br_ibge_censo_2022",
        materialized="table",
    )
}}
select
    safe_cast(ano as string) ano,
    safe_cast(id_municipio as string) id_municipio,
    safe_cast(grupo_idade as string) grupo_idade,
    safe_cast(creche as int64) as creche,
    safe_cast(pre_escola as int64) as pre_escola,
    safe_cast(eja as int64) as eja,
    safe_cast(ensino_fundamental as int64) as ensino_fundamental,
    safe_cast(eja_ensino_fundamental as int64) as eja_ensino_fundamental,
    safe_cast(ensino_medio as int64) as ensino_medio,
    safe_cast(eja_ensino_medio as int64) as eja_ensino_medio,
    safe_cast(graduacao as int64) as graduacao,
    safe_cast(especializacao as int64) as especializacao,
    safe_cast(mestrado as int64) as mestrado,
    safe_cast(doutorado as int64) as doutorado
from
    {{
        set_datalake_project(
            "br_ibge_censo_2022_staging.estudantes_tea_grupo_idade_nivel_escolaridade"
        )
    }} as t
