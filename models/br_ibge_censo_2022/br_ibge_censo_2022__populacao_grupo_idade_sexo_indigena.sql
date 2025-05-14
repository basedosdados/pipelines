{{
    config(
        alias="populacao_grupo_idade_sexo_indigena",
        schema="br_ibge_censo_2022",
    )
}}
select
    safe_cast(ano as int64) ano,
    safe_cast(cod_ as string) id_municipio,
    safe_cast(idade as string) grupo_idade,
    safe_cast(sexo as string) sexo,
    sum(safe_cast(pessoas_indigenas_pessoas_ as int64)) populacao_indigena,
from
    {{
        set_datalake_project(
            "br_ibge_censo_2022_staging.populacao_grupo_idade_sexo_indigena"
        )
    }} t
group by 1, 2, 3, 4
