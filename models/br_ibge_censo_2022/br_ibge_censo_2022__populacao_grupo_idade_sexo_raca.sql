{{ config(alias="populacao_grupo_idade_sexo_raca", schema="br_ibge_censo_2022") }}
select
    safe_cast(ano as int64) ano,
    safe_cast(cod_ as string) id_municipio,
    safe_cast(idade as string) grupo_idade,
    safe_cast(sexo as string) sexo,
    safe_cast(cor_ou_raca as string) cor_raca,
    safe_cast(populacao_residente_pessoas_ as int64) populacao,
from
    {{
        set_datalake_project(
            "br_ibge_censo_2022_staging.populacao_grupo_idade_sexo_raca"
        )
    }} t
