{{ config(alias="municipio", schema="br_ms_populacao", materialized="table") }}
select
    safe_cast(ano as int64) ano,
    safe_cast(id_municipio as string) id_municipio,
    safe_cast(sexo as string) sexo,
    safe_cast(grupo_idade as string) grupo_idade,
    safe_cast(populacao as int64) populacao,
from {{ set_datalake_project("br_ms_populacao_staging.municipio") }} as t
