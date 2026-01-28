{{
    config(
        alias="municipio",
        schema="br_ibge_populacao",
        materialized="table",
        cluster_by=["ano", "sigla_uf", "id_municipio"],
    )
}}
select
    safe_cast(ano as int64) ano,
    safe_cast(sigla_uf as string) sigla_uf,
    safe_cast(id_municipio as string) id_municipio,
    safe_cast(populacao as int64) populacao
from {{ set_datalake_project("br_ibge_populacao_staging.municipio") }} as t
