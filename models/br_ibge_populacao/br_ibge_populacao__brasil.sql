{{
    config(
        alias="brasil",
        schema="br_ibge_populacao",
        materialized="table",
        cluster_by=["ano"],
    )
}}
select safe_cast(ano as int64) ano, safe_cast(populacao as int64) populacao,
from {{ set_datalake_project("br_ibge_populacao_staging.brasil") }} as t
