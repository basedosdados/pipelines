{{
    config(
        alias="ano",
        schema="br_bd_diretorios_data_tempo",
        materialized="table",
    )
}}

select safe_cast(ano as int64) ano, safe_cast(bissexto as int64) bissexto
from {{ set_datalake_project("br_bd_diretorios_data_tempo_staging.ano") }}
