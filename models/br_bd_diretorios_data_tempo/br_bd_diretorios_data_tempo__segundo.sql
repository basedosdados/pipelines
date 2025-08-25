{{
    config(
        alias="segundo",
        schema="br_bd_diretorios_data_tempo",
        materialized="table",
    )
}}

select safe_cast(segundo as int64) segundo
from {{ set_datalake_project("br_bd_diretorios_data_tempo_staging.segundo") }}
