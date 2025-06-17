{{
    config(
        alias="minuto",
        schema="br_bd_diretorios_data_tempo",
        materialized="table",
    )
}}

select safe_cast(minuto as int64) minuto
from {{ set_datalake_project("br_bd_diretorios_data_tempo_staging.minuto") }}
