{{
    config(
        alias="dia",
        schema="br_bd_diretorios_data_tempo",
        materialized="table",
    )
}}

select safe_cast(dia as int64) dia
from {{ set_datalake_project("br_bd_diretorios_data_tempo_staging.dia") }}
