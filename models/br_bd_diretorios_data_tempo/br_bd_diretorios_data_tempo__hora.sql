{{
    config(
        alias="hora",
        schema="br_bd_diretorios_data_tempo",
        materialized="table",
    )
}}

select safe_cast(hora as int64) hora
from {{ set_datalake_project("br_bd_diretorios_data_tempo_staging.hora") }}
