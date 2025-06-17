{{
    config(
        alias="tempo",
        schema="br_bd_diretorios_data_tempo",
        materialized="table",
    )
}}

select
    safe_cast(tempo as time) tempo,
    safe_cast(hora as int64) hora,
    safe_cast(minuto as int64) minuto,
    safe_cast(segundo as int64) segundo
from {{ set_datalake_project("br_bd_diretorios_data_tempo_staging.tempo") }}
