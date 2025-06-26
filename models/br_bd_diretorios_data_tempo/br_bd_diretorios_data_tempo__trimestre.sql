{{
    config(
        alias="trimestre",
        schema="br_bd_diretorios_data_tempo",
        materialized="table",
    )
}}

select safe_cast(trimestre as int64) trimestre, safe_cast(semestre as int64) semestre
from {{ set_dalake_project("br_bd_diretorios_data_tempo_staging.trimestre") }}
