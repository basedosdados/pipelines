{{
    config(
        alias="semestre",
        schema="br_bd_diretorios_data_tempo",
        materialized="table",
    )
}}

select safe_cast(semestre as int64) semestre
from {{ set_datalake_project("br_bd_diretorios_data_tempo_staging.semestre") }}
