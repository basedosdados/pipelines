{{
    config(
        alias="bimestre",
        schema="br_bd_diretorios_data_tempo",
        materialized="table",
    )
}}

select safe_cast(bimestre as int64) bimestre, safe_cast(semestre as int64) semestre
from {{ set_datalake_project("br_bd_diretorios_data_tempo_staging.bimestre") }}
