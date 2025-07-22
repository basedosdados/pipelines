{{
    config(
        alias="data",
        schema="br_bd_diretorios_data_tempo",
        materialized="table",
    )
}}

select
    safe_cast(data as date) data,
    safe_cast(dia as int64) dia,
    safe_cast(mes as int64) mes,
    safe_cast(bimestre as int64) bimestre,
    safe_cast(trimestre as int64) trimestre,
    safe_cast(semestre as int64) semestre,
    safe_cast(ano as int64) ano,
    safe_cast(dia_semana as int64) dia_semana
from {{ set_datalake_project("br_bd_diretorios_data_tempo_staging.data") }}
