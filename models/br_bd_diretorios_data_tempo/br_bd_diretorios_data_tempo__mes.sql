{{
    config(
        alias="mes",
        schema="br_bd_diretorios_data_tempo",
        materialized="table",
    )
}}

select
    safe_cast(mes as int64) mes,
    safe_cast(nome as string) nome,
    safe_cast(bimestre as int64) bimestre,
    safe_cast(trimestre as int64) trimestre,
    safe_cast(semestre as int64) semestre
from {{ set_datalake_project("br_bd_diretorios_data_tempo_staging.mes") }}
