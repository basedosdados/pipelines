{{
    config(
        alias="dicionario_especie",
        schema="br_mps_beneficios",
        materialized="table",
    )
}}


select
    safe_cast(especie_beneficio as string) especie_beneficio,
    safe_cast(nome_especie as string) nome_especie,
    safe_cast(nome_especie_anterior as string) nome_especie_anterior,
    safe_cast(categoria_beneficio as string) categoria_beneficio,
    safe_cast(natureza_beneficio as string) natureza_beneficio,
    safe_cast(observacao as string) observacao
from {{ set_datalake_project("br_mps_beneficios_staging.dicionario_especie") }} as t
