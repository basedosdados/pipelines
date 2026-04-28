{{
    config(
        alias="estacao",
        schema="br_inmet_bdmep",
        materialized="table",
    )
}}


select
    safe_cast(id_municipio as string) id_municipio,
    safe_cast(id_estacao as string) id_estacao,
    safe_cast(nome_estacao as string) estacao,
    safe_cast(data_fundacao as date) data_fundacao,
    safe_cast(latitude as string) latitude,
    safe_cast(longitude as string) longitude,
    safe_cast(altitude as string) altitude
from {{ set_datalake_project("br_inmet_bdmep_staging.estacao") }} as t
