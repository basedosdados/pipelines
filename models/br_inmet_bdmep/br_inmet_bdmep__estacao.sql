{{
    config(
        alias="estacao",
        schema="br_inmet_bdmep",
        materialized="table",
        post_hook=[
            'CREATE OR REPLACE ROW ACCESS POLICY allusers_filter ON {{this}} GRANT TO ("allUsers") FILTER USING (DATE_DIFF(DATE("{{ run_started_at.strftime("%Y-%m-%d") }}"),DATE(CAST(ano AS INT64),CAST(extract(MONTH FROM data) as INT64),1), MONTH) > 6)',
            'CREATE OR REPLACE ROW ACCESS POLICY bdpro_filter ON {{this}} GRANT TO ("group:bd-pro@basedosdados.org", "group:sudo@basedosdados.org") FILTER USING (DATE_DIFF(DATE("{{ run_started_at.strftime("%Y-%m-%d") }}"),DATE(CAST(ano AS INT64),cast(extract(MONTH FROM data) as INT64),1), MONTH) <= 6)',
        ],
    )
}}


select
    safe_cast(id_municipio as int64) id_municipio,
    safe_cast(id_estacao as string) id_estacao,
    safe_cast(nome_estacao as string) estacao,
    safe_cast(data_fundacao as date) data_fundacao,
    safe_cast(latitude as float64) latitude,
    safe_cast(longitude as float64) longitude,
    safe_cast(altitude as float64) altitude
from {{ set_datalake_project("br_inmet_bdmep_staging.estacao") }} as t
