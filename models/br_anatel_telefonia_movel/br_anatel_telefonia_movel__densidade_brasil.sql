{{
    config(
        alias="densidade_brasil",
        schema="br_anatel_telefonia_movel",
        post_hook=[
            'CREATE OR REPLACE ROW ACCESS POLICY allusers_filter ON {{this}} GRANT TO ("allUsers") FILTER USING (DATE_DIFF(DATE("{{ run_started_at.strftime("%Y-%m-%d") }}"),DATE(CAST(ano AS INT64),CAST(mes AS INT64),1), MONTH) > 6)',
            'CREATE OR REPLACE ROW ACCESS POLICY bdpro_filter ON {{this}} GRANT TO ("group:bd-pro@basedosdados.org", "group:sudo@basedosdados.org") FILTER USING (DATE_DIFF(DATE("{{ run_started_at.strftime("%Y-%m-%d") }}"),DATE(CAST(ano AS INT64),CAST(mes AS INT64),1), MONTH) <= 6)',
        ],
    )
}}

select
    safe_cast(ano as int64) ano,
    safe_cast(mes as int64) mes,
    safe_cast(densidade as float64) densidade
from
    {{ set_datalake_project("br_anatel_telefonia_movel_staging.densidade_brasil") }}
    as t
