{{
    config(
        alias="mes_brasil",
        schema="br_ibge_ipca",
        materialized="incremental",
        pre_hook="DROP ALL ROW ACCESS POLICIES ON {{ this }}",
        post_hook=[
            'CREATE OR REPLACE ROW ACCESS POLICY allusers_filter ON {{this}} GRANT TO ("allUsers") FILTER USING (DATE_DIFF(DATE("{{ run_started_at.strftime("%Y-%m-%d") }}"),DATE(CAST(ano AS INT64),CAST(mes AS INT64),1), MONTH) > 6)',
            'CREATE OR REPLACE ROW ACCESS POLICY bdpro_filter ON {{this}} GRANT TO ("group:bd-pro@basedosdados.org", "group:sudo@basedosdados.org") FILTER USING (DATE_DIFF(DATE("{{ run_started_at.strftime("%Y-%m-%d") }}"),DATE(CAST(ano AS INT64),CAST(mes AS INT64),1), MONTH) <= 6)',
        ],
    )
}}

with
    drop_duplicates as (
        select distinct *
        from {{ set_datalake_project("br_ibge_ipca_staging.mes_brasil") }}
    )
select
    safe_cast(ano as int64) ano,
    safe_cast(mes as int64) mes,
    safe_cast(indice as float64) indice,
    safe_cast(variacao_mensal as float64) variacao_mensal,
    safe_cast(variacao_trimestral as float64) variacao_trimestral,
    safe_cast(variacao_semestral as float64) variacao_semestral,
    safe_cast(variacao_anual as float64) variacao_anual,
    safe_cast(variacao_doze_meses as float64) variacao_doze_meses
from drop_duplicates as t

{% if is_incremental() %}
    where
        date(cast(ano as int64), cast(mes as int64), 1)
        > (select max(date(cast(ano as int64), cast(mes as int64), 1)) from {{ this }})
{% endif %}
