{{
    config(
        alias="mes_categoria_rm",
        schema="br_ibge_inpc",
        materialized="incremental",
        pre_hook="DROP ALL ROW ACCESS POLICIES ON {{ this }}",
        post_hook=[
            'CREATE OR REPLACE ROW ACCESS POLICY allusers_filter ON {{this}} GRANT TO ("allUsers") FILTER USING (DATE_DIFF(DATE("{{ run_started_at.strftime("%Y-%m-%d") }}"),DATE(CAST(ano AS INT64),CAST(mes AS INT64),1), MONTH) > 6)',
            'CREATE OR REPLACE ROW ACCESS POLICY bdpro_filter ON {{this}} GRANT TO ("group:bd-pro@basedosdados.org", "group:sudo@basedosdados.org") FILTER USING (DATE_DIFF(DATE("{{ run_started_at.strftime("%Y-%m-%d") }}"),DATE(CAST(ano AS INT64),CAST(mes AS INT64),1), MONTH) <= 6)',
        ],
    )
}}
select
    safe_cast(ano as int64) ano,
    safe_cast(mes as int64) mes,
    safe_cast(id_regiao_metropolitana as string) id_regiao_metropolitana,
    safe_cast(id_categoria as string) id_categoria,
    safe_cast(id_categoria_bd as string) id_categoria_bd,
    safe_cast(categoria as string) categoria,
    safe_cast(peso_mensal as float64) peso_mensal,
    safe_cast(variacao_mensal as float64) variacao_mensal,
    safe_cast(variacao_anual as float64) variacao_anual,
    safe_cast(variacao_doze_meses as float64) variacao_doze_meses
from {{ set_datalake_project("br_ibge_inpc_staging.mes_categoria_rm") }} as t
{% if is_incremental() %}
    where
        date(cast(ano as int64), cast(mes as int64), 1)
        > (select max(date(cast(ano as int64), cast(mes as int64), 1)) from {{ this }})
{% endif %}
