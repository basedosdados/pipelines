{{
    config(
        alias="microdados",
        schema="br_inmet_bdmep",
        materialized="table",
        partition_by={
            "field": "ano",
            "data_type": "int64",
            "range": {"start": 2000, "end": 2023, "interval": 1},
        },
        cluster_by=["id_estacao"],
        post_hook=[
            'CREATE OR REPLACE ROW ACCESS POLICY allusers_filter ON {{this}} GRANT TO ("allUsers") FILTER USING (DATE_DIFF(DATE("{{ run_started_at.strftime("%Y-%m-%d") }}"),DATE(CAST(ano AS INT64),CAST(extract(MONTH FROM data) as INT64),1), MONTH) > 6)',
            'CREATE OR REPLACE ROW ACCESS POLICY bdpro_filter ON {{this}} GRANT TO ("group:bd-pro@basedosdados.org", "group:sudo@basedosdados.org") FILTER USING (DATE_DIFF(DATE("{{ run_started_at.strftime("%Y-%m-%d") }}"),DATE(CAST(ano AS INT64),cast(extract(MONTH FROM data) as INT64),1), MONTH) <= 6)',
        ],
    )
}}
select
    safe_cast(ano as int64) ano,
    safe_cast(extract(month from safe_cast(data as date)) as int64) mes,
    safe_cast(data as date) data,
    safe_cast(hora as time) hora,
    safe_cast(id_estacao as string) id_estacao,
    safe_cast(precipitacao_total as float64) precipitacao_total,
    safe_cast(pressao_atm_hora as float64) pressao_atm_hora,
    safe_cast(pressao_atm_max as float64) pressao_atm_max,
    safe_cast(pressao_atm_min as float64) pressao_atm_min,
    safe_cast(radiacao_global as float64) radiacao_global,
    safe_cast(temperatura_bulbo_hora as float64) temperatura_bulbo_hora,
    safe_cast(temperatura_orvalho_hora as float64) temperatura_orvalho_hora,
    safe_cast(temperatura_max as float64) temperatura_max,
    safe_cast(temperatura_min as float64) temperatura_min,
    safe_cast(temperatura_orvalho_max as float64) temperatura_orvalho_max,
    safe_cast(temperatura_orvalho_min as float64) temperatura_orvalho_min,
    safe_cast(umidade_rel_max as float64) umidade_rel_max,
    safe_cast(umidade_rel_min as float64) umidade_rel_min,
    safe_cast(umidade_rel_hora as float64) umidade_rel_hora,
    safe_cast(vento_direcao as float64) vento_direcao,
    safe_cast(vento_rajada_max as float64) vento_rajada_max,
    safe_cast(vento_velocidade as float64) vento_velocidade
from {{ set_datalake_project("br_inmet_bdmep_staging.microdados") }} as t
