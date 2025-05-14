{{
    config(
        alias="municipio_exportacao",
        schema="br_me_comex_stat",
        materialized="incremental",
        partition_by={
            "field": "ano",
            "data_type": "int64",
            "range": {"start": 1997, "end": 2025, "interval": 1},
        },
        cluster_by=["mes", "sigla_uf"],
        pre_hook="DROP ALL ROW ACCESS POLICIES ON {{ this }}",
    )
}}
select
    safe_cast(ano as int64) ano,
    safe_cast(mes as int64) mes,
    safe_cast(lpad(id_sh4, 4, '0') as string) id_sh4,
    safe_cast(id_pais as string) id_pais,
    {{ transform_mdic_country_code("id_pais") }} as sigla_pais_iso3,
    safe_cast(case when sigla_uf = 'ND' then null else sigla_uf end as string) sigla_uf,
    safe_cast(
        case
            when id_municipio = '9300000' or id_municipio = '9999999'
            then null
            else id_municipio
        end as string
    ) id_municipio,
    safe_cast(peso_liquido_kg as int64) peso_liquido_kg,
    safe_cast(valor_fob_dolar as int64) valor_fob_dolar
from {{ set_datalake_project("br_me_comex_stat_staging.municipio_exportacao") }} as t
{% if is_incremental() %}
    where
        date(cast(ano as int64), cast(mes as int64), 1)
        > (select max(date(cast(ano as int64), cast(mes as int64), 1)) from {{ this }})
{% endif %}
