{{
    config(
        alias="ncm_exportacao",
        schema="br_me_comex_stat",
        materialized="incremental",
        partition_by={
            "field": "ano",
            "data_type": "int64",
            "range": {"start": 1997, "end": 2025, "interval": 1},
        },
        cluster_by=["mes", "sigla_uf_ncm"],
        pre_hook="DROP ALL ROW ACCESS POLICIES ON {{ this }}",
    )
}}
with
    safe_select as (
        select
            safe_cast(ano as int64) ano,
            safe_cast(mes as int64) mes,
            safe_cast(lpad(id_ncm, 8, "0") as string) id_ncm,
            safe_cast(id_unidade as string) id_unidade,
            safe_cast(id_pais as string) id_pais,
            safe_cast(
                {{ transform_mdic_country_code("id_pais") }} as string
            ) as sigla_pais_iso3,
            safe_cast(
                case when sigla_uf_ncm = 'ND' then null else sigla_uf_ncm end as string
            ) sigla_uf_ncm,
            safe_cast(id_via as string) id_via,
            safe_cast(id_urf as string) id_urf,
            safe_cast(quantidade_estatistica as int64) quantidade_estatistica,
            safe_cast(peso_liquido_kg as int64) peso_liquido_kg,
            safe_cast(valor_fob_dolar as int64) valor_fob_dolar
        from {{ set_datalake_project("br_me_comex_stat_staging.ncm_exportacao") }} as t
        {% if is_incremental() %}
            where
                date(cast(ano as int64), cast(mes as int64), 1) > (
                    select max(date(cast(ano as int64), cast(mes as int64), 1))
                    from {{ this }}
                )
        {% endif %}
    )
select
    ano,
    mes,
    {% set cols = [
        "id_ncm",
        "id_unidade",
        "id_pais",
        "sigla_pais_iso3",
        "sigla_uf_ncm",
        "id_via",
        "id_urf",
    ] %}
    {% for col in cols %}
        {{ validate_null_cols(col) }} as {{ col }}{% if not loop.last %},{% endif %}
    {% endfor %},
    quantidade_estatistica,
    peso_liquido_kg,
    valor_fob_dolar
from safe_select
