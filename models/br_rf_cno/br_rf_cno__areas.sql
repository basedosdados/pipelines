{{
    config(
        alias="areas",
        schema="br_rf_cno",
        materialized="incremental",
        partition_by={
            "field": "data_extracao",
            "data_type": "date",
        },
        pre_hook="DROP ALL ROW ACCESS POLICIES ON {{ this }}",
    )
}}

with
    safe_select as (
        select
            safe_cast(data as date) data_extracao,
            safe_cast(id_cno as string) id_cno,
            safe_cast(categoria as string) categoria,
            safe_cast(destinacao as string) destinacao,
            safe_cast(tipo_obra as string) tipo_obra,
            safe_cast(tipo_area as string) tipo_area,
            safe_cast(tipo_area_complementar as string) tipo_area_complementar,
            safe_cast(metragem as float64) metragem
        from {{ set_datalake_project("br_rf_cno_staging.areas") }} as t
        {% if is_incremental() %}
            where safe_cast(data as date) > (select max(data_extracao) from {{ this }})
        {% endif %}
    )
select
    data_extracao,
    id_cno,
    {% set cols = [
        "categoria",
        "destinacao",
        "tipo_obra",
        "tipo_area",
        "tipo_area_complementar",
    ] %}
    {% for col in cols %}
        {{ validate_null_cols(col) }} as {{ col }}{% if not loop.last %},{% endif %}
    {% endfor %},
    metragem
from safe_select
