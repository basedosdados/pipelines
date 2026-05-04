{{
    config(
        alias="microdados_vinculos",
        schema="br_me_rais",
        materialized="incremental",
        partition_by={
            "field": "ano",
            "data_type": "int64",
            "range": {"start": 1985, "end": 2030, "interval": 1},
        },
        cluster_by=["sigla_uf", "id_municipio"],
    )
}}

with
    pre_2023 as ({{ vinculos_select("br_me_rais_staging.microdados_vinculos") }}),
    from_2023 as (
        {{
            vinculos_select(
                "br_me_rais_staging.microdados_vinculos_2023",
                has_vinculo_abandonado=true,
            )
        }}
    )

select *
from pre_2023
union all
select *
from from_2023

{% if is_incremental() %}
    where safe_cast(ano as int64) > (select max(ano) from {{ this }})
{% endif %}
