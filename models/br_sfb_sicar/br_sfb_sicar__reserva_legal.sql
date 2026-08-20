{{
    config(
        alias="reserva_legal",
        schema="br_sfb_sicar",
        materialized="incremental",
        incremental_strategy="insert_overwrite",
        partition_by={
            "field": "data",
            "data_type": "date",
            "granularity": "day",
        },
        cluster_by=["sigla_uf"],
    )
}}

select
    safe_cast(data as date) data,
    safe_cast(sigla_uf as string) sigla_uf,
    safe_cast(id_municipio as string) id_municipio,
    safe_cast(id_imovel as string) id_imovel,
    safe_cast(tipo as string) tipo,
    safe_cast(status as string) status,
    safe_cast(condicao as string) condicao,
    safe_cast(area as float64) area,
    safe.st_geogfromtext(geometria, make_valid => true) geometria,
from {{ set_datalake_project("br_sfb_sicar_staging.reserva_legal") }} as t
{% if is_incremental() %}
    where safe_cast(data as date) > (select max(data) from {{ this }})
{% endif %}
