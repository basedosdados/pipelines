{{
    config(
        alias="area_imovel",
        schema="br_sfb_sicar",
        materialized="incremental",
        partition_by={
            "field": "data_atualizacao_car",
            "data_type": "date",
            "granularity": "day",
        },
        cluster_by=["sigla_uf"],
    )
}}

with
    area_imovel as (
        select
            safe_cast(data_extracao as date) data_extracao,
            safe_cast(data_atualizacao_car as date) data_atualizacao_car,
            safe_cast(cod_estado as string) sigla_uf,
            safe_cast(split(cod_imovel, '-')[offset(1)] as string) as id_municipio,
            safe_cast(cod_imovel as string) id_imovel,
            safe_cast(mod_fiscal as string) modulos_fiscais,
            safe_cast(num_area as float64) area,
            safe_cast(ind_status as string) status,
            safe_cast(ind_tipo as string) tipo,
            safe_cast(des_condic as string) condicao,
            safe_cast(
                safe.st_geogfromtext(geometry, make_valid => true) as geography
            ) geometria,
        from {{ set_datalake_project("br_sfb_sicar_staging.area_imovel") }} as car

    )

select *
from area_imovel
{% if is_incremental() %}
    where data_extracao > (select max(data_extracao) from {{ this }})
{% endif %}
