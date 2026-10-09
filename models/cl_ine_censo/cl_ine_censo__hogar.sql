{{
    config(
        schema="cl_ine_censo",
        alias="hogar",
        materialized="table",
        partition_by={
            "field": "ano",
            "data_type": "int64",
            "range": {"start": 2024, "end": 2029, "interval": 1},
        },
        cluster_by=["id_comuna"],
    )
}}


select
    safe_cast(ano as int64) ano,
    lpad(safe_cast(id_region as string), 2, '0') id_region,
    safe_cast(id_provincia as string) id_provincia,
    safe_cast(id_comuna as string) id_comuna,
    safe_cast(id_vivienda as string) id_vivienda,
    safe_cast(id_hogar as string) id_hogar,
    safe_cast(comuna_bajo_umbral as string) comuna_bajo_umbral,
    safe_cast(area as string) area,
    safe_cast(tipo_operativo as string) tipo_operativo,
    safe_cast(p12_tenencia_viv as string) p12_tenencia_viv,
    safe_cast(p13_comb_cocina as string) p13_comb_cocina,
    safe_cast(p14_comb_calefaccion as string) p14_comb_calefaccion,
    safe_cast(p15a_serv_tel_movil as string) p15a_serv_tel_movil,
    safe_cast(p15b_serv_compu as string) p15b_serv_compu,
    safe_cast(p15c_serv_tablet as string) p15c_serv_tablet,
    safe_cast(p15d_serv_internet_fija as string) p15d_serv_internet_fija,
    safe_cast(p15e_serv_internet_movil as string) p15e_serv_internet_movil,
    safe_cast(p15f_serv_internet_satelital as string) p15f_serv_internet_satelital,
    safe_cast(tipologia_hogar as string) tipologia_hogar
from {{ set_datalake_project("cl_ine_censo_staging.hogar") }} as t
