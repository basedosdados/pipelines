{{
    config(
        schema="cl_res_empresas",
        alias="sociedad",
        materialized="table",
        partition_by={
            "field": "ano",
            "data_type": "int64",
            "range": {"start": 2013, "end": 2031, "interval": 1},
        },
    )
}}


select
    safe_cast(ano as int64) ano,
    safe_cast(mes as int64) mes,
    safe_cast(id_region_tributaria as string) id_region_tributaria,
    safe_cast(id_comuna_tributaria as string) id_comuna_tributaria,
    safe_cast(id_region_social as string) id_region_social,
    safe_cast(id_comuna_social as string) id_comuna_social,
    safe_cast(rut as string) rut,
    safe_cast(id_actuacion as string) id_actuacion,
    safe_cast(razon_social as string) razon_social,
    safe_cast(tipo_sociedad as string) tipo_sociedad,
    safe_cast(tipo_actuacion as string) tipo_actuacion,
    safe_cast(fecha_actuacion as date) fecha_actuacion,
    safe_cast(fecha_registro as date) fecha_registro,
    safe_cast(fecha_aprobacion_sii as date) fecha_aprobacion_sii,
    safe_cast(capital as float64) capital
from {{ set_datalake_project("cl_res_empresas_staging.sociedad") }} as t
