{{
    config(
        schema="cl_chilecompra_mercado_publico",
        alias="licitacion_oferta",
        materialized="table",
        partition_by={
            "field": "ano",
            "data_type": "int64",
            "range": {"start": 2007, "end": 2031, "interval": 1},
        },
        cluster_by=["mes"],
    )
}}


select
    safe_cast(ano as int64) ano,
    safe_cast(mes as int64) mes,
    safe_cast(codigo_licitacion as string) codigo_licitacion,
    safe_cast(codigo_item as string) codigo_item,
    safe_cast(codigo_proveedor as string) codigo_proveedor,
    safe_cast(nombre_oferta as string) nombre_oferta,
    safe_cast(codigo_sucursal_proveedor as string) codigo_sucursal_proveedor,
    safe_cast(rut_proveedor as string) rut_proveedor,
    safe_cast(nombre_proveedor as string) nombre_proveedor,
    safe_cast(razon_social_proveedor as string) razon_social_proveedor,
    safe_cast(descripcion_oferta_proveedor as string) descripcion_oferta_proveedor,
    safe_cast(estado_oferta as string) estado_oferta,
    safe_cast(estado_final_oferta as string) estado_final_oferta,
    safe_cast(indicador_oferta_seleccionada as string) indicador_oferta_seleccionada,
    safe_cast(cantidad_ofertada as float64) cantidad_ofertada,
    safe_cast(unidad_medida_oferta as string) unidad_medida_oferta,
    safe_cast(monto_unitario_oferta as float64) monto_unitario_oferta,
    safe_cast(monto_total_ofertado as float64) monto_total_ofertado,
    safe_cast(cantidad_adjudicada as float64) cantidad_adjudicada,
    safe_cast(monto_linea_adjudicada as float64) monto_linea_adjudicada,
    safe_cast(fecha_envio_oferta as date) fecha_envio_oferta
from
    {{
        set_datalake_project(
            "cl_chilecompra_mercado_publico_staging.licitacion_oferta"
        )
    }} as t
