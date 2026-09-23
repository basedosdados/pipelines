{{
    config(
        schema="cl_ine_censo",
        alias="vivienda",
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
    safe_cast(comuna_bajo_umbral as string) comuna_bajo_umbral,
    safe_cast(area as string) area,
    safe_cast(tipo_operativo as string) tipo_operativo,
    safe_cast(cant_hog as int64) cant_hog,
    safe_cast(cant_per as int64) cant_per,
    safe_cast(p2_tipo_vivienda as string) p2_tipo_vivienda,
    safe_cast(p3a_estado_ocupacion as string) p3a_estado_ocupacion,
    safe_cast(p3b_estado_ocupacion as string) p3b_estado_ocupacion,
    safe_cast(p4a_mat_paredes as string) p4a_mat_paredes,
    safe_cast(p4b_mat_techo as string) p4b_mat_techo,
    safe_cast(p4c_mat_piso as string) p4c_mat_piso,
    safe_cast(p5_num_dormitorios as int64) p5_num_dormitorios,
    safe_cast(p6_fuente_agua as string) p6_fuente_agua,
    safe_cast(p7_distrib_agua as string) p7_distrib_agua,
    safe_cast(p8_serv_hig as string) p8_serv_hig,
    safe_cast(p9_fuente_elect as string) p9_fuente_elect,
    safe_cast(p10_basura as string) p10_basura,
    safe_cast(p11a_num_personas as int64) p11a_num_personas,
    safe_cast(p11b_comparte_gasto as string) p11b_comparte_gasto,
    safe_cast(p11c_num_hogar as int64) p11c_num_hogar,
    safe_cast(indice_hacinamiento as string) indice_hacinamiento
from {{ set_datalake_project("cl_ine_censo_staging.vivienda") }} as t
