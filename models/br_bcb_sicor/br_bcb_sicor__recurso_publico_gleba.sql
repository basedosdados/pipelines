{{
    config(
        alias="recurso_publico_gleba", schema="br_bcb_sicor", materialized="table"
    )
}}
select
    safe_cast(id_referencia_bacen as string) id_referencia_bacen,
    safe_cast(numero_ordem as string) numero_ordem,
    safe_cast(numero_identificador_gleba as string) numero_identificador_gleba,
    safe_cast(indice_indice_gleba as int64) indice_gleba,
    safe_cast(indice_indice_ponto as int64) indice_ponto,
    st_geogpoint(safe_cast(longitude as float64), safe_cast(latitude as float64)) ponto,
    safe_cast(altitude as float64) altitude
from {{ set_datalake_project("br_bcb_sicor_staging.recurso_publico_gleba") }} as t
