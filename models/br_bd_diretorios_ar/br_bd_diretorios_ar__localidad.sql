{{
    config(
        schema="br_bd_diretorios_ar",
        alias="localidad",
        materialized="table",
    )
}}

select
    safe_cast(id_localidad as string) id_localidad,
    safe_cast(id_departamento as string) id_departamento,
    safe_cast(id_jurisdiccion as string) id_jurisdiccion,
    safe_cast(id_gobierno_local as string) id_gobierno_local,
    safe_cast(id_aglomerado as string) id_aglomerado,
    safe_cast(nombre as string) nombre,
    safe_cast(tipo as string) tipo
from {{ set_datalake_project("br_bd_diretorios_ar_staging.localidad") }} as t
