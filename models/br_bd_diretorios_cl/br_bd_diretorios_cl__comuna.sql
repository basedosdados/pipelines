{{
    config(
        schema="br_bd_diretorios_cl",
        alias="comuna",
        materialized="table",
    )
}}

select
    safe_cast(id_comuna as string) id_comuna,
    safe_cast(id_provincia as string) id_provincia,
    safe_cast(id_region as string) id_region,
    safe_cast(nombre as string) nombre
from {{ set_datalake_project("br_bd_diretorios_cl_staging.comuna") }} as t
