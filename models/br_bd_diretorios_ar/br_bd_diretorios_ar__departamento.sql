{{
    config(
        schema="br_bd_diretorios_ar",
        alias="departamento",
        materialized="table",
    )
}}

select
    safe_cast(id_departamento as string) id_departamento,
    safe_cast(id_jurisdiccion as string) id_jurisdiccion,
    safe_cast(nombre as string) nombre
from {{ set_datalake_project("br_bd_diretorios_ar_staging.departamento") }} as t
