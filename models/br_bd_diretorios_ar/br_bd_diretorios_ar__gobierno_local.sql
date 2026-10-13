{{
    config(
        schema="br_bd_diretorios_ar",
        alias="gobierno_local",
        materialized="table",
    )
}}

select
    safe_cast(id_gobierno_local as string) id_gobierno_local,
    safe_cast(id_jurisdiccion as string) id_jurisdiccion,
    safe_cast(nombre as string) nombre,
    safe_cast(categoria as string) categoria
from {{ set_datalake_project("br_bd_diretorios_ar_staging.gobierno_local") }} as t
