{{
    config(
        schema="br_bd_diretorios_ar",
        alias="aglomerado",
        materialized="table",
    )
}}

select
    safe_cast(id_aglomerado as string) id_aglomerado, safe_cast(nombre as string) nombre
from {{ set_datalake_project("br_bd_diretorios_ar_staging.aglomerado") }} as t
