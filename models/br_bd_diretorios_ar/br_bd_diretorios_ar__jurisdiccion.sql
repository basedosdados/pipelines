{{
    config(
        schema="br_bd_diretorios_ar",
        alias="jurisdiccion",
        materialized="table",
    )
}}

select
    safe_cast(id_jurisdiccion as string) id_jurisdiccion,
    safe_cast(nombre as string) nombre,
    safe_cast(nombre_completo as string) nombre_completo,
    safe_cast(sigla as string) sigla
from {{ set_datalake_project("br_bd_diretorios_ar_staging.jurisdiccion") }} as t
