{{
    config(
        schema="br_bd_diretorios_de",
        alias="municipality",
        materialized="table",
    )
}}

select
    safe_cast(id_municipality as string) id_municipality,
    safe_cast(id_county as string) id_county,
    safe_cast(id_state as string) id_state,
    safe_cast(name as string) name
from {{ set_datalake_project("br_bd_diretorios_de_staging.municipality") }} as t
