{{
    config(
        alias="postal_area_2021",
        schema="br_bd_diretorios_au",
        materialized="table",
    )
}}
select
    safe_cast(id_postal_area as string) id_postal_area,
    safe_cast(name as string) name,
    safe_cast(area_albers_sqkm as float64) area_albers_sqkm
from {{ set_datalake_project("br_bd_diretorios_au_staging.postal_area_2021") }} as t
