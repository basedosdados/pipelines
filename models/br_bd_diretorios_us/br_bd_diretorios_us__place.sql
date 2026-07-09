{{
    config(
        alias="place",
        schema="br_bd_diretorios_us",
        materialized="table",
    )
}}
select
    safe_cast(id_place as string) id_place,
    safe_cast(name as string) name,
    safe_cast(type as string) type,
    safe_cast(class_code as string) class_code,
    safe_cast(id_gnis as string) id_gnis,
    safe_cast(id_state as string) id_state,
    safe_cast(abbreviation_state as string) abbreviation_state,
    safe_cast(area_land_km2 as float64) area_land_km2,
    safe_cast(area_water_km2 as float64) area_water_km2,
    safe.st_geogfromtext(centroid) centroid
from {{ set_datalake_project("br_bd_diretorios_us_staging.place") }} as t
