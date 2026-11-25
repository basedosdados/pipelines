{{
    config(
        alias="county",
        schema="br_bd_diretorios_us",
        materialized="table",
    )
}}
select
    safe_cast(id_county as string) id_county,
    safe_cast(name as string) name,
    safe_cast(type as string) type,
    safe_cast(id_state as string) id_state,
    safe_cast(abbreviation_state as string) abbreviation_state,
    safe_cast(name_state as string) name_state,
    safe_cast(id_gnis as string) id_gnis,
    safe_cast(id_cbsa as string) id_cbsa,
    safe_cast(name_cbsa as string) name_cbsa,
    safe_cast(id_csa as string) id_csa,
    safe_cast(name_csa as string) name_csa,
    safe_cast(central_outlying as string) central_outlying,
    safe_cast(area_land_km2 as float64) area_land_km2,
    safe_cast(area_water_km2 as float64) area_water_km2,
    safe.st_geogfromtext(centroid) centroid
from {{ set_datalake_project("br_bd_diretorios_us_staging.county") }} as t
