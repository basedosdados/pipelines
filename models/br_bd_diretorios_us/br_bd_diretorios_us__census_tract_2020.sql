{{
    config(
        alias="census_tract_2020",
        schema="br_bd_diretorios_us",
        materialized="table",
    )
}}
select
    safe_cast(id_census_tract as string) id_census_tract,
    safe_cast(id_county as string) id_county,
    safe_cast(id_state as string) id_state,
    safe_cast(area_land_km2 as float64) area_land_km2,
    safe_cast(area_water_km2 as float64) area_water_km2,
    safe.st_geogfromtext(centroid) centroid
from {{ set_datalake_project("br_bd_diretorios_us_staging.census_tract_2020") }} as t
