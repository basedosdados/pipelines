{{
    config(
        alias="zcta_2020",
        schema="br_bd_diretorios_us",
        materialized="table",
    )
}}
select
    safe_cast(id_zcta as string) id_zcta,
    safe_cast(area_land_km2 as float64) area_land_km2,
    safe_cast(area_water_km2 as float64) area_water_km2,
    safe.st_geogfromtext(centroid) centroid
from {{ set_datalake_project("br_bd_diretorios_us_staging.zcta_2020") }} as t
