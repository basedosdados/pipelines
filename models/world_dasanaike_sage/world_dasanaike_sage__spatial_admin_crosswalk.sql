-- Reads staging rows built by models/world_dasanaike_sage/code/ (download.py ->
-- clean.py -> upload.py).
{{
    config(
        schema="world_dasanaike_sage",
        alias="spatial_admin_crosswalk",
        materialized="table",
        partition_by={
            "field": "year",
            "data_type": "int64",
            "range": {"start": 1959, "end": 2030, "interval": 1},
        },
        cluster_by=["country_iso3_code"],
    )
}}


select
    safe_cast(year as int64) year,
    safe_cast(country_iso3_code as string) country_iso3_code,
    safe_cast(latitude as float64) latitude,
    safe_cast(longitude as float64) longitude,
    safe_cast(gadm_1_gid as string) gadm_1_gid,
    safe_cast(gadm_1_hasc as string) gadm_1_hasc,
    safe_cast(gadm_1_name as string) gadm_1_name,
    safe_cast(gadm_2_gid as string) gadm_2_gid,
    safe_cast(gadm_2_hasc as string) gadm_2_hasc,
    safe_cast(gadm_2_name as string) gadm_2_name,
    safe_cast(gadm_3_gid as string) gadm_3_gid,
    safe_cast(gadm_3_hasc as string) gadm_3_hasc,
    safe_cast(gadm_3_name as string) gadm_3_name,
    safe_cast(iso_3166_2 as string) iso_3166_2,
    safe_cast(nuts_1_id as string) nuts_1_id,
    safe_cast(nuts_1_name as string) nuts_1_name,
    safe_cast(nuts_2_id as string) nuts_2_id,
    safe_cast(nuts_2_name as string) nuts_2_name,
    safe_cast(nuts_3_id as string) nuts_3_id,
    safe_cast(nuts_3_name as string) nuts_3_name,
    safe_cast(fips_state as string) fips_state,
    safe_cast(fips_county as string) fips_county
from
    {{ set_datalake_project("world_dasanaike_sage_staging.spatial_admin_crosswalk") }}
    as t
