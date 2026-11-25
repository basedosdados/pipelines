{{
    config(
        schema="world_wil_wid",
        alias="country",
        materialized="table",
    )
}}


select
    safe_cast(country_code as string) country_code,
    safe_cast(base_code as string) base_code,
    safe_cast(country_iso2 as string) country_iso2,
    safe_cast(title_name as string) title_name,
    safe_cast(short_name as string) short_name,
    safe_cast(region as string) region,
    safe_cast(region2 as string) region2,
    safe_cast(geography_type as string) geography_type,
    safe_cast(conversion as string) conversion
from {{ set_datalake_project("world_wil_wid_staging.country") }} as t
