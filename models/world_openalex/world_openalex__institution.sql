{{
    config(
        schema="world_openalex",
        alias="institution",
        materialized="table",
        cluster_by=["institution_id"],
    )
}}


select
    safe_cast(institution_id as string) institution_id,
    safe_cast(ror_id as string) ror_id,
    safe_cast(display_name as string) display_name,
    safe_cast(country_code as string) country_code,
    safe_cast(type as string) type,
    safe_cast(city as string) city,
    safe_cast(region as string) region,
    safe_cast(geonames_city_id as string) geonames_city_id,
    safe_cast(latitude as float64) latitude,
    safe_cast(longitude as float64) longitude,
    safe_cast(is_super_system as bool) is_super_system,
    safe_cast(status as string) status,
    safe_cast(homepage_url as string) homepage_url,
    safe_cast(wikidata_id as string) wikidata_id,
    safe_cast(works_count as int64) works_count,
    safe_cast(cited_by_count as int64) cited_by_count,
    safe_cast(two_year_mean_citedness as float64) two_year_mean_citedness,
    safe_cast(h_index as int64) h_index,
    safe_cast(i10_index as int64) i10_index,
    safe_cast(created_date as date) created_date,
    safe_cast(updated_date as date) updated_date
from {{ set_datalake_project("world_openalex_staging.institution") }} as t
