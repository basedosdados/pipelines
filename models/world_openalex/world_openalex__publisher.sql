{{
    config(
        schema="world_openalex",
        alias="publisher",
        materialized="table",
        cluster_by=["publisher_id"],
    )
}}


select
    safe_cast(publisher_id as string) publisher_id,
    safe_cast(display_name as string) display_name,
    safe_cast(hierarchy_level as string) hierarchy_level,
    safe_cast(parent_publisher_id as string) parent_publisher_id,
    safe_cast(country_codes as string) country_codes,
    safe_cast(ror_id as string) ror_id,
    safe_cast(wikidata_id as string) wikidata_id,
    safe_cast(homepage_url as string) homepage_url,
    safe_cast(works_count as int64) works_count,
    safe_cast(cited_by_count as int64) cited_by_count,
    safe_cast(two_year_mean_citedness as float64) two_year_mean_citedness,
    safe_cast(h_index as int64) h_index,
    safe_cast(i10_index as int64) i10_index,
    safe_cast(created_date as date) created_date,
    safe_cast(updated_date as date) updated_date
from {{ set_datalake_project("world_openalex_staging.publisher") }} as t
