{{
    config(
        schema="world_openalex",
        alias="funder",
        materialized="table",
        cluster_by=["funder_id"],
    )
}}


select
    safe_cast(funder_id as string) funder_id,
    safe_cast(display_name as string) display_name,
    safe_cast(country_code as string) country_code,
    safe_cast(description as string) description,
    safe_cast(ror_id as string) ror_id,
    safe_cast(wikidata_id as string) wikidata_id,
    safe_cast(crossref_funder_id as string) crossref_funder_id,
    safe_cast(homepage_url as string) homepage_url,
    safe_cast(works_count as int64) works_count,
    safe_cast(cited_by_count as int64) cited_by_count,
    safe_cast(awards_count as int64) awards_count,
    safe_cast(two_year_mean_citedness as float64) two_year_mean_citedness,
    safe_cast(h_index as int64) h_index,
    safe_cast(i10_index as int64) i10_index,
    safe_cast(created_date as date) created_date,
    safe_cast(updated_date as date) updated_date
from {{ set_datalake_project("world_openalex_staging.funder") }} as t
