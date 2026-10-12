{{
    config(
        schema="world_openalex",
        alias="author",
        materialized="table",
        cluster_by=["author_id"],
    )
}}


select
    safe_cast(author_id as string) author_id,
    safe_cast(display_name as string) display_name,
    safe_cast(full_name as string) full_name,
    safe_cast(orcid as string) orcid,
    safe_cast(works_count as int64) works_count,
    safe_cast(cited_by_count as int64) cited_by_count,
    safe_cast(two_year_mean_citedness as float64) two_year_mean_citedness,
    safe_cast(h_index as int64) h_index,
    safe_cast(i10_index as int64) i10_index,
    safe_cast(created_date as date) created_date,
    safe_cast(updated_date as date) updated_date
from {{ set_datalake_project("world_openalex_staging.author") }} as t
