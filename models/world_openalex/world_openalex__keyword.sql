{{
    config(
        schema="world_openalex",
        alias="keyword",
        materialized="table",
        cluster_by=["keyword_id"],
    )
}}


select
    safe_cast(keyword_id as string) keyword_id,
    safe_cast(display_name as string) display_name,
    safe_cast(works_count as int64) works_count,
    safe_cast(cited_by_count as int64) cited_by_count,
    safe_cast(created_date as date) created_date,
    safe_cast(updated_date as date) updated_date
from {{ set_datalake_project("world_openalex_staging.keyword") }} as t
