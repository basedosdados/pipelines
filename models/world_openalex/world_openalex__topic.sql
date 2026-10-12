{{
    config(
        schema="world_openalex",
        alias="topic",
        materialized="table",
        cluster_by=["topic_id"],
    )
}}


select
    safe_cast(topic_id as string) topic_id,
    safe_cast(display_name as string) display_name,
    safe_cast(description as string) description,
    safe_cast(subfield_id as string) subfield_id,
    safe_cast(field_id as string) field_id,
    safe_cast(domain_id as string) domain_id,
    safe_cast(keywords as string) keywords,
    safe_cast(wikipedia_url as string) wikipedia_url,
    safe_cast(works_count as int64) works_count,
    safe_cast(cited_by_count as int64) cited_by_count,
    safe_cast(created_date as date) created_date,
    safe_cast(updated_date as date) updated_date
from {{ set_datalake_project("world_openalex_staging.topic") }} as t
