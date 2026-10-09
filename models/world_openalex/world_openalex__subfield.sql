{{
    config(
        schema="world_openalex",
        alias="subfield",
        materialized="table",
        cluster_by=["subfield_id"],
    )
}}


select
    safe_cast(subfield_id as string) subfield_id,
    safe_cast(display_name as string) display_name,
    safe_cast(description as string) description,
    safe_cast(field_id as string) field_id,
    safe_cast(domain_id as string) domain_id,
    safe_cast(works_count as int64) works_count,
    safe_cast(cited_by_count as int64) cited_by_count,
    safe_cast(created_date as date) created_date,
    safe_cast(updated_date as date) updated_date
from {{ set_datalake_project("world_openalex_staging.subfield") }} as t
