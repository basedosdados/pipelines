{{
    config(
        schema="world_openalex",
        alias="author_alternative_name",
        materialized="table",
        cluster_by=["author_id"],
    )
}}


select
    safe_cast(author_id as string) author_id,
    safe_cast(alternative_name as string) alternative_name
from {{ set_datalake_project("world_openalex_staging.author_alternative_name") }} as t
