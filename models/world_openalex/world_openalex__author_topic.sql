{{
    config(
        schema="world_openalex",
        alias="author_topic",
        materialized="table",
        cluster_by=["author_id"],
    )
}}


select
    safe_cast(author_id as string) author_id,
    safe_cast(topic_id as string) topic_id,
    safe_cast(works_count as int64) works_count,
    safe_cast(topic_share as float64) topic_share
from {{ set_datalake_project("world_openalex_staging.author_topic") }} as t
