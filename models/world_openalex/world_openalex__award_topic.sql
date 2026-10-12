{{
    config(
        schema="world_openalex",
        alias="award_topic",
        materialized="table",
        cluster_by=["award_id"],
    )
}}


select
    safe_cast(award_id as string) award_id,
    safe_cast(topic_id as string) topic_id,
    safe_cast(score as float64) score
from {{ set_datalake_project("world_openalex_staging.award_topic") }} as t
