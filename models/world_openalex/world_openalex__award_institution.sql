{{
    config(
        schema="world_openalex",
        alias="award_institution",
        materialized="table",
        cluster_by=["award_id"],
    )
}}


select
    safe_cast(award_id as string) award_id,
    safe_cast(institution_id as string) institution_id
from {{ set_datalake_project("world_openalex_staging.award_institution") }} as t
