{{
    config(
        schema="ca_ourresearch_openalex",
        alias="author_last_known_institution",
        materialized="table",
        cluster_by=["author_id"],
    )
}}


select
    safe_cast(author_id as string) author_id,
    safe_cast(institution_id as string) institution_id
from
    {{
        set_datalake_project(
            "ca_ourresearch_openalex_staging.author_last_known_institution"
        )
    }} as t
