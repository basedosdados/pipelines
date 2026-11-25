{{
    config(
        schema="ca_ourresearch_openalex",
        alias="author_alternative_name",
        materialized="table",
        cluster_by=["author_id"],
    )
}}


select
    safe_cast(author_id as string) author_id,
    safe_cast(alternative_name as string) alternative_name
from
    {{
        set_datalake_project(
            "ca_ourresearch_openalex_staging.author_alternative_name"
        )
    }} as t
