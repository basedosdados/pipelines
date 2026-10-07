{{
    config(
        schema="world_openalex",
        alias="author_affiliation",
        materialized="table",
        partition_by={
            "field": "year",
            "data_type": "int64",
            "range": {"start": 1900, "end": 2031, "interval": 1},
        },
        cluster_by=["author_id"],
    )
}}


select
    safe_cast(year as int64) year,
    safe_cast(author_id as string) author_id,
    safe_cast(institution_id as string) institution_id
from {{ set_datalake_project("world_openalex_staging.author_affiliation") }} as t
