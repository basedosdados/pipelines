{{
    config(
        schema="world_openalex",
        alias="author_counts_by_year",
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
    safe_cast(works_count as int64) works_count,
    safe_cast(oa_works_count as int64) oa_works_count,
    safe_cast(cited_by_count as int64) cited_by_count
from {{ set_datalake_project("world_openalex_staging.author_counts_by_year") }} as t
