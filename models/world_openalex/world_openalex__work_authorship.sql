{{
    config(
        schema="world_openalex",
        alias="work_authorship",
        materialized="table",
        partition_by={
            "field": "publication_year",
            "data_type": "int64",
            "range": {"start": 1500, "end": 2031, "interval": 1},
        },
        cluster_by=["work_id"],
    )
}}


select
    safe_cast(publication_year as int64) publication_year,
    safe_cast(work_id as string) work_id,
    safe_cast(author_sequence as string) author_sequence,
    safe_cast(author_id as string) author_id,
    safe_cast(author_position as string) author_position,
    safe_cast(author_name as string) author_name,
    safe_cast(orcid as string) orcid,
    safe_cast(raw_author_name as string) raw_author_name,
    safe_cast(raw_orcid as string) raw_orcid,
    safe_cast(is_corresponding as bool) is_corresponding
from {{ set_datalake_project("world_openalex_staging.work_authorship") }} as t
