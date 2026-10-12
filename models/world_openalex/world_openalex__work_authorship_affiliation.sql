{{
    config(
        schema="world_openalex",
        alias="work_authorship_affiliation",
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
    safe_cast(affiliation_sequence as string) affiliation_sequence,
    safe_cast(raw_affiliation_string as string) raw_affiliation_string,
    safe_cast(institution_ids as string) institution_ids
from
    {{ set_datalake_project("world_openalex_staging.work_authorship_affiliation") }}
    as t
