{{
    config(
        schema="world_openalex",
        alias="work_abstract",
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
    safe_cast(abstract as string) abstract
from {{ set_datalake_project("world_openalex_staging.work_abstract") }} as t
