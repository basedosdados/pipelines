{{
    config(
        schema="world_openalex",
        alias="work_mesh",
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
    safe_cast(descriptor_id as string) descriptor_id,
    safe_cast(descriptor_name as string) descriptor_name,
    safe_cast(qualifier_id as string) qualifier_id,
    safe_cast(qualifier_name as string) qualifier_name,
    safe_cast(is_major_topic as bool) is_major_topic
from {{ set_datalake_project("world_openalex_staging.work_mesh") }} as t
