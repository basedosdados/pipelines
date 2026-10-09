{{
    config(
        schema="world_openalex",
        alias="work_location",
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
    safe_cast(location_sequence as string) location_sequence,
    safe_cast(location_id as string) location_id,
    safe_cast(source_id as string) source_id,
    safe_cast(is_primary as bool) is_primary,
    safe_cast(is_best_open_access as bool) is_best_open_access,
    safe_cast(is_open_access as bool) is_open_access,
    safe_cast(is_published as bool) is_published,
    safe_cast(is_accepted as bool) is_accepted,
    safe_cast(version as string) version,
    safe_cast(license as string) license,
    safe_cast(landing_page_url as string) landing_page_url,
    safe_cast(pdf_url as string) pdf_url,
    safe_cast(raw_source_name as string) raw_source_name,
    safe_cast(raw_type as string) raw_type,
    safe_cast(provenance as string) provenance
from {{ set_datalake_project("world_openalex_staging.work_location") }} as t
