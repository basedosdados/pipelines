{{
    config(
        schema="au_aph_hansard",
        alias="sitting_day",
        materialized="table",
        partition_by={
            "field": "year",
            "data_type": "int64",
            "range": {"start": 1901, "end": 2031, "interval": 1},
        },
        cluster_by=["chamber"],
    )
}}


select
    safe_cast(year as int64) year,
    safe_cast(date as date) date,
    safe_cast(chamber as string) chamber,
    safe_cast(parliament_number as string) parliament_number,
    safe_cast(session_number as string) session_number,
    safe_cast(period_number as string) period_number,
    safe_cast(is_proof as string) is_proof,
    safe_cast(speech_count as int64) speech_count,
    safe_cast(page_count as int64) page_count,
    safe_cast(source_url as string) source_url
from {{ set_datalake_project("au_aph_hansard_staging.sitting_day") }} as t
