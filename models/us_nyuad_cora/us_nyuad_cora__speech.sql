{{
    config(
        alias="speech",
        schema="us_nyuad_cora",
        materialized="table",
        partition_by={
            "field": "year",
            "data_type": "int64",
            "range": {"start": 1873, "end": 2031, "interval": 1},
        },
        cluster_by=["chamber", "bioguide_id"],
    )
}}
select
    safe_cast(year as int64) year,
    safe_cast(date as date) date,
    safe_cast(congress as string) congress,
    safe_cast(session as string) session,
    safe_cast(chamber as string) chamber,
    safe_cast(speech_id as string) speech_id,
    safe_cast(bioguide_id as string) bioguide_id,
    safe_cast(state_id as string) state_id,
    safe_cast(state_abbreviation as string) state_abbreviation,
    safe_cast(speaker_name_raw as string) speaker_name_raw,
    safe_cast(speaker_first_name as string) speaker_first_name,
    safe_cast(speaker_last_name as string) speaker_last_name,
    safe_cast(party as string) party,
    safe_cast(gender as string) gender,
    safe_cast(cap_major_topic as string) cap_major_topic,
    safe_cast(speech_text as string) speech_text,
    safe_cast(bills as string) bills,
    safe_cast(joint_resolutions as string) joint_resolutions,
    safe_cast(concurrent_resolutions as string) concurrent_resolutions,
    safe_cast(simple_resolutions as string) simple_resolutions,
    safe_cast(volume as string) volume,
    safe_cast(pages as string) pages,
    safe_cast(source_document_id as string) source_document_id,
    safe_cast(source_url as string) source_url,
    safe_cast(pdf_url as string) pdf_url
from {{ set_datalake_project("us_nyuad_cora_staging.speech") }} as t
