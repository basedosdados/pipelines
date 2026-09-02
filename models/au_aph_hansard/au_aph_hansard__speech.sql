{{
    config(
        schema="au_aph_hansard",
        alias="speech",
        materialized="table",
        partition_by={
            "field": "year",
            "data_type": "int64",
            "range": {"start": 1901, "end": 2031, "interval": 1},
        },
        cluster_by=["chamber", "talk_type"],
    )
}}


select
    safe_cast(year as int64) year,
    safe_cast(date as date) date,
    safe_cast(chamber as string) chamber,
    safe_cast(parliament_number as string) parliament_number,
    safe_cast(session_number as string) session_number,
    safe_cast(period_number as string) period_number,
    safe_cast(speech_order as string) speech_order,
    safe_cast(speaker_id as string) speaker_id,
    safe_cast(talk_type as string) talk_type,
    safe_cast(debate_type as string) debate_type,
    safe_cast(debate_title as string) debate_title,
    safe_cast(subdebate_title as string) subdebate_title,
    safe_cast(speaker_name as string) speaker_name,
    safe_cast(electorate as string) electorate,
    safe_cast(party as string) party,
    safe_cast(role as string) role,
    safe_cast(in_government as string) in_government,
    safe_cast(first_speech as string) first_speech,
    safe_cast(time_stamp as string) time_stamp,
    safe_cast(page_number as string) page_number,
    safe_cast(body as string) body,
    safe_cast(word_count as int64) word_count
from {{ set_datalake_project("au_aph_hansard_staging.speech") }} as t
