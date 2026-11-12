{{
    config(
        alias="country",
        schema="se_gu_vdem",
        materialized="table",
    )
}}
select
    safe_cast(country_id as string) country_id,
    safe_cast(country_text_id as string) country_text_id,
    safe_cast(country_name as string) country_name,
    safe_cast(historical as string) historical,
    safe_cast(coding_start as int64) coding_start,
    safe_cast(coding_end as int64) coding_end,
    safe_cast(coding_start_contemporary as int64) coding_start_contemporary,
    safe_cast(coding_end_contemporary as int64) coding_end_contemporary,
    safe_cast(coding_start_historical as int64) coding_start_historical,
    safe_cast(coding_end_historical as int64) coding_end_historical,
    safe_cast(gap_start_1 as int64) gap_start_1,
    safe_cast(gap_end_1 as int64) gap_end_1,
    safe_cast(gap_start_2 as int64) gap_start_2,
    safe_cast(gap_end_2 as int64) gap_end_2,
    safe_cast(gap_start_3 as int64) gap_start_3,
    safe_cast(gap_end_3 as int64) gap_end_3
from {{ set_datalake_project("se_gu_vdem_staging.country") }} as t
