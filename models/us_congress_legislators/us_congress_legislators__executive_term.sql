{{
    config(
        alias="executive_term",
        schema="us_congress_legislators",
        materialized="table",
    )
}}
select
    safe_cast(id_bioguide as string) id_bioguide,
    safe_cast(id_icpsr_prez as string) id_icpsr_prez,
    safe_cast(id_govtrack as string) id_govtrack,
    safe_cast(first_name as string) first_name,
    safe_cast(middle_name as string) middle_name,
    safe_cast(last_name as string) last_name,
    safe_cast(suffix as string) suffix,
    safe_cast(nickname as string) nickname,
    safe_cast(full_name as string) full_name,
    safe_cast(birthday as date) birthday,
    safe_cast(gender as string) gender,
    safe_cast(term_type as string) term_type,
    safe_cast(term_start as date) term_start,
    safe_cast(term_end as date) term_end,
    safe_cast(party as string) party,
    safe_cast(how as string) how
from {{ set_datalake_project("us_congress_legislators_staging.executive_term") }} as t
