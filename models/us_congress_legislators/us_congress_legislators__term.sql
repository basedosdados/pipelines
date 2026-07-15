{{
    config(
        alias="term",
        schema="us_congress_legislators",
        materialized="table",
    )
}}
select
    safe_cast(id_bioguide as string) id_bioguide,
    safe_cast(term_type as string) term_type,
    safe_cast(term_start as date) term_start,
    safe_cast(term_end as date) term_end,
    safe_cast(state as string) state,
    safe_cast(district as int64) district,
    safe_cast(senate_class as int64) senate_class,
    safe_cast(state_rank as string) state_rank,
    safe_cast(party as string) party,
    safe_cast(caucus as string) caucus,
    safe_cast(how as string) how,
    safe_cast(end_type as string) end_type,
    safe_cast(url as string) url,
    safe_cast(address as string) address,
    safe_cast(phone as string) phone,
    safe_cast(fax as string) fax,
    safe_cast(contact_form as string) contact_form,
    safe_cast(office as string) office,
    safe_cast(rss_url as string) rss_url
from {{ set_datalake_project("us_congress_legislators_staging.term") }} as t
