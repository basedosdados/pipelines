{{
    config(
        alias="committee_membership",
        schema="us_congress_legislators",
        materialized="table",
    )
}}
select
    safe_cast(id_committee as string) id_committee,
    safe_cast(id_bioguide as string) id_bioguide,
    safe_cast(party as string) party,
    safe_cast(rank as int64) rank,
    safe_cast(title as string) title,
    safe_cast(chamber as string) chamber
from
    {{ set_datalake_project("us_congress_legislators_staging.committee_membership") }}
    as t
