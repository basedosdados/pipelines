{{
    config(
        alias="committee",
        schema="us_congress_legislators",
        materialized="table",
    )
}}
select
    safe_cast(id_committee as string) id_committee,
    safe_cast(id_committee_parent as string) id_committee_parent,
    safe_cast(thomas_id as string) thomas_id,
    safe_cast(name as string) name,
    safe_cast(committee_type as string) committee_type,
    safe_cast(is_subcommittee as bool) is_subcommittee,
    safe_cast(is_current as bool) is_current,
    safe_cast(house_committee_id as string) house_committee_id,
    safe_cast(senate_committee_id as string) senate_committee_id,
    safe_cast(jurisdiction as string) jurisdiction,
    safe_cast(url as string) url,
    safe_cast(address as string) address,
    safe_cast(phone as string) phone,
    safe_cast(wikipedia as string) wikipedia
from {{ set_datalake_project("us_congress_legislators_staging.committee") }} as t
