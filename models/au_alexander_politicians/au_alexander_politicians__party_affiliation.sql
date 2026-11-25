{{
    config(
        alias="party_affiliation",
        schema="au_alexander_politicians",
        materialized="table",
    )
}}
select
    safe_cast(id_politician as string) id_politician,
    safe_cast(party_abbreviation as string) party_abbreviation,
    safe_cast(party_name as string) party_name,
    safe_cast(party_simplified_name as string) party_simplified_name,
    safe_cast(date_start as date) date_start,
    safe_cast(date_end as date) date_end,
    safe_cast(indicator_party_changed_name as boolean) indicator_party_changed_name,
    safe_cast(
        indicator_specific_date_inputted as boolean
    ) indicator_specific_date_inputted,
    safe_cast(comments as string) comments
from
    {{ set_datalake_project("au_alexander_politicians_staging.party_affiliation") }}
    as t
