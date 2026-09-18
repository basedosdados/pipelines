{{
    config(
        alias="legislator",
        schema="us_congress_legislators",
        materialized="table",
    )
}}
select
    safe_cast(id_bioguide as string) id_bioguide,
    safe_cast(id_thomas as string) id_thomas,
    safe_cast(id_lis as string) id_lis,
    safe_cast(id_govtrack as string) id_govtrack,
    safe_cast(id_icpsr as string) id_icpsr,
    safe_cast(id_opensecrets as string) id_opensecrets,
    safe_cast(id_votesmart as string) id_votesmart,
    safe_cast(id_fec as string) id_fec,
    safe_cast(id_cspan as string) id_cspan,
    safe_cast(id_maplight as string) id_maplight,
    safe_cast(id_house_history as string) id_house_history,
    safe_cast(id_pictorial as string) id_pictorial,
    safe_cast(id_wikidata as string) id_wikidata,
    safe_cast(id_google_entity as string) id_google_entity,
    safe_cast(name_wikipedia as string) name_wikipedia,
    safe_cast(name_ballotpedia as string) name_ballotpedia,
    safe_cast(first_name as string) first_name,
    safe_cast(middle_name as string) middle_name,
    safe_cast(last_name as string) last_name,
    safe_cast(suffix as string) suffix,
    safe_cast(nickname as string) nickname,
    safe_cast(full_name as string) full_name,
    safe_cast(birthday as date) birthday,
    safe_cast(gender as string) gender,
    safe_cast(is_current as bool) is_current
from {{ set_datalake_project("us_congress_legislators_staging.legislator") }} as t
