{{
    config(
        alias="congress_member",
        schema="br_bd_diretorios_us",
        materialized="table",
    )
}}
select
    safe_cast(id_bioguide as string) id_bioguide,
    safe_cast(name_first as string) name_first,
    safe_cast(name_middle as string) name_middle,
    safe_cast(name_last as string) name_last,
    safe_cast(name_suffix as string) name_suffix,
    safe_cast(name_official_full as string) name_official_full,
    safe_cast(birthday as date) birthday,
    safe_cast(gender as string) gender,
    safe_cast(id_govtrack as string) id_govtrack,
    safe_cast(id_icpsr as string) id_icpsr,
    safe_cast(id_lis as string) id_lis,
    safe_cast(id_thomas as string) id_thomas,
    safe_cast(id_opensecrets as string) id_opensecrets,
    safe_cast(id_votesmart as string) id_votesmart,
    safe_cast(id_cspan as string) id_cspan,
    safe_cast(id_fec as string) id_fec,
    safe_cast(id_wikidata as string) id_wikidata,
    safe_cast(wikipedia as string) wikipedia,
    safe_cast(ballotpedia as string) ballotpedia
from {{ set_datalake_project("br_bd_diretorios_us_staging.congress_member") }} as t
