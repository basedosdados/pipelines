{{
    config(
        schema="world_openalex",
        alias="award_investigator",
        materialized="table",
        cluster_by=["award_id"],
    )
}}


select
    safe_cast(award_id as string) award_id,
    safe_cast(investigator_sequence as string) investigator_sequence,
    safe_cast(role as string) role,
    safe_cast(given_name as string) given_name,
    safe_cast(family_name as string) family_name,
    safe_cast(orcid as string) orcid,
    safe_cast(role_start_date as date) role_start_date,
    safe_cast(affiliation_name as string) affiliation_name,
    safe_cast(country_code as string) country_code
from {{ set_datalake_project("world_openalex_staging.award_investigator") }} as t
