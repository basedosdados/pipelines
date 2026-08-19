{{
    config(
        schema="us_fec_campaign_finance",
        alias="candidate",
        materialized="table",
        partition_by={
            "field": "year",
            "data_type": "int64",
            "range": {"start": 1980, "end": 2031, "interval": 1},
        },
    )
}}


select
    safe_cast(year as int64) year,
    safe_cast(candidate_id as string) candidate_id,
    safe_cast(principal_committee_id as string) principal_committee_id,
    safe_cast(candidate_name as string) candidate_name,
    safe_cast(party as string) party,
    safe_cast(election_year as int64) election_year,
    safe_cast(office as string) office,
    safe_cast(office_state as string) office_state,
    safe_cast(office_district as string) office_district,
    safe_cast(incumbent_challenger_status as string) incumbent_challenger_status,
    safe_cast(candidate_status as string) candidate_status,
    safe_cast(address_1 as string) address_1,
    safe_cast(address_2 as string) address_2,
    safe_cast(city as string) city,
    safe_cast(state as string) state,
    safe_cast(zip_code as string) zip_code
from {{ set_datalake_project("us_fec_campaign_finance_staging.candidate") }} as t
