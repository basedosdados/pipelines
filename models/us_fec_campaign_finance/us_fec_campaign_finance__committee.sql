{{
    config(
        schema="us_fec_campaign_finance",
        alias="committee",
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
    safe_cast(committee_id as string) committee_id,
    safe_cast(candidate_id as string) candidate_id,
    safe_cast(committee_name as string) committee_name,
    safe_cast(treasurer_name as string) treasurer_name,
    safe_cast(committee_designation as string) committee_designation,
    safe_cast(committee_type as string) committee_type,
    safe_cast(party as string) party,
    safe_cast(filing_frequency as string) filing_frequency,
    safe_cast(organization_type as string) organization_type,
    safe_cast(connected_organization_name as string) connected_organization_name,
    safe_cast(address_1 as string) address_1,
    safe_cast(address_2 as string) address_2,
    safe_cast(city as string) city,
    safe_cast(state as string) state,
    safe_cast(zip_code as string) zip_code
from {{ set_datalake_project("us_fec_campaign_finance_staging.committee") }} as t
