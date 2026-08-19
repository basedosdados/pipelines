{{
    config(
        schema="us_fec_campaign_finance",
        alias="candidate_committee_link",
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
    safe_cast(committee_id as string) committee_id,
    safe_cast(linkage_id as string) linkage_id,
    safe_cast(candidate_election_year as int64) candidate_election_year,
    safe_cast(fec_election_year as int64) fec_election_year,
    safe_cast(committee_type as string) committee_type,
    safe_cast(committee_designation as string) committee_designation
from
    {{
        set_datalake_project(
            "us_fec_campaign_finance_staging.candidate_committee_link"
        )
    }} as t
