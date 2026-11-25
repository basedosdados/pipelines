{{
    config(
        schema="au_qld_ecq_elections",
        alias="result_voting_centre",
        materialized="table",
        partition_by={
            "field": "year",
            "data_type": "int64",
            "range": {"start": 2013, "end": 2035, "interval": 1},
        },
    )
}}


select
    safe_cast(year as int64) year,
    safe_cast(election_id as string) election_id,
    safe_cast(contest_id as string) contest_id,
    safe_cast(lga_id as string) lga_id,
    safe_cast(lga_code as string) lga_code,
    safe_cast(state_electoral_division_id as string) state_electoral_division_id,
    safe_cast(government_level as string) government_level,
    safe_cast(contest_type as string) contest_type,
    safe_cast(voting_system as string) voting_system,
    safe_cast(lga_name as string) lga_name,
    safe_cast(district_name as string) district_name,
    safe_cast(voting_centre_id as string) voting_centre_id,
    safe_cast(count_status as string) count_status,
    safe_cast(count_type as string) count_type,
    safe_cast(ballot_order_number as string) ballot_order_number,
    safe_cast(voting_centre_name as string) voting_centre_name,
    safe_cast(voting_centre_type_code as string) voting_centre_type_code,
    safe_cast(voting_centre_district_name as string) voting_centre_district_name,
    safe_cast(ballot_name as string) ballot_name,
    safe_cast(party_code as string) party_code,
    safe_cast(party_name as string) party_name,
    safe_cast(votes as int64) votes,
    safe_cast(percentage as float64) percentage,
    safe_cast(votes_total as int64) votes_total,
    safe_cast(votes_formal as int64) votes_formal,
    safe_cast(votes_informal as int64) votes_informal
from
    {{ set_datalake_project("au_qld_ecq_elections_staging.result_voting_centre") }} as t
