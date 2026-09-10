{{
    config(
        schema="au_ecq_elections",
        alias="candidate",
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
    safe_cast(ballot_order_number as string) ballot_order_number,
    safe_cast(ballot_name as string) ballot_name,
    safe_cast(candidate_surname as string) candidate_surname,
    safe_cast(candidate_given_names as string) candidate_given_names,
    safe_cast(party_code as string) party_code,
    safe_cast(party_name as string) party_name,
    safe_cast(is_declared_elected as string) is_declared_elected
from {{ set_datalake_project("au_ecq_elections_staging.candidate") }} as t
