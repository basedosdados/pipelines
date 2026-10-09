{{
    config(
        schema="au_aec_elections",
        alias="disclosure_election_return",
        materialized="table",
        partition_by={
            "field": "year",
            "data_type": "int64",
            "range": {"start": 1998, "end": 2035, "interval": 1},
        },
    )
}}


select
    safe_cast(year as int64) year,
    safe_cast(election_name as string) election_name,
    safe_cast(return_type as string) return_type,
    safe_cast(name as string) name,
    safe_cast(party_id as string) party_id,
    safe_cast(party_name as string) party_name,
    safe_cast(electorate_name as string) electorate_name,
    safe_cast(electorate_state as string) electorate_state,
    safe_cast(nil_return as string) nil_return,
    safe_cast(amendment_number as string) amendment_number,
    safe_cast(total_gift_value as float64) total_gift_value,
    safe_cast(number_of_donors as int64) number_of_donors,
    safe_cast(total_electoral_expenditure as float64) total_electoral_expenditure,
    safe_cast(
        discretionary_benefits_received as float64
    ) discretionary_benefits_received,
    safe_cast(broadcasting_cost as float64) broadcasting_cost,
    safe_cast(publishing_cost as float64) publishing_cost,
    safe_cast(display_ad_cost as float64) display_ad_cost,
    safe_cast(direct_mailing_cost as float64) direct_mailing_cost,
    safe_cast(campaign_material_cost as float64) campaign_material_cost,
    safe_cast(opinion_poll_cost as float64) opinion_poll_cost
from
    {{ set_datalake_project("au_aec_elections_staging.disclosure_election_return") }}
    as t
