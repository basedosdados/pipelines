{{
    config(
        schema="au_qld_ecq_elections",
        alias="disclosure_gift",
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
    safe_cast(government_level as string) government_level,
    safe_cast(date_gift_made as date) date_gift_made,
    safe_cast(donor_name as string) donor_name,
    safe_cast(recipient_name as string) recipient_name,
    safe_cast(gift_value as float64) gift_value,
    safe_cast(election_name as string) election_name,
    safe_cast(is_political_donation as string) is_political_donation,
    safe_cast(has_electoral_committee as string) has_electoral_committee,
    safe_cast(electoral_committee_name as string) electoral_committee_name
from {{ set_datalake_project("au_qld_ecq_elections_staging.disclosure_gift") }} as t
