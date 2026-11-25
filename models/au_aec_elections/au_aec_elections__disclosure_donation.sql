{{
    config(
        schema="au_aec_elections",
        alias="disclosure_donation",
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
    safe_cast(disclosure_type as string) disclosure_type,
    safe_cast(financial_year as string) financial_year,
    safe_cast(election_name as string) election_name,
    safe_cast(return_type as string) return_type,
    safe_cast(direction as string) direction,
    safe_cast(donor_name as string) donor_name,
    safe_cast(recipient_name as string) recipient_name,
    safe_cast(donation_date as date) donation_date,
    safe_cast(value as float64) value
from {{ set_datalake_project("au_aec_elections_staging.disclosure_donation") }} as t
