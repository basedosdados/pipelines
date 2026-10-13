{{
    config(
        schema="au_vic_vec_elections",
        alias="disclosure_gift",
        materialized="table",
        partition_by={
            "field": "year",
            "data_type": "int64",
            "range": {"start": 2002, "end": 2035, "interval": 1},
        },
    )
}}


select
    safe_cast(year as int64) year,
    safe_cast(donation_id as string) donation_id,
    safe_cast(date_made as date) date_made,
    safe_cast(date_received as date) date_received,
    safe_cast(donor_id as string) donor_id,
    safe_cast(donor_name as string) donor_name,
    safe_cast(donor_suburb as string) donor_suburb,
    safe_cast(donor_state as string) donor_state,
    safe_cast(recipient_id as string) recipient_id,
    safe_cast(recipient_name as string) recipient_name,
    safe_cast(recipient_party_id as string) recipient_party_id,
    safe_cast(recipient_party_name as string) recipient_party_name,
    safe_cast(gift_value as float64) gift_value,
    safe_cast(donation_type as string) donation_type,
    safe_cast(disclosure_status as string) disclosure_status,
    safe_cast(electorate_name as string) electorate_name
from {{ set_datalake_project("au_vic_vec_elections_staging.disclosure_gift") }} as t
