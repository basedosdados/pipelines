{{
    config(
        schema="us_stanford_dime",
        alias="contributor",
        materialized="table",
    )
}}


select
    safe_cast(contributor_id as string) contributor_id,
    safe_cast(contributor_type as string) contributor_type,
    safe_cast(contributor_gender as string) contributor_gender,
    safe_cast(is_corporation as string) is_corporation,
    safe_cast(contributor_cfscore as float64) contributor_cfscore,
    safe_cast(is_projected as string) is_projected,
    safe_cast(number_distinct_recipients as int64) number_distinct_recipients,
    safe_cast(first_cycle_active as int64) first_cycle_active,
    safe_cast(last_cycle_active as int64) last_cycle_active,
    safe_cast(most_recent_name as string) most_recent_name,
    safe_cast(most_recent_address as string) most_recent_address,
    safe_cast(most_recent_city as string) most_recent_city,
    safe_cast(most_recent_state as string) most_recent_state,
    safe_cast(most_recent_zipcode as string) most_recent_zipcode,
    safe_cast(most_recent_latitude as float64) most_recent_latitude,
    safe_cast(most_recent_longitude as float64) most_recent_longitude,
    safe_cast(most_recent_occupation as string) most_recent_occupation,
    safe_cast(most_recent_employer as string) most_recent_employer,
    safe_cast(most_recent_transaction_id as string) most_recent_transaction_id,
    date(
        safe_cast(most_recent_transaction_date as datetime)
    ) most_recent_transaction_date
from {{ set_datalake_project("us_stanford_dime_staging.contributor") }} as t
