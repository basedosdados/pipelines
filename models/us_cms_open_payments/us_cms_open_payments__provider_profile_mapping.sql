{{
    config(
        schema="us_cms_open_payments",
        alias="provider_profile_mapping",
        materialized="table",
    )
}}


select
    safe_cast(primary_profile_id as string) primary_profile_id,
    safe_cast(secondary_profile_id as string) secondary_profile_id
from {{ set_datalake_project("us_cms_open_payments_staging.provider_profile_mapping") }} as t
