{{
    config(
        schema="us_meta_sci",
        alias="country",
        materialized="table",
        cluster_by=["user_country_id"],
    )
}}


select
    safe_cast(user_country_id as string) user_country_id,
    safe_cast(friend_country_id as string) friend_country_id,
    safe_cast(scaled_sci as int64) scaled_sci
from {{ set_datalake_project("us_meta_sci_staging.country") }} as t
