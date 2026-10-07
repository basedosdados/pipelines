{{
    config(
        schema="us_meta_sci",
        alias="us_county",
        materialized="table",
        cluster_by=["user_county_id"],
    )
}}


select
    safe_cast(user_county_id as string) user_county_id,
    safe_cast(friend_county_id as string) friend_county_id,
    safe_cast(scaled_sci as int64) scaled_sci
from {{ set_datalake_project("us_meta_sci_staging.us_county") }} as t
