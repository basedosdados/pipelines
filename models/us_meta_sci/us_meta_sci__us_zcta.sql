{{
    config(
        schema="us_meta_sci",
        alias="us_zcta",
        materialized="table",
        cluster_by=["user_zcta_id"],
    )
}}


select
    safe_cast(user_zcta_id as string) user_zcta_id,
    safe_cast(friend_zcta_id as string) friend_zcta_id,
    safe_cast(scaled_sci as int64) scaled_sci
from {{ set_datalake_project("us_meta_sci_staging.us_zcta") }} as t
