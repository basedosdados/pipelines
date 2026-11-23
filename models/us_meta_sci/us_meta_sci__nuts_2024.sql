{{
    config(
        schema="us_meta_sci",
        alias="nuts_2024",
        materialized="table",
        cluster_by=["nuts_level", "user_country_id", "user_region_id"],
    )
}}


select
    safe_cast(nuts_level as string) nuts_level,
    safe_cast(user_country_id as string) user_country_id,
    safe_cast(friend_country_id as string) friend_country_id,
    safe_cast(user_region_id as string) user_region_id,
    safe_cast(friend_region_id as string) friend_region_id,
    safe_cast(scaled_sci as int64) scaled_sci
from {{ set_datalake_project("us_meta_sci_staging.nuts_2024") }} as t
