{{
    config(
        materialized="incremental",
        partition_by={
            "field": "upload_day",
            "data_type": "date",
            "granularity": "month",
        },
    )
}}


select *
from
    (
        select
            safe_cast(upload_ts as int64) upload_ts,
            extract(date from timestamp_millis(upload_ts * 1000)) as upload_day,
            safe_cast(id as string) id,
            safe_cast(text as string) text,
            safe_cast(created_at as string) created_at,
            safe_cast(retweet_count as int64) retweet_count,
            safe_cast(reply_count as int64) reply_count,
            safe_cast(like_count as int64) like_count,
            safe_cast(quote_count as int64) quote_count,
            safe_cast(impression_count as float64) impression_count,
            safe_cast(user_profile_clicks as float64) user_profile_clicks,
            safe_cast(url_link_clicks as float64) url_link_clicks,
            safe_cast(following_count as int64) following_count,
            safe_cast(followers_count as int64) followers_count,
            safe_cast(tweet_count as int64) tweet_count,
            safe_cast(listed_count as int64) listed_count
        from {{ set_datalake_project("br_bd_indicadores_staging.twitter_metrics") }}
    )
where
    upload_day <= current_date('America/Sao_Paulo')

    {% if is_incremental() %}

        {% set max_partition = (
            run_query(
                "SELECT gr FROM (SELECT IF(max(upload_day) > CURRENT_DATE('America/Sao_Paulo'), CURRENT_DATE('America/Sao_Paulo'), max(upload_day)) as gr FROM "
                ~ this
                ~ ")"
            )
            .columns[0]
            .values()[0]
        ) %}

        and upload_day > ("{{ max_partition }}")

    {% endif %}
