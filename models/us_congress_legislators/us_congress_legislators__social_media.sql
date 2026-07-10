{{
    config(
        alias="social_media",
        schema="us_congress_legislators",
        materialized="table",
    )
}}
select
    safe_cast(id_bioguide as string) id_bioguide,
    safe_cast(twitter as string) twitter,
    safe_cast(twitter_id as string) twitter_id,
    safe_cast(facebook as string) facebook,
    safe_cast(instagram as string) instagram,
    safe_cast(instagram_id as string) instagram_id,
    safe_cast(youtube as string) youtube,
    safe_cast(youtube_id as string) youtube_id,
    safe_cast(mastodon as string) mastodon
from {{ set_datalake_project("us_congress_legislators_staging.social_media") }} as t
