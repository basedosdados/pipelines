select
    extract(date from timestamp_millis(upload_ts * 1000)) as upload_day,
    sum(retweet_count) as retweets,
    sum(reply_count) as replies,
    sum(like_count) as likes,
    sum(quote_count) as quote_tweets,
    sum(impression_count) as impressions,
    sum(user_profile_clicks) as profile_clicks,
    sum(url_link_clicks) as links_clicks,
    any_value(following_count) as followings,
    any_value(followers_count) as followers,
    any_value(tweet_count) as tweets,
    any_value(listed_count) as listed
from {{ set_datalake_project("br_bd_indicadores.twitter_metrics") }}
group by upload_day
order by upload_day
