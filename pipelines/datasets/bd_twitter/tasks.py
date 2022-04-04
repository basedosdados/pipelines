"""
Tasks for bd_tweet_data
"""
import os
import collections
from datetime import datetime, timedelta, timezone

from prefect import task
import requests
from tqdm import tqdm
from requests_oauthlib import OAuth1
import pandas as pd
import numpy as np
from dotenv import load_dotenv
load_dotenv()

from pipelines.constants import constants

@task(
    max_retries=constants.TASK_MAX_RETRIES.value,
    retry_delay=timedelta(seconds=constants.TASK_RETRY_DELAY.value),
)
def crawler():
    bearer_token = os.getenv('TWITTER_TOKEN')

    os.system('mkdir -p /tmp/data')

    headers = _create_headers(bearer_token)
    keyword = "xbox lang:en"

    # non_public_metrics only available for last 30 days
    now = datetime.now(timezone.utc)
    thirty_before = now - timedelta(days = 30)

    start_time = thirty_before.strftime("%Y-%m-%dT00:00:00.000Z")
    end_time=now.strftime("%Y-%m-%dT00:00:00.000Z")

    max_results = 100
    url = _create_url(keyword, start_time,end_time, max_results)
    json_response = _connect_to_endpoint(url[0], headers, url[1])
    data = [_flatten(i) for i in json_response["data"]]
    df1 = pd.DataFrame(data)

    ids = [k for k in df1['id']]

    temp_dict ={}
    for id_field in tqdm(ids):
        # retweets don't have non_public_metrics
        if not df1[df1.id==id_field].text.to_list()[0].startswith('RT @'):
            url = f'https://api.twitter.com/2/tweets/{id_field}?tweet.fields=non_public_metrics'

            CONSUMER_KEY=os.getenv('CONSUMER_KEY')
            CONSUMER_SECRET=os.getenv('CONSUMER_SECRET')
            ACCESS_TOKEN=os.getenv('ACCESS_TOKEN')
            ACCESS_SECRET=os.getenv('ACCESS_SECRET')

            headeroauth = OAuth1(CONSUMER_KEY, CONSUMER_SECRET,ACCESS_TOKEN, ACCESS_SECRET, signature_type='auth_header')
            try:
                r = requests.get(url, auth=headeroauth)

                json_response = r.json()
                temp_dict.update({id_field: json_response['data']['non_public_metrics']})
            except:
                print(json_response['errors'])
        else:
            temp_dict.update({id_field: {'url_link_clicks': np.nan, 'user_profile_clicks': np.nan, 'impression_count': np.nan}})



    df2 = pd.DataFrame(temp_dict).T
    df2.columns = ['non_public_metrics_'+k for k in df2.columns]

    df = df1.set_index('id').join(df2)

    df.columns = [col.replace('non_public_metrics_','').replace('public_metrics_','') for col in df.columns]

    full_filepath = f'/tmp/data/metricas_tweets/dia={now.strftime("%Y-%m-%d")}/metricas_tweets.csv'
    df.to_csv(full_filepath)

    return '/tmp/data/metricas_tweets'


def _create_headers(bearer_token):
    headers = {"Authorization": "Bearer {}".format(bearer_token)}
    return headers


def _create_url(keyword, start_date, end_date, max_results = 10):
    ttid = 1184334528837574656
    search_url = f"https://api.twitter.com/2/users/{ttid}/tweets" #Change to the endpoint you want to collect data from

    #change params based on the endpoint you are using
    query_params = {'start_time': start_date,
                    'end_time': end_date,
                    'max_results': max_results,
                    'tweet.fields': 'public_metrics,created_at',
                    'next_token': {}}
    
    return (search_url, query_params)


def _connect_to_endpoint(url, headers, params, next_token = None):
    params['next_token'] = next_token   #params object received from create_url function
    response = requests.request("GET", url, headers = headers, params = params)
    print("Endpoint Response Code: " + str(response.status_code))
    if response.status_code != 200:
        raise Exception(response.status_code, response.text)
    return response.json()


def _flatten(d, parent_key='', sep='_'):
    items = []
    for k, v in d.items():
        new_key = parent_key + sep + k if parent_key else k
        if isinstance(v, collections.MutableMapping):
            items.extend(_flatten(v, new_key, sep=sep).items())
        else:
            items.append((new_key, v))
    return dict(items)
