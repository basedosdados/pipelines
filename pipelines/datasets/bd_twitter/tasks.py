"""
Tasks for bd_tweet_data
"""
import os
from datetime import datetime, timedelta
import pytz

from prefect import task
import requests
from tqdm import tqdm
from requests_oauthlib import OAuth1
import pandas as pd
import numpy as np
from typing import Union
from dotenv import load_dotenv
load_dotenv()

from pipelines.utils.utils import get_storage_blobs, log
from pipelines.datasets.bd_twitter.utils import (
    create_headers,
    create_url,
    connect_to_endpoint,
    flatten
)
from pipelines.constants import constants

@task(
    max_retries=constants.TASK_MAX_RETRIES.value,
    retry_delay=timedelta(seconds=constants.TASK_RETRY_DELAY.value),
)
def echo(message:str)-> None:
    log(message)

@task(
    max_retries=constants.TASK_MAX_RETRIES.value,
    retry_delay=timedelta(seconds=constants.TASK_RETRY_DELAY.value),
)
def has_new_tweets()-> bool:
    now = datetime.now(tz=pytz.UTC)
    os.system(f'mkdir -p /tmp/data/metricas_tweets/dia={now.strftime("%Y-%m-%d")}/')

    bearer_token = os.getenv('TWITTER_TOKEN')
    headers = create_headers(bearer_token)
    keyword = "xbox lang:en"

    # non_public_metrics only available for last 30 days
    before = now - timedelta(days = 29)
    start_time = before.strftime("%Y-%m-%dT00:00:00.000Z")
    end_time=now.strftime("%Y-%m-%dT00:00:00.000Z")
    max_results = 100
    url = create_url(keyword, start_time,end_time, max_results)
    json_response = connect_to_endpoint(url[0], headers, url[1])
    data = [flatten(i) for i in json_response["data"]]
    df1 = pd.DataFrame(data)

    blobs = get_storage_blobs(dataset_id='bd_twitter', table_id='metricas_tweets')
    now = datetime.now(tz=pytz.UTC)

    if len(blobs)!=0:
        dfs =[]
        for blob in blobs:
            url_data = blob.public_url
            df = pd.read_csv(url_data, dtype={'id':str})
            dfs.append(df)

        df=dfs[0].append(dfs[1:])
        ids = df.id.to_list()
        df1=df1[~df1['id'].isin(ids)]

    if len(df1)>0:
        log(f'{len(df1)} new tweets founded')
        
    df1.to_csv('/tmp/basic_metrics.csv', index=False)

    return not df1.empty

@task(
    max_retries=constants.TASK_MAX_RETRIES.value,
    retry_delay=timedelta(seconds=constants.TASK_RETRY_DELAY.value),
)
def crawler_metricas() -> str:
    now = datetime.now(tz=pytz.UTC)
    df1 = pd.read_csv('/tmp/basic_metrics.csv', dtype = {'url_link_clicks': int, 'user_profile_clicks': int, 'impression_count': int})
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
                log(json_response['errors'])
        else:
            temp_dict.update({id_field: {'url_link_clicks': np.nan, 'user_profile_clicks': np.nan, 'impression_count': np.nan}})



    df2 = pd.DataFrame(temp_dict).T
    df2.columns = ['non_public_metrics_'+k for k in df2.columns]

    df = df1.set_index('id').join(df2)

    df.columns = [col.replace('non_public_metrics_','').replace('public_metrics_','') for col in df.columns]

    full_filepath = f'/tmp/data/metricas_tweets/dia={now.strftime("%Y-%m-%d")}/metricas_tweets.csv'
    df.to_csv(full_filepath)

    return '/tmp/data/metricas_tweets'

@task(
    max_retries=constants.TASK_MAX_RETRIES.value,
    retry_delay=timedelta(seconds=constants.TASK_RETRY_DELAY.value),
)
def crawler_metricas_agg():
    CONSUMER_KEY=os.getenv('CONSUMER_KEY')
    CONSUMER_SECRET=os.getenv('CONSUMER_SECRET')
    ACCESS_TOKEN=os.getenv('ACCESS_TOKEN')
    ACCESS_SECRET=os.getenv('ACCESS_SECRET')

    now = datetime.now(tz=pytz.UTC)
    os.system('mkdir -p /tmp/data/metricas_tweets_agg/')

    dfs=[]
    blobs = get_storage_blobs(dataset_id='bd_twitter', table_id='metricas_tweets')
    for blob in blobs:
        url_data = blob.public_url
        df = pd.read_csv(url_data, dtype={'id':str}, parse_dates=['created_at'])
        dfs.append(df)

    df=dfs[0].append(dfs[1:])


    df1 = df.groupby('created_at').agg({
        'retweet_count': 'sum',
        'reply_count': 'sum',
        'like_count': 'sum',
        'quote_count': 'sum', 
        'impression_count': 'sum',
        'user_profile_clicks': 'sum',
        'url_link_clicks': 'sum'
    })

    df1= df1.reset_index()

    url = 'https://api.twitter.com/2/users/1184334528837574656?user.fields=public_metrics'

    headeroauth = OAuth1(CONSUMER_KEY, CONSUMER_SECRET,ACCESS_TOKEN, ACCESS_SECRET, signature_type='auth_header')
    try:
        r = requests.get(url, auth=headeroauth)

        json_response = r.json()
        result = json_response['data']['public_metrics']
    except:
        log(json_response['errors'])

    df2 = pd.DataFrame(result, index=[1])
    now = datetime.now().strftime('%Y-%m-%d')
    df2['date'] = now

    df1['date'] = [date.strftime('%Y-%m-%d') for date in df1['created_at']]

    df1=df1.drop('created_at', axis=1)

    if now not in df1['date'].to_list():
        part = pd.DataFrame([[np.nan]*len(df1.columns)],columns=df1.columns)
        part['date']=now
        df1=df1.append(part)

    df = df1.set_index('date').join(df2.set_index('date'))

    filepath = '/tmp/data/metricas_tweets_agg/metricas_tweets_agg.csv'
    df.to_csv(filepath)

    return filepath