from datetime import datetime

import os
import pandas as pd
import numpy as np
from requests_oauthlib import OAuth1
import requests

from dotenv import load_dotenv
load_dotenv()

bearer_token = os.getenv('TWITTER_TOKEN')
CONSUMER_KEY=os.getenv('CONSUMER_KEY')
CONSUMER_SECRET=os.getenv('CONSUMER_SECRET')
ACCESS_TOKEN=os.getenv('ACCESS_TOKEN')
ACCESS_SECRET=os.getenv('ACCESS_SECRET')

os.system('mkdir -p /tmp/data')


df = pd.read_csv('/tmp/data/metricas_tweets.csv', parse_dates = ['created_at'])

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

CONSUMER_KEY=os.getenv('CONSUMER_KEY')
CONSUMER_SECRET=os.getenv('CONSUMER_SECRET')
ACCESS_TOKEN=os.getenv('ACCESS_TOKEN')
ACCESS_SECRET=os.getenv('ACCESS_SECRET')

headeroauth = OAuth1(CONSUMER_KEY, CONSUMER_SECRET,ACCESS_TOKEN, ACCESS_SECRET, signature_type='auth_header')
try:
    r = requests.get(url, auth=headeroauth)

    json_response = r.json()
    result = json_response['data']['public_metrics']
except:
    print(json_response['errors'])

df2 = pd.DataFrame(result, index=[1])
now = datetime.now().strftime('%Y-%m-%d')
df2['date'] = now

df1['date'] = [date.strftime('%Y-%m-%d') for date in df1['created_at']]

df1=df1.drop('created_at', axis=1)


if now not in df1['date'].to_list():
    part = pd.DataFrame([[np.nan]*len(df1.columns)],columns=df1.columns)
    part['date']=now
    df1=df1.append(part)

# print(part)
# print(df1)

df = df1.set_index('date').join(df2.set_index('date'))

# df=df.reset_index()

df.to_csv('/tmp/data/metricas_tweets_agg.csv')
