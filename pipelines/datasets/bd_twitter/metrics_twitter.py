import requests
from datetime import datetime, timedelta, timezone

from tqdm import tqdm
from bd_twitter import create_headers,create_url,connect_to_endpoint,flatten
import os
from requests_oauthlib import OAuth1
import pandas as pd
import numpy as np

from dotenv import load_dotenv
load_dotenv()

bearer_token = os.getenv('TWITTER_TOKEN')

os.system('mkdir -p /tmp/data')

headers = create_headers(bearer_token)
keyword = "xbox lang:en"

# non_public_metrics only available for last 30 days
now = datetime.now(timezone.utc)
thirty_before = now - timedelta(days = 30)

start_time = thirty_before.strftime("%Y-%m-%dT00:00:00.000Z")
end_time=now.strftime("%Y-%m-%dT00:00:00.000Z")

max_results = 100
url = create_url(keyword, start_time,end_time, max_results)
json_response = connect_to_endpoint(url[0], headers, url[1])
data = [flatten(i) for i in json_response["data"]]
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

df.to_csv('/tmp/data/metricas_tweets.csv')






