import pandas as pd
from bd_twitter import auth,create_headers,create_url,connect_to_endpoint,flatten
import os

from dotenv import load_dotenv
load_dotenv()

os.system('mkdir -p /tmp/data')

#Inputs for the request
bearer_token = auth()
headers = create_headers(bearer_token)
keyword = "xbox lang:en"

start_time = "2021-12-01T00:00:00.000Z"
end_time = "2021-12-22T00:00:00.000Z"

max_results = 100
url = create_url(keyword, start_time,end_time, max_results)

json_response = connect_to_endpoint(url[0], headers, url[1])

print(json_response['errors'])

# data = [flatten(i) for i in json_response["data"]]

# data = pd.DataFrame(data)

# data.to_csv('/tmp/data/metricas_tweets.csv', index=False)

# for col in data.columns:
#     print(col)

# print(os.system('tree /tmp/data'))




