import pandas as pd
from bd_twitter import auth,create_headers,create_url,connect_to_endpoint,flatten

from dotenv import load_dotenv
load_dotenv()

#Inputs for the request
bearer_token = auth()
headers = create_headers(bearer_token)
keyword = "xbox lang:en"

start_time = "2021-12-01T00:00:00.000Z"
end_time = "2021-12-22T00:00:00.000Z"

max_results = 100
url = create_url(keyword, start_time,end_time, max_results)

json_response = connect_to_endpoint(url[0], headers, url[1])

data = [flatten(i) for i in json_response["data"]]

data = pd.DataFrame(data)

print(data.head())



