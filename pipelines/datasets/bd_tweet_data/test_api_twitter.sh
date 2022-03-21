BEARER_TOKEN=AAAAAAAAAAAAAAAAAAAAAF8lXQEAAAAAak%2FulN%2Bx15ztfHfKWVrsp%2FiIGiQ%3DRPstb26vCts7ZDGy9HMAtKKzZuDZuaXJ1q4IIOHJ0RKz6A23aH

curl --request GET \
  --url 'https://api.twitter.com/2/tweets/search/recent?query=from%3Atwitterdev&tweet.fields=public_metrics' \
  --header 'Authorization: Bearer $BEARER_TOKEN'

    