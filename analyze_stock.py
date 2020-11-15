import os
import requests
from datetime import datetime


def fetchTickerFromTwitter(ticker):
    endpoint = "https://api.twitter.com/2/tweets/search/recent"
    bearer = os.environ['TWITTER_BEARER_TOKEN']
    headers = {'Authorization': 'Bearer {}'.format(bearer)}
    params = {"query": '"$TSLA"',
              "max_results": 100, "tweet.fields": "lang,created_at,public_metrics"}
    res = None

    try:
        res = requests.get(endpoint, headers=headers, timeout=5, params=params)
    except:
        print("Unable to fetch Twitter data")
        return []

    if res.status_code == 200:
        data = res.json()['data']
        filtered_data = [d for d in data if (d['lang']
                                             == "en" and "$" in d['text'])]
        return filtered_data
    else:
        return []


def fetchTickerFromStocktwits(ticker):
    endpoint = "https://api.stocktwits.com/api/2/streams/symbol/" + ticker + ".json"
    res = None

    def procecssTwit(twit):
        sentiment = twit.get('entities', {}).get(
            'sentiment', {})
        sentiment_val = 0
        if sentiment == None:
            sentiment_val = -69
        elif (sentiment.get('basic') == "Bullish"):
            sentiment_val = 1
        elif (sentiment.get('basic') == "Bearish"):
            sentiment_val = -1
        else:
            print(sentiment)
            sentiment_val = 0

        created_date_time = datetime.strptime(
            twit['created_at'], "%Y-%m-%dT%H:%M:%SZ")

        body = twit['body'].lower()

        all_symbols = list(
            map(lambda x: x['symbol'].lower(), twit['symbols']))

        tickers_in_body = list(filter(
            lambda symbol: symbol in body, all_symbols))

        row = {'id': twit['id'], 'sentiment': sentiment_val,
               'body': body, 'likes': twit.get('likes', {}).get(
            'total', 0)}

        return row

    try:
        res = requests.get(endpoint, timeout=5)
    except:
        return []

    if res.status_code == 200:
        messages = res.json()['messages']
        return [procecssTwit(m) for m in messages]
    else:
        return []


# def plotSentimentGraph(tickers):
print(fetchTickerFromTwitter("AMD"))
