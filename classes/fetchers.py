from abc import ABC, abstractmethod
from classes.message import Message, Sentiment
from datetime import datetime
from dateutil import parser
import requests
import os


class MessageFetcher(ABC):
    @abstractmethod
    def fetchMessages(self, ticker, params=None, headers=None, proxies=None) -> [Message]:
        pass

    @abstractmethod
    def processFetched(self, message: {}) -> Message:
        pass


class StocktwitsFetcher(MessageFetcher):
    def fetchMessages(self, ticker, params=None, headers=None, proxies=None) -> [Message]:
        endpoint = "https://api.stocktwits.com/api/2/streams/symbol/" + ticker + ".json"
        res = requests.get(endpoint, timeout=5, params=params,
                           headers=headers, proxies=proxies)

        if res.status_code == 200:
            messages = res.json()['messages']
            return [self.processFetched(m) for m in messages]
        elif (res.status_code == 429 or res.status_code == 403):
            raise requests.exceptions.ConnectionError(
                "Rate limit exhausted. Must switch to a proxy.")
        else:
            raise RuntimeError("API call unsuccessful. Code: " +
                               str(res.status_code))

    def processFetched(self, twit: {}) -> Message:
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
            sentiment_val = 0

        created_date_time = parser.parse(twit['created_at'])

        body = twit['body'].lower()

        all_symbols = list(
            map(lambda x: x['symbol'].lower(), twit['symbols']))

        tickers_in_body = list(filter(
            lambda symbol: symbol in body, all_symbols))

        message = Message(body, twit['id'], created_date_time, tickers_in_body, Sentiment(sentiment_val), twit.get('likes', {}).get(
            'total', 0))

        return message


class TwitterFetcher(MessageFetcher):
    def fetchMessages(self, ticker, params=None, headers=None, proxies=None) -> [Message]:
        endpoint = "https://api.twitter.com/2/tweets/search/recent"

        bearer = os.environ['TWITTER_BEARER_TOKEN']

        if not headers:
            headers = {'Authorization': 'Bearer {}'.format(bearer)}

        if not params:
            params = {"query": '"${}"'.format(ticker),
                      "max_results": 100, "tweet.fields": "lang,created_at,public_metrics"}

        res = requests.get(endpoint, headers=headers,
                           timeout=5, params=params)

        if res.status_code == 200:
            data = res.json()['data']
            filtered_data = [self.processFetched(d) for d in data if (d['lang']
                                                                      == "en" and "$" in d['text'])]
            return filtered_data
        else:
            raise RuntimeError("API call unsuccessful. Code: " +
                               str(res.status_code))

    def processFetched(self, tweet: {}) -> Message:
        likes = tweet.get('public_metrics', {}).get('like_count')
        created_date_time = parser.parse(tweet['created_at'])

        message = Message(tweet['text'], tweet['id'],
                          created_date_time, None, None, likes)
        return message
