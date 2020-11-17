from abc import ABC, abstractmethod
from classes.message import Message, Sentiment
from datetime import datetime
from dateutil import parser
import requests
import os
import aiohttp

from enum import Enum


class Direction(Enum):
    NONE = "NONE"
    FORWARD = "FORWARD"
    BACKWARD = "BACKWARD"


class MessageFetcher(ABC):
    @abstractmethod
    def fetchMessages(self, ticker, params=None, headers=None) -> [Message]:
        pass

    @abstractmethod
    def processFetched(self, message: {}) -> Message:
        pass


class StocktwitsFetcher(MessageFetcher):
    direction: Direction
    marker_cache: dict

    def __init__(self, direction=Direction.NONE):
        self.direction = direction
        self.marker_cache = {}

    async def fetchMessages(self, ticker, session: aiohttp.ClientSession,
                            params=None, headers=None) -> [Message]:
        endpoint = f'https://api.stocktwits.com/api/2/streams/symbol/{ticker}.json'
        if params is None:
            params = self.__initParams(ticker)

        async with session.get(endpoint, params=params,
                               headers=headers, timeout=5) as res:
            status_code = res.status
            if status_code == 200:
                json = await res.json()
                messages = json["messages"]
                return [self.processFetched(m, ticker) for m in messages]
            elif (status_code == 429 or status_code == 403):
                raise requests.exceptions.ConnectionError(
                    "Rate limit exhausted.")
            else:
                raise RuntimeError("API call unsuccessful. Code: " +
                                   str(res.status_code))

    def __initParams(self, ticker) -> dict:
        params = {}
        ticker_id = self.marker_cache[ticker]["id"]
        if ticker_id is None:
            return params
        if self.direction == Direction.BACKWARD:
            params['max'] = ticker_id
        elif self.direction == Direction.FORWARD:
            params['since'] = ticker_id
        return params

    def setTickerMarker(self, ticker: str, marker_id: str, marker_datetime: datetime):
        self.marker_cache[ticker] = {
            "id": marker_id,
            "datetime": marker_datetime}

    def processFetched(self, twit: {}, ticker: str) -> Message:
        sentiment = twit.get('entities', {}).get(
            'sentiment', {})
        sentiment_val = 0
        if sentiment is None:
            sentiment_val = -69
        elif (sentiment.get('basic') == "Bullish"):
            sentiment_val = 1
        elif (sentiment.get('basic') == "Bearish"):
            sentiment_val = -1
        else:
            sentiment_val = 0

        created_date_time = parser.parse(twit['created_at'], ignoretz=True)

        body = twit['body'].lower()

        all_symbols = list(
            map(lambda x: x['symbol'].lower(), twit['symbols']))

        tickers_in_body = list(filter(
            lambda symbol: symbol in body, all_symbols))

        if self.direction is Direction.BACKWARD:
            if self.marker_cache[ticker]['datetime'] > created_date_time:
                self.marker_cache[ticker]['id'] = twit['id']
                self.marker_cache[ticker]['datetime'] = created_date_time
        elif self.direction is Direction.FORWARD:
            if self.marker_cache[ticker]['datetime'] < created_date_time:
                self.marker_cache[ticker]['id'] = twit['id']
                self.marker_cache[ticker]['datetime'] = created_date_time

        message = Message(body, twit['id'],
                          created_date_time,
                          tickers_in_body,
                          Sentiment(sentiment_val),
                          twit.get('likes', {}).get('total', 0))

        return message


class TwitterFetcher(MessageFetcher):
    def fetchMessages(self, ticker, params=None, headers=None) -> [Message]:
        endpoint = "https://api.twitter.com/2/tweets/search/recent"

        bearer = os.environ['TWITTER_BEARER_TOKEN']

        if not headers:
            headers = {'Authorization': 'Bearer {}'.format(bearer)}

        if not params:
            params = {"query": '"${}"'.format(ticker),
                      "max_results": 100,
                      "tweet.fields": "lang,created_at,public_metrics"}

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
