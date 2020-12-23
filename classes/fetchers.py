from abc import ABC, abstractmethod
from classes.message import Message, Sentiment
from datetime import datetime
from dateutil import parser
import requests
import os
import aiohttp
import asyncio
from typing import Tuple

from enum import Enum


class Direction(Enum):
    NONE = "NONE"
    FORWARD = "FORWARD"
    BACKWARD = "BACKWARD"


class MessageFetcher(ABC):
    @abstractmethod
    async def fetch(self,
                    ticker,
                    session: aiohttp.ClientSession,
                    params=None,
                    headers=None) -> [Message]:
        pass

    @abstractmethod
    def processFetched(self, message: {}) -> Message:
        pass


class StocktwitsFetcher(MessageFetcher):
    direction: Direction
    markers_cache: dict

    def __init__(self, direction=Direction.NONE):
        self.direction = direction
        self.markers_cache = {}

    async def fetch(self,
                    ticker,
                    session: aiohttp.ClientSession,
                    params=None,
                    headers=None) -> [Message]:
        endpoint = f'https://api.stocktwits.com/api/2/streams/symbol/{ticker}.json'
        if params is None:
            params = self.__initParams(ticker)

        async with session.get(endpoint,
                               params=params,
                               headers=headers,
                               timeout=8) as res:
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
        markers = self.markers_cache.get(ticker)
        if self.direction == Direction.BACKWARD:
            if markers["oldest"] is not None:
                params['max'] = markers["oldest"]["id"]
        elif self.direction == Direction.FORWARD:
            if markers["newest"] is not None:
                params['since'] = markers["newest"]["id"]
        return params

    def getTickerMarkers(self, ticker: str) -> dict:
        return self.markers_cache.get(ticker)

    def setTickerMarkers(self, ticker: str, markers: dict):
        assert isinstance(markers['newest']['datetime'], datetime)
        assert isinstance(markers['oldest']['datetime'], datetime)
        self.markers_cache[ticker] = markers

    def processFetched(self, twit: {}, ticker: str) -> Message:
        sentiment = twit.get('entities', {}).get('sentiment', {})
        sentiment_val = 0
        if sentiment is None:
            sentiment_val = -69
        elif (sentiment.get('basic') == "Bullish"):
            sentiment_val = 1
        elif (sentiment.get('basic') == "Bearish"):
            sentiment_val = 0

        created_date_time = parser.parse(twit['created_at'], ignoretz=True)

        body = twit['body'].lower()

        markers = self.markers_cache.get(ticker)

        likes = twit.get('likes', {}).get('total', 0)
        replies = twit.get('conversation', {}).get('replies', 0)
        user = twit.get("user", {}).get("id", "")

        if markers:
            if markers["oldest"]['datetime'] > created_date_time:
                markers["oldest"]['id'] = twit['id']
                markers["oldest"]['datetime'] = created_date_time

            if markers["newest"]['datetime'] < created_date_time:
                markers["newest"]['id'] = twit['id']
                markers["newest"]['datetime'] = created_date_time

        message = Message(twit['id'], body, user,
                          created_date_time, "StockTwits",
                          Sentiment(sentiment_val), likes, replies)

        return message


class TwitterFetcher(MessageFetcher):
    async def fetch(self,
                    ticker,
                    session: aiohttp.ClientSession,
                    params=None,
                    headers=None) -> [Message]:
        endpoint = "https://api.twitter.com/2/tweets/search/recent"

        bearer = os.environ['TWITTER_BEARER_TOKEN']
        default_headers = {'Authorization': 'Bearer {}'.format(bearer)}
        default_params = {
            "query": '"${}"'.format(ticker),
            "max_results": 100,
            "tweet.fields": "lang,created_at,public_metrics"
        }

        if headers:
            default_headers.update(headers)

        if params:
            default_params.update(params)

        async with session.get(endpoint,
                               params=default_params,
                               headers=default_headers,
                               timeout=8) as res:
            status_code = res.status
            if status_code == 200:
                j = await res.json()
                data = j['data']
                filtered_data = [
                    self.processFetched(d) for d in data
                    if (d['lang'] == "en" and "$" in d['text'])
                ]
                return filtered_data
            else:
                raise RuntimeError("API call unsuccessful. Code: " +
                                   str(status_code))

    def processFetched(self, tweet: {}) -> Message:
        likes = tweet.get('public_metrics', {}).get('like_count')
        created_date_time = parser.parse(tweet['created_at'])

        # TODO: Look up how to get comments num on Twitter
        message = Message(tweet['id'], tweet['text'], tweet["author_id"],
                          created_date_time, "Twitter", Sentiment(-69), likes)
        return message


class RedditFetcher(MessageFetcher):
    subreddits = ["r/stocks", "r/wallstreetbets"]
    endpoint = "http://www.reddit.com/"

    # Authentication is not necessary!
    def __init__(self, auth=False):
        if auth:
            client_id = os.environ['REDDIT_CLIENT_ID']
            client_secret = os.environ['REDDIT_CLIENT_SECRET']
            username = os.environ['REDDIT_USERNAME']
            password = os.environ['REDDIT_PASSWORD']

            client_auth = requests.auth.HTTPBasicAuth(client_id, client_secret)
            post_data = {
                "grant_type": "password",
                "username": username,
                "password": password
            }
            headers = {"User-Agent": "Thunderstorm/1.0 (Linux)"}
            response = requests.post(
                "https://www.reddit.com/api/v1/access_token",
                auth=client_auth,
                data=post_data,
                headers=headers)
            if response.status_code == 200:
                self.access_token = response.json()["access_token"]
                self.token_type = response.json()["token_type"]
            else:
                raise RuntimeError("Unable to authenticate with reddit.")

    async def fetch(self,
                    ticker,
                    session: aiohttp.ClientSession,
                    params=None,
                    headers=None) -> [Message]:

        default_headers = {
            # "Authorization": self.token_type + " " + self.access_token
        }

        default_params = {
            "q": '{}'.format(ticker),
            "limit": 5,
            "t": 'day',
            "sort": "new",
            "restrict_sr": "true",
        }

        async def get_fn(sr):
            async with session.get(self.endpoint + sr + "/search.json",
                                   params=default_params,
                                   headers=default_headers,
                                   timeout=8) as res:
                status_code = res.status
                if status_code == 200:
                    j = await res.json()
                    return [
                        self.processFetched(child["data"])
                        for child in j["data"]["children"]
                    ]
                else:
                    raise RuntimeError("API call unsuccessful. Code: " +
                                       str(status_code))
                    return res

        operations = [get_fn(sr) for sr in self.subreddits]
        status: Tuple[int] = await asyncio.gather(*operations,
                                                  return_exceptions=True)
        return [
            item for sublist in status for item in sublist
            if sublist is not Exception
        ]

    def processFetched(self, post: {}) -> Message:
        full_text = post.get("title", "") + ": " + post.get("selftext", "")
        return Message(post["name"], full_text, post["author_fullname"],
                       datetime.fromtimestamp(post["created_utc"]), "Reddit",
                       Sentiment(-69), post["ups"] - post["downs"],
                       post["num_comments"])
