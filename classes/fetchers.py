from abc import ABC, abstractmethod
from classes.message import Message, Sentiment
from datetime import datetime
from dateutil import parser
import requests
import os
import aiohttp
import asyncpraw
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
    def convertFetchedToMessage(self, fetched) -> Message:
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
                return [
                    self.convertFetchedToMessage(m, ticker) for m in messages
                ]
            elif (status_code == 429 or status_code == 403):
                raise requests.exceptions.ConnectionError("Rate limited.")
            else:
                raise RuntimeError("API call unsuccessful. Code: " +
                                   str(res.status_code))

    def __initParams(self, ticker) -> dict:
        params = {}
        markers = self.markers_cache.get(ticker, None)
        if markers is None:
            return {}
        else:
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
        if not isinstance(markers['newest']['datetime'], datetime):
            markers['newest']['datetime'] = parser.parse(
                markers['newest']['datetime'], ignoretz=True)
        if not isinstance(markers['oldest']['datetime'], datetime):
            markers['oldest']['datetime'] = parser.parse(
                markers['oldest']['datetime'], ignoretz=True)
        self.markers_cache[ticker] = markers

    def convertFetchedToMessage(self, twit: {}, ticker: str) -> Message:
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

        markers = self.markers_cache.get(ticker, None)

        if markers is None:
            obj = {}
            obj["oldest"] = {}
            obj["oldest"]['id'] = twit['id']
            obj["oldest"]['datetime'] = created_date_time

            obj["newest"] = {}
            obj["newest"]['id'] = twit['id']
            obj["newest"]['datetime'] = created_date_time
            self.setTickerMarkers(ticker, obj)

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
                    self.convertFetchedToMessage(d) for d in data
                    if (d['lang'] == "en" and "$" in d['text'])
                ]
                return filtered_data
            else:
                raise RuntimeError("Failure. Code: " + str(status_code))

    def convertFetchedToMessage(self, tweet: {}) -> Message:
        likes = tweet.get('public_metrics', {}).get('like_count')
        created_date_time = parser.parse(tweet['created_at'])

        # TODO: Look up how to get comments num on Twitter
        message = Message(tweet['id'], tweet['text'], tweet["author_id"],
                          created_date_time, "Twitter", Sentiment(-69), likes)
        return message


class RedditFetcher(MessageFetcher):
    subreddits = ["stocks", "wallstreetbets", "investing"]

    def __init__(self):
        client_id = os.environ.get('REDDIT_CLIENT_ID')
        client_secret = os.environ.get('REDDIT_CLIENT_SECRET')
        username = os.environ.get('REDDIT_USERNAME')
        password = os.environ.get('REDDIT_PASSWORD')

        if None in (client_id, client_secret, username, password):
            raise RuntimeError("Missing authentication info!")

        self.reddit = asyncpraw.Reddit(user_agent="Thunderstorm/1.0 (Linux)",
                                       client_id=client_id,
                                       client_secret=client_secret,
                                       username=username,
                                       password=password)

    async def fetch(self,
                    ticker,
                    session: aiohttp.ClientSession,
                    params=None,
                    headers=None) -> [Message]:
        subreddit = await self.reddit.subreddit("+".join(self.subreddits))
        messages = []
        async for submission in subreddit.search(ticker,
                                                 sort="relevance",
                                                 time_filter="day",
                                                 limit=100):
            messages.append(self.convertFetchedToMessage(submission))
            # TODO Add Functionality to parse comment tree
        return messages

    async def listenToSubreddits(self, processing_fn):
        subreddit = await self.reddit.subreddit("+".join(self.subreddits))
        async for submission in subreddit.stream.submissions():
            message = self.convertFetchedToMessage(submission)
            processing_fn(message)  # fn to process the message.

    def convertFetchedToMessage(self, post) -> Message:
        if isinstance(post, asyncpraw.models.Submission):
            return self._convertSubmissions(post)
        elif isinstance(post, asyncpraw.models.Comment):
            return self._convertComments(post)
        else:
            return None

    def _convertSubmissions(self, s: asyncpraw.models.Submission) -> Message:
        full_text = s.title + s.selftext
        author_id = s.author.name
        created = datetime.fromtimestamp(s.created_utc)
        return Message(s.id, full_text, author_id, created, "Reddit",
                       Sentiment(-69), s.score, s.num_comments)

    def _convertComments(self, comment: asyncpraw.models.Comment) -> Message:
        raise NotImplementedError()
