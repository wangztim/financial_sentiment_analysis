import asyncio
import sys
from aiohttp import ClientSession
from typing import Tuple, List
from collections import Counter
import matplotlib.pyplot as plt
from classes.fetchers import (TwitterFetcher, StocktwitsFetcher, Message,
                              MessageFetcher, RedditFetcher)
import json


async def fetchTwits(ticker, sources=["stocktwits", "twitter", "reddit"]):
    st_fetcher = StocktwitsFetcher()
    tw_fetcher = TwitterFetcher()
    re_fetcher = RedditFetcher()

    fetchers: List[MessageFetcher] = [re_fetcher]

    async with ClientSession() as session:
        futures = []

        for fetcher in fetchers:
            futures.append(fetcher.fetch(ticker, session))

        messages: Tuple[List[Message]] = await asyncio.gather(
            *futures, return_exceptions=True)

        out = []

        for m in messages:
            if isinstance(m, list):
                out.extend(m)

        return out


def plotStockSentiment(messages: [Message]):
    labeled = [m for m in messages if m.sentiment is not None]
    unlabeled = [m for m in messages if m.sentiment is None]
    sentiments = Counter([str(int(m.sentiment)) for m in labeled])
    names = list(sentiments.keys())
    values = list(sentiments.values())
    plt.barh(names, values)
    plt.show()


async def main(ticker):
    messages = await fetchTwits(ticker)
    print(json.dumps(messages[0]))
    # plotStockSentiment(messages)


if __name__ == "__main__":
    ticker = sys.argv[1]

    asyncio.run(main(ticker))
