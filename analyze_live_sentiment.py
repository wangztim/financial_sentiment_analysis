import asyncio
import sys
from aiohttp import ClientSession
from typing import Tuple, List

import pandas as pd
import matplotlib.pyplot as plt
from models.fetchers import (TwitterFetcher, StocktwitsFetcher, MessageFetcher,
                             RedditFetcher)
from models.message import (Message, to_dict)
from sentiment_model.sentiment_analyzer import SentimentAnalyzer

import os
WORKING_DIR = os.path.dirname(os.path.abspath(__file__))

analyzer = SentimentAnalyzer(
    path=os.path.join(WORKING_DIR, 'sentiment_model/_model/'))


async def fetchTwits(ticker, sources=["stocktwits", "twitter"]):
    st_fetcher = StocktwitsFetcher()
    tw_fetcher = TwitterFetcher()
    re_fetcher = RedditFetcher()

    fetchers: List[MessageFetcher] = [st_fetcher, tw_fetcher, re_fetcher]

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
    df = pd.DataFrame([to_dict(m) for m in messages])
    df.set_index('id', inplace=True)

    unlabeled = df[df["sentiment"] == -69]
    results = analyzer.classify(unlabeled.body.tolist())

    # ? I don't know if we should care about spams.
    # spam_ratings = seq_outs_to_ints(
    #     spam_filterer(unlabeled_tokens["input_ids"],
    #                   attention_mask=unlabeled_tokens["attention_mask"]))

    # df.loc[unlabeled.index, "is_spam"] = spam_ratings

    # unlabeled_hams = unlabeled[unlabeled["is_spam"] == 0]
    # unlabeled_ham_tokens = tokenize(unlabeled_hams.body.tolist())

    # unlabeled_hams_sentiments = seq_outs_to_ints(
    #     sentiment_analyzer(
    #         unlabeled_ham_tokens["input_ids"],
    #         attention_mask=unlabeled_ham_tokens["attention_mask"]))

    df.loc[unlabeled.index, "sentiment"] = [int(r) for r in results]
    print(df["sentiment"].value_counts())
    df["sentiment"].value_counts().plot.bar()
    plt.show()


async def main(ticker):
    messages = await fetchTwits(ticker)
    plotStockSentiment(messages)


if __name__ == "__main__":
    ticker = sys.argv[1]
    asyncio.run(main(ticker))
