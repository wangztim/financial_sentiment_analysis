import asyncio
import sys
from aiohttp import ClientSession
from typing import Tuple, List
from collections import Counter
import pandas as pd
import matplotlib.pyplot as plt
from classes.fetchers import (TwitterFetcher, StocktwitsFetcher,
                              MessageFetcher, RedditFetcher)
from classes.message import (Sentiment, Message, to_dict)
from transformers import (DistilBertForSequenceClassification,
                          DistilBertTokenizerFast)

import torch.nn.functional as F
from torch import argmax

tokenizer = DistilBertTokenizerFast.from_pretrained('distilbert-base-uncased')
spam_filterer = DistilBertForSequenceClassification.from_pretrained(
    "./post_filtering_model/model")
sentiment_analyzer = DistilBertForSequenceClassification.from_pretrained(
    "./sentiment_model/model")


def tokenize(input_strings):
    return tokenizer(input_strings,
                     max_length=160,
                     padding="max_length",
                     return_tensors='pt',
                     truncation=True)


def seq_outs_to_ints(seq_outs):
    logits = seq_outs.logits
    soft = F.softmax(logits, dim=1)
    results = argmax(soft, dim=1)
    return results.numpy()


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
    df["is_spam"] = 0

    # TODO: Fix Inefficient use of Tokenization.
    unlabeled = df[df["sentiment"] == -69]
    unlabeled_tokens = tokenize(unlabeled.body.tolist())
    spam_ratings = seq_outs_to_ints(
        spam_filterer(unlabeled_tokens["input_ids"],
                      attention_mask=unlabeled_tokens["attention_mask"]))

    df.loc[unlabeled.index, "is_spam"] = spam_ratings

    unlabeled_hams = unlabeled[unlabeled["is_spam"] == 0]
    unlabeled_ham_tokens = tokenize(unlabeled_hams.body.tolist())

    unlabeled_hams_sentiments = seq_outs_to_ints(
        sentiment_analyzer(
            unlabeled_ham_tokens["input_ids"],
            attention_mask=unlabeled_ham_tokens["attention_mask"]))

    df.loc[unlabeled_hams.index, "sentiment"] = unlabeled_hams_sentiments
    df["sentiment"].value_counts().plot.bar()
    plt.show()


async def main(ticker):
    messages = await fetchTwits(ticker)
    plotStockSentiment(messages)


if __name__ == "__main__":
    ticker = sys.argv[1]
    asyncio.run(main(ticker))
