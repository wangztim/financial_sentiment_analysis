from classes.fetchers import TwitterFetcher, StocktwitsFetcher, Message
import seaborn as sns
import matplotlib.pyplot as plt
from collections import Counter


def plotStockSentiment(messages: [Message]):
    sentiments = Counter([str(int(m.sentiment)) for m in messages])
    names = list(sentiments.keys())
    values = list(sentiments.values())
    plt.barh(names, values)
    plt.show()


fetcher = StocktwitsFetcher()
twits = fetcher.fetchMessages("AMD")

plotStockSentiment(twits)
