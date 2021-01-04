import csv
import json
import os
import uuid

from models.message import Sentiment, Message, to_dict
from datetime import date

curr_dir = os.path.dirname(os.path.abspath(__file__))
row_headers = [
    'id', 'body', 'author', 'created_at', 'source', 'sentiment', 'likes',
    'replies'
]

with open(curr_dir + '/posts.json',
          'r') as headlines, open(curr_dir + '/posts.csv',
                                  'a+') as headlines_csv:
    data = json.load(headlines)
    writer = csv.DictWriter(headlines_csv,
                            delimiter=',',
                            lineterminator='\n',
                            fieldnames=row_headers)
    writer.writeheader()
    today = date.today()
    for idx in data:
        headline = data[idx]

        total_sentiment = 0
        for e in headline['info']:
            sentiment_score = e['sentiment_score']
            total_sentiment += float(sentiment_score)

        avg_sentiment = total_sentiment / len(headline['info'])
        breaking_point = 0.05
        if (abs(avg_sentiment) < breaking_point):
            avg_sentiment = Sentiment.UNCERTAIN
        elif avg_sentiment >= breaking_point:
            avg_sentiment = Sentiment.BULLISH
        else:
            avg_sentiment = Sentiment.BEARISH

        message = Message(uuid.uuid4(), headline['sentence'], uuid.uuid4(),
                          today, "FIQA", avg_sentiment)

        writer.writerow(to_dict(message))
