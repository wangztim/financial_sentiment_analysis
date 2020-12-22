import requests
import time
import csv
import json
import os
import uuid
from datetime import date

curr_dir = os.path.dirname(os.path.abspath(__file__))

with open(curr_dir + '/posts.json', 'r') as headlines, open(curr_dir + '/posts.csv', 'a+') as headlines_csv:
    data = json.load(headlines)
    row_headers = ['id', 'body', 'created_at', 'sentiment', 'symbols']
    writer = csv.DictWriter(
        headlines_csv, delimiter=',', lineterminator='\n', fieldnames=row_headers)
    writer.writeheader()
    today = date.today()
    for idx in data:
        headline = data[idx]
        symbols = []

        total_sentiment = 0
        for e in headline['info']:
            sentiment_score = e['sentiment_score']
            total_sentiment += float(sentiment_score)
            symbol = e['target']
            if symbol not in symbols:
                symbols.append(symbol)

        avg_sentiment = total_sentiment / len(headline['info'])
        breaking_point = 0.11
        if (abs(avg_sentiment) < breaking_point):
            avg_sentiment = 0
        elif avg_sentiment >= breaking_point:
            avg_sentiment = 1
        else:
            avg_sentiment = -1
        row = {'id': uuid.uuid4(), 'sentiment': avg_sentiment,
               'body': headline['sentence'].lower(), 'created_at': today, "symbols": symbols}
        writer.writerow(row)
