import requests
import time
import csv
import os
import pprint
from datetime import datetime
from dateutil import parser
import random
import proxyscrape

sp_500_tickers = open(os.path.dirname(
    os.path.abspath(__file__)) + "/igm.txt", "r").read().splitlines()


FORWARD = "FORWARD"
BACKWARD = "BACKWARD"

desired_dir = BACKWARD


def fetchAndWriteTwits(ticker, proxy=None):
    endpoint = "https://api.stocktwits.com/api/2/streams/symbol/" + ticker + ".json"
    ticker_dir = os.path.dirname(
        os.path.abspath(__file__)) + "/tickers/" + ticker

    headers = ['id', 'body', 'created_at', 'sentiment', 'symbols']

    if not os.path.exists(ticker_dir):
        os.makedirs(ticker_dir)

    file_path = ticker_dir + '/twits.csv'

    with open(file_path, 'a+', encoding='utf-8', errors='ignore') as twits_csv:
        writer = csv.DictWriter(
            twits_csv, delimiter=',', lineterminator='\n', fieldnames=headers)

        file_empty = os.path.getsize(file_path) == 0
        marker_datetime, marker_id = None, None

        if file_empty:
            writer.writeheader()
        else:
            marker_datetime, marker_id = initializeMarkerDatetimeAndId(
                desired_dir, twits_csv)

        twits = fetchTwits(endpoint, desired_dir, marker_id, proxy)

        for twit in twits:
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
                print(sentiment)
                sentiment_val = 0

            created_date_time = datetime.strptime(
                twit['created_at'], "%Y-%m-%dT%H:%M:%SZ")

            body = twit['body'].lower()

            all_symbols = list(
                map(lambda x: x['symbol'].lower(), twit['symbols']))

            if (marker_datetime == None) or (desired_dir == BACKWARD and created_date_time < marker_datetime) or (desired_dir == FORWARD and created_date_time > marker_datetime):
                marker_datetime = created_date_time
                marker_id = twit['id']

            tickers_in_body = list(filter(
                lambda symbol: symbol in body, all_symbols))

            row = {'id': twit['id'], 'sentiment': sentiment_val,
                   'body': body, 'created_at': created_date_time, 'symbols': tickers_in_body}

            writer.writerow(row)


def initializeMarkerDatetimeAndId(direction, twits_csv):
    marker_datetime, marker_id = None, None

    twits_csv.seek(0)
    reader = csv.reader(twits_csv, delimiter=',',
                        lineterminator='\n')

    headers = next(reader)

    id_idx = headers.index("id")
    created_at_idx = headers.index('created_at')

    try:
        for row in reader:
            row_id = row[id_idx]
            row_datetime = parser.parse(
                row[created_at_idx])

            if marker_datetime == None and marker_id == None:
                marker_datetime, marker_id = row_datetime, row_id
            elif direction == FORWARD and row_datetime > marker_datetime:
                marker_datetime, marker_id = row_datetime, row_id
            elif direction == BACKWARD and row_datetime < marker_datetime:
                marker_datetime, marker_id = row_datetime, row_id

    except:
        raise RuntimeError("the row after " + row_id)

    return marker_datetime, marker_id


def fetchTwits(endpoint, direction, twit_id, proxy=None):
    params = {}
    if direction == BACKWARD:
        params['max'] = twit_id
    elif direction == FORWARD:
        params['since'] = twit_id

    res = None

    try:
        res = requests.get(endpoint, params=params, timeout=5, proxies=proxy)
    except requests.exceptions.Timeout:
        raise requests.exceptions.HTTPError("Request timed out :(")

    if (res.status_code == 429 or res.status_code == 403):
        raise requests.exceptions.ConnectionError(
            "Rate limit exhausted. Must switch to a proxy.")
    elif (res.status_code != 200):
        raise requests.exceptions.HTTPError("API call unsuccessful. Code: " +
                                            str(res.status_code))
    data = res.json()
    twits = data["messages"]

    if len(twits) == 0:
        raise RuntimeError("All TWITS exhausted.")

    return twits


def getExhaustedStocks():
    exh_path = os.path.dirname(
        os.path.abspath(__file__)) + "/exhausted_stocks.txt"
    with open(exh_path, "r") as f:
        return f.read().splitlines()


def addExhaustedStock(name):
    exh_path = os.path.dirname(
        os.path.abspath(__file__)) + "/exhausted_stocks.txt"
    with open(exh_path, "a") as f:
        return f.write(name + "\n")


collector = proxyscrape.create_collector('free-proxy-list', 'https')

active_proxy = None

while True:
    # exhausted_stocks = getExhaustedStocks()
    # good_tickers = [t for t in sp_500_tickers if t not in exhausted_stocks]
    seq = list(range(0, len(sp_500_tickers)))
    random.shuffle(seq)
    for ticker_idx in seq:
        ticker = sp_500_tickers[ticker_idx]
        try:
            print(ticker)
            fetchAndWriteTwits(ticker, active_proxy)  # Is this legal?
        except requests.exceptions.ConnectionError as er:
            print('switching proxies because ' + str(er))
            proxy = collector.get_proxy()
            proxy_url = f"{proxy.host}:{proxy.port}"
            active_proxy = {
                "http": proxy_url,
                "https": proxy_url
            }
        except requests.exceptions.HTTPError as er:
            print('unable to fetch information because of error: ' + str(er))
            continue
        except RuntimeError as er:
            print("Skipping because " + str(er))
            addExhaustedStock(ticker)
            continue
