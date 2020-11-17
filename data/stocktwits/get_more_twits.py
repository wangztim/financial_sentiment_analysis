import time
import csv
import os
import random
import pathlib

from dateutil import parser
from subprocess import call
from classes.fetchers import StocktwitsFetcher
from classes.message import Message
from aiohttp import ClientSession
import asyncio
from typing import Tuple, List

sp_500_tickers = open(os.path.dirname(
    os.path.abspath(__file__)) + "/igm.txt", "r").read().splitlines()


FORWARD = "FORWARD"
BACKWARD = "BACKWARD"

desired_dir = BACKWARD
fields = ['id', 'body', 'created_at', 'sentiment', 'symbols']


def initializeCSV(ticker):
    ticker_dir = os.path.dirname(
        os.path.abspath(__file__)) + "/tickers/" + ticker
    file_path = ticker_dir + '/twits.csv'
    twits_csv = open(file_path, 'a+', encoding='utf-8', errors='ignore')
    file_empty = os.path.getsize(file_path) == 0
    writer = csv.DictWriter(
        twits_csv, delimiter=',', lineterminator='\n', fieldnames=fields)
    if file_empty:
        writer.writeheader()
    return twits_csv


def findStartingId(direction, twits_csv):
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
                row[created_at_idx], ignoretz=True)

            if marker_datetime is None and marker_id is None:
                marker_datetime, marker_id = row_datetime, row_id
            elif direction == FORWARD and row_datetime > marker_datetime:
                marker_datetime, marker_id = row_datetime, row_id
            elif direction == BACKWARD and row_datetime < marker_datetime:
                marker_datetime, marker_id = row_datetime, row_id
    except Exception as er:
        print(er)
        print(f'the row after {row} in {twits_csv.name} is bad')
        return None

    return marker_id


def buildParams(direction, csv):
    twit_id = findStartingId(direction, csv)
    params = {}
    if twit_id is None:
        return params
    if direction == BACKWARD:
        params['max'] = twit_id
    elif direction == FORWARD:
        params['since'] = twit_id
    return params


async def main():
    vpn = "Hotspot Shield"
    fetcher = StocktwitsFetcher()

    while True:
        print("let's go")
        stock_tickers = random.sample(sp_500_tickers, 200)
        csvs = [initializeCSV(ticker) for ticker in stock_tickers]
        params = [buildParams(desired_dir, csv) for csv in csvs]
        async with ClientSession() as session:
            futures = []
            for i in range(len(stock_tickers)):
                ticker = stock_tickers[i]
                future = asyncio.ensure_future(
                    fetcher.fetchMessages(ticker, session, params[i]))
                futures.append(future)

            # type: Tuple[Message]
            responses: Tuple[List[Message], ...] = await asyncio.gather(
                *futures, return_exceptions=True)

            print("fetched responses")

            successes = 0

            for i in range(len(stock_tickers)):
                messages = responses[i]
                csv_io = csvs[i]
                if not isinstance(messages, list):
                    csv_io.close()
                    continue
                writer = csv.DictWriter(
                    csv_io,
                    delimiter=',',
                    lineterminator='\n',
                    fieldnames=fields)
                for message in messages:
                    row = {'id': message.id,
                           'sentiment': int(message.sentiment),
                           'body': message.body,
                           'created_at': message.created_at,
                           'symbols': message.symbols}
                    writer.writerow(row)
                successes += 1
                csv_io.close()

            print(f"{successes} / 200 Succeses!")

            print('restarting VPN process.')
            call(["killall", vpn])
            time.sleep(2)
            print("starting " + vpn)
            call(["open", "-a", vpn])
            print("waiting some time to let connection get established")
            time.sleep(16)


if __name__ == "__main__":
    asyncio.run(main())
