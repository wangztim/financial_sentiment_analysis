import csv
import os
import random

from datetime import datetime
from dateutil import parser
from subprocess import call

from classes.fetchers import StocktwitsFetcher, Direction
from classes.message import Message
from aiohttp import ClientSession
import asyncio
from typing import Tuple, List

sp_500_tickers = open(os.path.dirname(
    os.path.abspath(__file__)) + "/igm.txt", "r").read().splitlines()


desired_dir = Direction.BACKWARD
fields = ['id', 'body', 'created_at', 'sentiment', 'symbols']


def initializeCSV(ticker):
    ticker_dir = os.path.dirname(
        os.path.abspath(__file__)) + "/tickers/" + ticker
    if not os.path.exists(ticker_dir):
        os.makedirs(ticker_dir)
    file_path = ticker_dir + '/twits.csv'
    twits_csv = open(file_path, 'a+', encoding='utf-8', errors='ignore')
    file_empty = os.path.getsize(file_path) == 0
    writer = csv.DictWriter(
        twits_csv, delimiter=',', lineterminator='\n', fieldnames=fields)
    if file_empty:
        writer.writeheader()
    return twits_csv


def findStartingId(direction, ticker) -> (datetime, str):
    marker_datetime, marker_id = None, None
    ticker_dir = os.path.dirname(
        os.path.abspath(__file__)) + "/tickers/" + ticker
    if not os.path.exists(ticker_dir):
        os.makedirs(ticker_dir)
    file_path = ticker_dir + '/twits.csv'
    with open(file_path, 'a+', encoding='utf-8', errors='ignore') as twits_csv:
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
                elif direction == Direction.FORWARD and row_datetime > marker_datetime:
                    marker_datetime, marker_id = row_datetime, row_id
                elif direction == Direction.BACKWARD and row_datetime < marker_datetime:
                    marker_datetime, marker_id = row_datetime, row_id
        except Exception as er:
            print(er)
            print(f'the row after {row} in {twits_csv.name} is bad')
            return None

        return marker_datetime, marker_id


sudo_pw = input("Please enter your sudo password: ")


async def main():
    fetcher = StocktwitsFetcher(desired_dir)
    NUM_TICKERS_TO_GET = 180

    print("initializing markers")

    [initializeCSV(ticker) for ticker in sp_500_tickers]

    for ticker in sp_500_tickers:
        marker_datetime, marker_id = findStartingId(desired_dir, ticker)
        fetcher.setTickerMarker(ticker, marker_id, marker_datetime)

    while True:
        print("let's go")
        stock_tickers = random.sample(sp_500_tickers, NUM_TICKERS_TO_GET)
        csvs = [initializeCSV(ticker) for ticker in stock_tickers]

        async with ClientSession() as session:
            futures = []
            for ticker in stock_tickers:
                future = asyncio.ensure_future(
                    fetcher.fetchMessages(ticker, session))
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

            print(f"{successes} / {NUM_TICKERS_TO_GET} Succeses!")

            print('restarting VPN process.')
            cmd1 = "protonvpn d"
            cmd2 = "protonvpn c -r"
            call('echo {} | sudo -S {}'.format(sudo_pw, cmd1), shell=True)
            call('echo {} | sudo -S {}'.format(sudo_pw, cmd2), shell=True)

if __name__ == "__main__":
    asyncio.run(main())
