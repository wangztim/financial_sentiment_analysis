import time
import csv
import os
import random

from dateutil import parser
from subprocess import call
from classes.fetchers import StocktwitsFetcher, Direction
from classes.message import Message
from aiohttp import ClientSession
import asyncio
from typing import Tuple, List

sudo_pw = input("Please enter your sudo password: ")

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


def findStartingId(direction, ticker) -> str:
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

        return marker_id


async def main():
    fetcher = StocktwitsFetcher()

    for ticker in sp_500_tickers:
        marker_id = findStartingId(desired_dir, ticker)
        fetcher.setTickerMarker(ticker, marker_id)

    while True:
        print("let's go")
        stock_tickers = random.sample(sp_500_tickers, 200)
        csvs = [initializeCSV(ticker) for ticker in stock_tickers]

        async with ClientSession() as session:
            futures = []
            for i in range(len(stock_tickers)):
                ticker = stock_tickers[i]
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

            print(f"{successes} / 200 Succeses!")

            print('restarting VPN process.')

            cmd = "protonvpn connect -r"
            call('echo {} | sudo -S {}'.format(sudo_pw, cmd), shell=True)
            print('waiting for VPN to reboot.')
            time.sleep(2)

if __name__ == "__main__":
    asyncio.run(main())
