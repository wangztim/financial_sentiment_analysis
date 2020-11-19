import csv
import os
import random
import json

from copy import deepcopy
from datetime import datetime
from dateutil import parser
from subprocess import call

from classes.fetchers import StocktwitsFetcher, Direction
from classes.message import Message
from aiohttp import ClientSession
import asyncio
from typing import Tuple, List

tickers_file = open(os.path.dirname(
    os.path.abspath(__file__)) + "/igm.txt", "r")
tickers = tickers_file.read().splitlines()
tickers_file.close()

tickers_folder = os.path.dirname(
    os.path.abspath(__file__)) + "/tickers/"

desired_dir = Direction.BACKWARD
fields = ['id', 'body', 'created_at', 'sentiment', 'symbols']


def initTickerTwitsCSV(ticker):
    ticker_dir = tickers_folder + ticker
    if not os.path.exists(ticker_dir):
        os.makedirs(ticker_dir)
    csv_path = ticker_dir + '/twits.csv'
    twits_csv = open(csv_path, 'a+', encoding='utf-8', errors='ignore')
    file_empty = os.path.getsize(csv_path) == 0
    if file_empty:
        writer = csv.DictWriter(
            twits_csv, delimiter=',', lineterminator='\n', fieldnames=fields)
        writer.writeheader()
    return twits_csv


def findStartingId(direction, ticker) -> (datetime, str):
    m_dt, m_id = None, None
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

                if m_dt is None and m_id is None:
                    m_dt, m_id = row_datetime, row_id
                elif direction == Direction.FORWARD and row_datetime > m_dt:
                    m_dt, m_id = row_datetime, row_id
                elif direction == Direction.BACKWARD and row_datetime < m_dt:
                    m_dt, m_id = row_datetime, row_id
        except Exception as er:
            print(er)
            print(f'the row after {row} in {twits_csv.name} is bad')
            return None

        return m_dt, m_id


def updateTickerMarkers(ticker, markers):
    ticker_dir = tickers_folder + ticker
    json_path = ticker_dir + '/markers.json'
    with open(json_path, 'w', encoding='utf-8', errors='ignore') as m_json:
        out = deepcopy(markers)
        out['newest']['datetime'] = out['newest']['datetime'].timestamp()
        out['oldest']['datetime'] = out['oldest']['datetime'].timestamp()
        json.dump(out, m_json)
        m_json.close()


def initTickerMarkers(ticker):
    ticker_dir = tickers_folder + ticker
    if not os.path.exists(ticker_dir):
        os.makedirs(ticker_dir)
    json_path = ticker_dir + '/markers.json'
    file_exists = os.path.exists(json_path)
    if not file_exists:
        open(json_path, 'w', encoding='utf-8', errors='ignore').close()
    file_empty = os.path.getsize(json_path) == 0

    markers = None

    if file_empty:
        with open(json_path, 'w', encoding='utf-8', errors='ignore') as _:
            newest_dt, newest_id = findStartingId(
                Direction.FORWARD, ticker)
            oldest_dt, oldest_id = findStartingId(
                Direction.BACKWARD, ticker)
            now = datetime.today()
            markers = {
                "newest": {
                    "datetime": newest_dt if newest_dt else now,
                    "id": newest_id if newest_id else 0
                },
                "oldest": {
                    "datetime": oldest_dt if oldest_dt else now,
                    "id": oldest_id if oldest_id else 0
                }
            }
            updateTickerMarkers(ticker, markers)
    else:
        with open(json_path, 'r', encoding='utf-8', errors='ignore') as r:
            markers = json.load(r)
            newest_dt = datetime.fromtimestamp(markers['newest']['datetime'])
            oldest_dt = datetime.fromtimestamp(markers['oldest']['datetime'])
            markers['newest']['datetime'] = newest_dt
            markers['oldest']['datetime'] = oldest_dt

    return markers


async def main():
    fetcher = StocktwitsFetcher(desired_dir)
    NUM_TICKERS_TO_GET = 160

    print("initializing markers")
    all_csvs = [initTickerTwitsCSV(ticker) for ticker in tickers]
    all_indices = range(0, len(tickers))

    for ticker in tickers:
        markers = initTickerMarkers(ticker)
        fetcher.setTickerMarkers(ticker, markers)

    sudo_pw = input("Please enter your sudo password: ")

    while True:
        print("let's go")
        target_indices = random.sample(all_indices, NUM_TICKERS_TO_GET)
        target_csvs = [all_csvs[i] for i in target_indices]
        target_tickers = [tickers[i] for i in target_indices]

        async with ClientSession() as session:
            futures = [fetcher.fetch(tickers[i], session)
                       for i in target_indices]

            # type: Tuple[Message]
            responses: Tuple[List[Message], ...] = await asyncio.gather(
                *futures, return_exceptions=True)

        print("fetched responses")

        successes = 0

        for i in range(len(responses)):
            messages = responses[i]
            csv_io = target_csvs[i]
            ticker = target_tickers[i]
            if not isinstance(messages, list):
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
            ticker_markers = fetcher.getTickerMarkers(ticker)
            updateTickerMarkers(ticker, ticker_markers)
            successes += 1

        print(f"{successes} / {NUM_TICKERS_TO_GET} Succeses!")

        print('restarting VPN process.')
        cmd1 = "protonvpn d"
        cmd2 = "protonvpn c -r"
        call('echo {} | sudo -S {}'.format(sudo_pw, cmd1), shell=True)
        call('echo {} | sudo -S {}'.format(sudo_pw, cmd2), shell=True)

if __name__ == "__main__":
    asyncio.run(main())
