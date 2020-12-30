import os
import random
import sqlite3 as sql

from subprocess import call

from classes.fetchers import StocktwitsFetcher, Direction
from classes.message import Message, to_tuple
from aiohttp import ClientSession
import asyncio
from typing import Tuple, List, Dict
import requests

CURR_DIR = os.path.dirname(os.path.abspath(__file__))
tickers_file = open(CURR_DIR + "/tickers.txt", "r")
tickers = tickers_file.read().splitlines()
tickers_file.close()
tickers_folder = CURR_DIR + "/tickers/"

desired_dir = Direction.BACKWARD

columns = ('id', 'body', "author", 'created_at', 'sentiment', "source",
           "likes", "replies")


class TickerDBManager:
    connections: Dict[str, sql.Connection] = {}

    def initTickerDB(self, ticker):
        ticker_dir = tickers_folder + ticker
        if not os.path.exists(ticker_dir):
            os.makedirs(ticker_dir)
        db_path = ticker_dir + '/twits.db'
        conn = sql.connect(db_path, check_same_thread=False)
        cursor = conn.cursor()

        cursor.execute("""CREATE table IF NOT EXISTS messages (
            id text PRIMARY KEY,
            body text NOT NULL,
            author text NOT NULL,
            created_at timestamp NOT NULL,
            sentiment DEFAULT -69,
            source NOT NULL,
            likes DEFAULT 0,
            replies DEFAULT 0
        );""")
        self.connections[ticker] = conn

    def insertMessages(self, ticker, messages):
        conn = self.connections[ticker]
        messages = [to_tuple(m) for m in messages]
        conn.cursor().executemany(
            "INSERT OR IGNORE INTO messages VALUES (?, ?, ?, ?, ?, ?, ?, ?);",
            messages)
        conn.commit()

    def getTickerMarkers(self, ticker):
        conn = self.connections[ticker]
        cursor = conn.cursor()
        count = cursor.execute("SELECT count(*) FROM messages").fetchone()[0]
        if count <= 1:
            return None

        oldest_row = cursor.execute(
            "SELECT * FROM messages ORDER BY created_at ASC LIMIT 1;"
        ).fetchone()
        newest_row = cursor.execute(
            "SELECT * FROM messages ORDER BY created_at DESC LIMIT 1;"
        ).fetchone()

        id_idx = columns.index("id")
        created_at_idx = columns.index("created_at")

        markers = {}
        if oldest_row is not None:
            markers["oldest"] = {
                "id": oldest_row[id_idx],
                "datetime": oldest_row[created_at_idx]
            }

        if newest_row is not None:
            markers["newest"] = {
                "id": newest_row[id_idx],
                "datetime": newest_row[created_at_idx]
            }
        return markers


async def fetchAndStoreMessages(ticker, fetcher: StocktwitsFetcher,
                                manager: TickerDBManager, session):
    try:
        messages: List[Message] = await fetcher.fetch(ticker, session)
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(None, manager.insertMessages, ticker,
                                   messages)
        return 1
    except sql.OperationalError as er:
        print(er)
        return 0
    except requests.exceptions.ConnectionError:
        return 0
    except Exception as er2:
        print(er2)
        return 0


def restartVPN(sudo_pw):
    print('restarting VPN')
    cmd1 = "protonvpn d"
    cmd2 = "protonvpn c -r"
    call('echo {} | sudo -S {}'.format(sudo_pw, cmd1), shell=True)
    call('echo {} | sudo -S {}'.format(sudo_pw, cmd2), shell=True)


async def main():
    fetcher = StocktwitsFetcher(desired_dir)
    manager = TickerDBManager()

    all_indices = range(0, len(tickers))

    for ticker in tickers:
        manager.initTickerDB(ticker)

    NUM_TICKERS_TO_GET = min(200, len(tickers))

    print("initializing markers")
    for ticker in tickers:
        markers = manager.getTickerMarkers(ticker)
        if markers:
            fetcher.setTickerMarkers(ticker, markers)
    sudo_pw = input("Please enter your sudo password: ")

    while True:
        print("fetching batch")
        target_indices = random.sample(all_indices, NUM_TICKERS_TO_GET)

        async with ClientSession() as session:
            futures = [
                fetchAndStoreMessages(tickers[i], fetcher, manager, session)
                for i in target_indices
            ]

            status: Tuple[int] = await asyncio.gather(*futures,
                                                      return_exceptions=True)

        successes = sum(status)
        print(f"{successes} / {NUM_TICKERS_TO_GET} Succeses!")

        loop = asyncio.get_running_loop()
        await loop.run_in_executor(None, restartVPN, sudo_pw)


if __name__ == "__main__":
    asyncio.run(main())
