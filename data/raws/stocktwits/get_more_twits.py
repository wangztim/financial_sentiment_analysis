import os
import random
import sqlite3 as sql

from subprocess import call

from models.fetchers import StocktwitsFetcher, Direction
from models.message import Message, to_tuple
from aiohttp import ClientSession
import asyncio
from typing import Tuple, List, Dict
from collections import defaultdict

CURR_DIR = os.path.dirname(os.path.abspath(__file__))
tickers_file = open(CURR_DIR + "/tickers.txt", "r")
tickers = tickers_file.read().splitlines()
tickers_file.close()
tickers_folder = CURR_DIR + "/tickers/"

desired_dir = Direction.BACKWARD

columns = ('id', 'body', "author", 'created_at', 'sentiment', "source",
           "likes", "replies")


class NoEntriesInserted(Exception):
    def __str__(self):
        return "No entries were inserted into the table!"


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
            sentiment,
            source NOT NULL,
            likes DEFAULT 0,
            replies DEFAULT 0
        );""")
        cursor.execute("""CREATE INDEX IF NOT EXISTS idx1
        ON messages (created_at, sentiment, source);""")

        self.connections[ticker] = conn

    def insertMessages(self, ticker, messages):
        conn = self.connections[ticker]
        messages = [to_tuple(m) for m in messages]
        prev = conn.total_changes
        conn.cursor().executemany(
            "INSERT OR IGNORE INTO messages VALUES (?, ?, ?, ?, ?, ?, ?, ?);",
            messages)
        conn.commit()
        curr = conn.total_changes
        if curr - prev == 0:
            print(ticker + " wrote nothing :(")
            raise NoEntriesInserted()

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
    except Exception as er:
        return str(er)


def restartVPN(sudo_pw):
    print('restarting VPN')
    cmd = "protonvpn c -r"
    call('echo {} | sudo -S {}'.format(sudo_pw, cmd), shell=True)


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

        successes = 0
        errors_dict = defaultdict(int)

        for res in status:
            if res == 1:
                successes += res
            else:
                errors_dict[res] += 1

        print(f"{successes} / {NUM_TICKERS_TO_GET} Succeses.")
        if successes < NUM_TICKERS_TO_GET:
            print("Errors:", errors_dict)

        loop = asyncio.get_running_loop()
        await loop.run_in_executor(None, restartVPN, sudo_pw)


if __name__ == "__main__":
    asyncio.run(main())
