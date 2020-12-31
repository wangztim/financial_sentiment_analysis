import sqlite3 as sql
import numpy as np
import seaborn as sns


def plotTickerSentiment(ticker):
    db_path = f'data/raws/stocktwits/tickers/{ticker}/twits.db'
    print(db_path)
    conn = sql.connect(db_path)
    cursor = conn.cursor()
    cmd = cursor.execute("""SELECT AVG(sentiment), DATE(created_at)
                      FROM messages
                      WHERE sentiment != -69
                      GROUP BY DATE(created_at)""")
    for res in cmd.fetchall():
        print(res)


plotTickerSentiment("AAPL")