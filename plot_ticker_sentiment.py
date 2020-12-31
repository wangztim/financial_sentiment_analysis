import sqlite3 as sql
import pandas as pd
import matplotlib.pyplot as plt


def plotTickerSentiment(ticker):
    db_path = f'data/raws/stocktwits/tickers/{ticker}/twits.db'
    print(db_path)
    conn = sql.connect(db_path)
    cmd = """SELECT AVG(sentiment), DATE(created_at), COUNT(*)
                      FROM messages
                      WHERE sentiment != -69
                      GROUP BY DATE(created_at)"""
    df = pd.read_sql(cmd, conn, parse_dates=["created_at"])
    df.plot(x="DATE(created_at)", y="AVG(sentiment)")
    plt.show()


plotTickerSentiment("AAPL")
