import sqlite3 as sql
import pandas as pd
import plotly.express as px


def plotTickerSentiment(ticker):
    db_path = f'data/raws/stocktwits/tickers/{ticker}/twits.db'
    print(db_path)
    conn = sql.connect(db_path)
    cmd = """SELECT AVG(sentiment), DATE(created_at), COUNT(*)
                      FROM messages
                      WHERE sentiment != -69
                      GROUP BY DATE(created_at)"""
    df = pd.read_sql(cmd, conn, parse_dates=["created_at"])
    df = df.rename(
        mapper={
            "AVG(sentiment)": "sentiment",
            "DATE(created_at)": "created_at",
            "COUNT(*)": "count"
        })

    px.line(df, x="created_at", y="sentiment")


plotTickerSentiment("AAPL")
