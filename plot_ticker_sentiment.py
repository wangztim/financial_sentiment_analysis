import sqlite3 as sql
import pandas as pd
import plotly.express as px

# Hourly Query
"""SELECT AVG(sentiment), strftime('%m/%d %H',created_at), COUNT(*)
                      FROM messages
                      WHERE sentiment != -69 AND created_at
                      BETWEEN '2020-12-27' and '2020-12-30'
                      GROUP BY strftime('%Y/%m/%d %H',created_at)"""

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
        columns={
            "AVG(sentiment)": "sentiment",
            "DATE(created_at)": "created_at",
            "COUNT(*)": "count"
        })

    fig = px.line(df, x="created_at", y="sentiment")
    fig.update_traces(mode="markers+lines")
    fig.show()


plotTickerSentiment("AMZN")
