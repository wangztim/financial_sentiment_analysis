import sqlite3 as sql
import pandas as pd
import plotly.express as px


def plotTickerSentiment(ticker):
    db_path = f'data/raws/stocktwits/tickers/{ticker}/twits.db'
    print(db_path)
    conn = sql.connect(db_path)
    cmd = """SELECT AVG(sentiment), strftime('%m/%d %H','created_at'), COUNT(*)
                      FROM messages
                      WHERE sentiment != -69
                      GROUP BY strftime('%Y/%m/%d %H','created_at')"""
    df = pd.read_sql(cmd, conn, parse_dates=["created_at"])
    print(df)
    df = df.rename(
        columns={
            "AVG(sentiment)": "sentiment",
            "strftime('%m/%d %H','created_at')": "created_at",
            "COUNT(*)": "count"
        })

    fig = px.line(df, x="created_at", y="sentiment")
    fig.update_traces(mode="markers+lines")
    fig.show()


plotTickerSentiment("AAPL")
