import pandas as pd
import os
from glob import glob
import sqlite3 as sql

PATH = os.path.dirname(os.path.abspath(__file__))
EXT = "*.csv"

all_csv_files = [
    file for path, subdir, files in os.walk(PATH)
    for file in glob(os.path.join(path, EXT))
]

parse_csv = lambda file: pd.read_csv(
    file, parse_dates=['created_at'], index_col="id")

messages = ((f, parse_csv(f)) for f in all_csv_files)

for path, m in messages:
    m.index = m.index.astype(str)
    m["author"] = m["author"].astype(str)
    path = path.replace("twits.csv", "twits.db")
    conn = sql.connect(path)
    m.to_sql('messages', conn, if_exists="replace")
