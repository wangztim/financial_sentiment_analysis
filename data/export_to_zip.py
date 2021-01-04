import os
import pandas as pd
import sqlite3 as sql
from glob import glob
import zipfile

PATH = os.path.dirname(os.path.abspath(__file__))

all_csv_files = [
    file for path, subdir, files in os.walk(PATH)
    for file in glob(os.path.join(path, "*.csv"))
]


def parse_csv(file):
    return pd.read_csv(file, parse_dates=['created_at'], index_col="id")


csv_messages = pd.concat((parse_csv(f) for f in all_csv_files))

all_db_files = [
    file for path, subdir, files in os.walk(PATH)
    for file in glob(os.path.join(path, "*.db"))
]


def parse_sql(path):
    conn = sql.connect(path)
    return pd.read_sql("SELECT * from messages",
                       conn,
                       index_col='id',
                       parse_dates=['created_at'])


sql_messages = pd.concat((parse_sql(f) for f in all_db_files))
all_messages = pd.concat((sql_messages, csv_messages))

all_messages.index = all_messages.index.astype(str)
all_messages['body'] = all_messages['body'].astype(str)

all_messages.to_parquet("all_messages.parquet")
zipfile.ZipFile('data.zip', mode='w').write("all_messages.parquet")
