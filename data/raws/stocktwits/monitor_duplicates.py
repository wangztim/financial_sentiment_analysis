import pandas as pd
import os
from glob import glob
import sqlite3 as sql

PATH = os.path.dirname(os.path.abspath(__file__))
EXT = "*.db"

all_files = [
    file for path, subdir, files in os.walk(PATH)
    for file in glob(os.path.join(path, EXT))
]

parse_sql = lambda conn: pd.read_sql(
    "SELECT * from messages", conn, parse_dates=['created_at'], index_col="id")

for path in all_files:
    conn = sql.connect(path)
    m = parse_sql(conn)
    non_dupes = m.drop_duplicates()
    print(path, len(m) - len(non_dupes))
