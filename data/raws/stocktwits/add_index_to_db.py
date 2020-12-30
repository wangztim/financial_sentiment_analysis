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

count = 0
for path in all_files:
    conn = sql.connect(path)
    curs = conn.cursor()
    command = ("CREATE INDEX idx1 ON messages (created_at, sentiment)")
    curs.execute(command)
    check_command = ("PRAGMA INDEX_LIST(messages)")
    out = curs.execute(check_command).fetchall()
    for row in out:
        if "idx1" in row:
            count += 1
            break

if count == len(all_files):
    print("all good chief!")