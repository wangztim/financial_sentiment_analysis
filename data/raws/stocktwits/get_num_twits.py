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
    command = ("select count(*) from messages GROUP BY sentiment HAVING sentiment != -69")
    res = curs.execute(command).fetchall()
    for i in res:
        count += i[0]
    
print(count)