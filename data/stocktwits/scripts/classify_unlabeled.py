import os
import sys
from glob import glob
import sqlite3 as sql

CURR_DIR = os.path.dirname(os.path.abspath(__file__))
PARENT_DIR = os.path.dirname(CURR_DIR)
ROOT_DIR = os.path.dirname((os.path.dirname(PARENT_DIR)))

sys.path.append(ROOT_DIR)

from sentiment_model.sentiment_analyzer import SentimentAnalyzer  # noqa

analyzer = SentimentAnalyzer(
    path=os.path.join(ROOT_DIR, 'sentiment_model/_model/'))

PATH = PARENT_DIR
EXT = "*.db"

all_files = [
    file for path, subdir, files in os.walk(PATH)
    for file in glob(os.path.join(path, EXT))
]

count = 0
for path in all_files:
    conn = sql.connect(path)
    curs = conn.cursor()
    command = ("select * from messages WHERE sentiment IS NULL")
    res = curs.execute(command).fetchall()
    for i in res:
        row_id = i[0]
        row_body = i[1]
        sentiment = analyzer.classify(row_body)
        sentiment_num = int(sentiment)
        curs.execute("UPDATE messages SET sentiment=? WHERE id=-?;",
                     (sentiment_num, row_id))
        conn.commit()
    conn.close()
