import os
from glob import glob
import sqlite3 as sql

CURR_DIR = os.path.dirname(os.path.abspath(__file__))
PARENT_DIR = os.path.dirname(os.path.abspath(CURR_DIR))

PATH = PARENT_DIR
EXT = "*.db"

all_files = [
    file for path, subdir, files in os.walk(PATH)
    for file in glob(os.path.join(path, EXT))
]

count = 0
for path in all_files:
    conn = sql.connect(path)
    cursor = conn.cursor()
    cursor.execute("""CREATE table IF NOT EXISTS messages_new (
            id text PRIMARY KEY,
            body text NOT NULL,
            author text NOT NULL,
            created_at timestamp NOT NULL,
            sentiment,
            source NOT NULL,
            likes DEFAULT 0,
            replies DEFAULT 0
        );""")
    cursor.execute("""CREATE INDEX IF NOT EXISTS idx1
    ON messages_new (created_at, sentiment, source);""")
    cursor.execute("INSERT INTO messages_new SELECT * FROM messages;")
    cursor.execute("DROP TABLE IF EXISTS messages_backup")
    cursor.execute("ALTER TABLE messages RENAME to messages_backup")
    cursor.execute("ALTER TABLE messages_new RENAME to messages")
    before = cursor.execute(
        "SELECT * from messages WHERE sentiment=-69").fetchall()
    cursor.execute("UPDATE messages SET sentiment=NULL WHERE sentiment=-69;")
    after = cursor.execute(
        "SELECT * from messages WHERE sentiment IS NULL").fetchall()
    if len(before) == len(after):
        count += 1
    conn.commit()

print(count, len(all_files))
