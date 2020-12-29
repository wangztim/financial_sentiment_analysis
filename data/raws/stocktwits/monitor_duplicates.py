import pandas as pd
import os
from glob import glob

PATH = os.path.dirname(os.path.abspath(__file__))
EXT = "*.csv"

all_csv_files = [
    file for path, subdir, files in os.walk(PATH)
    for file in glob(os.path.join(path, EXT))
]

parse_csv = lambda file: pd.read_csv(file, parse_dates=['created_at'])

messages = ((f, parse_csv(f)) for f in all_csv_files)

for path, m in messages:
    non_dupes = m.drop_duplicates()
    print(path, len(m) - len(non_dupes))