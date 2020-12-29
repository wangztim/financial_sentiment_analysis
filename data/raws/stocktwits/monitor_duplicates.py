import pandas as pd
import os
import re
from glob import glob

WORKING_DIR = '../data/'  #
ticker_dir = WORKING_DIR + 'stocktwits'
PATH = ticker_dir
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