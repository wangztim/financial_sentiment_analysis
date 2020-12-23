import os
import pandas as pd
import re
from glob import glob
import zipfile

PATH = os.path.dirname(os.path.abspath(__file__))
EXT = "*.csv"

all_csv_files = [
    file for path, subdir, files in os.walk(PATH)
    for file in glob(os.path.join(path, EXT))
]


def parse_csv(file):
    return pd.read_csv(file, parse_dates=['created_at'])


messages = pd.concat((parse_csv(f) for f in all_csv_files),
                     ignore_index=True,
                     sort=False)

messages.set_index('id', inplace=True)
messages.index = messages.index.map(str)
messages = messages[~messages.index.duplicated(keep='first')]


def filter_urls(text):
    return re.sub(r"http\S+", "", str(text))


messages['body'] = messages['body'].apply(filter_urls)

messages["sentiment"] = messages["sentiment"].replace({-1: 0})
messages.to_parquet("all_messages.parquet")

zipfile.ZipFile('data.zip', mode='w').write("all_messages.parquet")
