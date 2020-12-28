import requests

url = "https://www.sec.gov/include/ticker.txt"
download = requests.get(url, allow_redirects=True)
decoded_content = download.content.decode('utf-8')
tickers = set()
for line in decoded_content.splitlines():
    t = line.split("\t")[0]
    tickers.add(t)

print(len(tickers))
