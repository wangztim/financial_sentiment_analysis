import urllib.request

url = "https://www.sec.gov/include/ticker.txt"
tickers = set()
file = urllib.request.urlopen(url).read().decode('utf-8')

tickers = set()
for line in file.splitlines():
    t = line.split("\t")[0]
    tickers.add(t)
