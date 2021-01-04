import urllib.request
urls = [
    "ftp://ftp.nasdaqtrader.com/symboldirectory/nasdaqlisted.txt",
    "ftp://ftp.nasdaqtrader.com/symboldirectory/otherlisted.txt"
]

tickers = set()
for url in urls:
    file = urllib.request.urlopen(url)
    res = file.read().decode('utf-8')

    line_num = 0  # Symbol|Security Name|Market Category|Test Issue|...
    row_labels = []
    for line in res.splitlines():
        line = line.split("|")
        if line_num == 0:
            row_labels = line
            line_num += 1
        else:
            symbol_idx = row_labels.index(
                "Symbol") if "Symbol" in row_labels else row_labels.index(
                    "ACT Symbol")
            tickers.add(line[symbol_idx])
