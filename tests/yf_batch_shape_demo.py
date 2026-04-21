from datetime import timedelta

import _path_setup  # noqa: F401
import pandas as pd
import yfinance as yf

days = 5
symbols = ["AAPL", "MSFT", "SPY"]

end = pd.Timestamp.utcnow().normalize().tz_localize(None)
start = end - timedelta(days=days)

df = yf.download(symbols, start=start, end=end, interval="1d", progress=False)

for idx, row in df.iterrows():
    print(idx, row[("Volume", "MSFT")])






