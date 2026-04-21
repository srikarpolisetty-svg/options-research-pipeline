from datetime import timedelta

import _path_setup  # noqa: F401
import pandas as pd
import yfinance as yf

from databentodatabasebackfillworkingversion import db_end_utc_day, get_sp500_symbols


symbols = [s.strip().upper() for s in get_sp500_symbols() if s and isinstance(s, str)]
days = 35

end = db_end_utc_day()
start = end - timedelta(days=days)
symbol_groups = [
    symbols[i:i + 40]
    for i in range(0, len(symbols), 40)
]
underlying_data = {}

for symbol_group in symbol_groups:
    df = yf.download(symbol_group, start=start, end=end, interval="1d", progress=False,auto_adjust=False)
    if df is None or df.empty:
        underlying_data = None
        break

    for symbol in symbol_group:
        underlying_data.setdefault(symbol, [])
        for idx, row in df.iterrows():
            price = row[("Open", symbol)]
            if pd.notna(price):
                underlying_data[symbol].append({
                "timestamp": idx,
                "underlying_price": float(price),
                })

for symbol in list(underlying_data):
    if underlying_data[symbol] is None:
        del underlying_data[symbol]



print(underlying_data)
print(len(underlying_data))
