import yfinance as yf
import pandas as pd

df = yf.Ticker("SPY").history(period="5d", interval="1m", auto_adjust=False)

if df.index.tz is None:
    df.index = df.index.tz_localize("UTC")
df.index = df.index.tz_convert("America/New_York")

first_15_high = (
    df.between_time("09:30", "09:44")
      .groupby(df.index.date)["High"]
      .max()
)

print(first_15_high)


high = otday highest price in the first 15 minutes 

if price rises above high again and has stayed there for atleast 5 minutes or 0.05 percent above the range high and above VWAP , buy atm call 

1 trade per day 

stop loss - 0.10

take profit 0.20