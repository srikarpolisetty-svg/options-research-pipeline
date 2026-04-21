import sys
from datetime import timedelta

import _path_setup  # noqa: F401
import pandas as pd
import yfinance as yf

from databentodatabasebackfillworkingversion import db_end_utc_day


def print_open_summary(df: pd.DataFrame, symbols: list[str], label: str) -> None:
    print(f"\n=== {label} ===")

    if df is None or df.empty:
        print("download returned empty dataframe")
        return

    print(f"shape: {df.shape}")
    print(f"columns type: {type(df.columns).__name__}")

    multi_cols = isinstance(df.columns, pd.MultiIndex)
    for symbol in symbols:
        if multi_cols:
            key = ("Open", symbol)
            if key not in df.columns:
                print(f"{symbol}: missing Open column in batch result")
                continue
            series = df[key].dropna()
        else:
            if "Open" not in df.columns:
                print(f"{symbol}: missing Open column")
                continue
            series = df["Open"].dropna()

        if series.empty:
            print(f"{symbol}: Open series is empty")
            continue

        print(f"{symbol}: rows={len(series)} first={series.index[0]} {float(series.iloc[0])} last={series.index[-1]} {float(series.iloc[-1])}")


def main() -> None:
    symbols = [s.strip().upper() for s in sys.argv[1:] if s.strip()] or ["ETR"]

    end = db_end_utc_day()
    start = end - timedelta(days=35)

    print(f"testing symbols: {symbols}")
    print(f"start={start} end={end}")

    for symbol in symbols:
        try:
            df_single = yf.download(
                symbol,
                start=start,
                end=end,
                interval="1d",
                progress=False,
                auto_adjust=False,
            )
            print_open_summary(df_single, [symbol], f"single download: {symbol}")
        except Exception as e:
            print(f"\n=== single download: {symbol} ===")
            print(f"exception: {type(e).__name__}: {e}")

    try:
        df_batch = yf.download(
            symbols,
            start=start,
            end=end,
            interval="1d",
            progress=False,
            auto_adjust=False,
        )
        print_open_summary(df_batch, symbols, "batch download")
    except Exception as e:
        print("\n=== batch download ===")
        print(f"exception: {type(e).__name__}: {e}")


if __name__ == "__main__":
    main()
