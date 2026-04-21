from __future__ import annotations

import argparse
import datetime as dt
import pytz
import databento as db

from config import DATABENTO_API_KEY
from databentodatabase import run_db_option_snapshot_to_parquet
from databasefunctions import get_sp500_symbols

# now expects (rec, quote_cache, vol_cache)
from databentodatabase import handle_live_record  # adjust import if needed


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--shard", type=int, default=0)
    parser.add_argument("--shards", type=int, default=1)
    parser.add_argument("--run_id", required=True)
    args = parser.parse_args()

    symbols = sorted(get_sp500_symbols())
    my_symbols = symbols[args.shard::args.shards]

    print(
        f"Shard {args.shard}/{args.shards} processing {len(my_symbols)} symbols (run_id={args.run_id})"
    )

    for symbol in my_symbols:
        print(f"--- {symbol} (shard {args.shard}) ---")
        try:
            run_db_option_snapshot_to_parquet(
                args.run_id,
                symbol,
                args.shard,
            )
        except Exception as e:
            print(f"ERROR on {symbol}: {e}")


if __name__ == "__main__":
    main()
