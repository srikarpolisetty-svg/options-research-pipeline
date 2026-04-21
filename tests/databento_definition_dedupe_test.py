import argparse

import _path_setup  # noqa: F401
import pandas as pd

from databentodatabasebackfillworkingversion import (
    batch_get_df_chunked,
    clamp_end,
    db_end_utc_day,
)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("symbols", nargs="*", default=["AAPL", "TSLA"])
    parser.add_argument("--days-back", type=int, default=35)
    args = parser.parse_args()

    symbols = [symbol.strip().upper() for symbol in args.symbols if symbol.strip()]
    end = clamp_end("OPRA.PILLAR", db_end_utc_day())
    start = end - pd.Timedelta(days=args.days_back)
    parents = [f"{symbol}.OPT" for symbol in symbols]

    print(f"testing parents: {parents}")
    print(f"start={start} end={end - pd.Timedelta(hours=48)}")

    df_defs_all = batch_get_df_chunked(
        dataset="OPRA.PILLAR",
        schema="definition",
        stype_in="parent",
        symbols=parents,
        start=start,
        end=end - pd.Timedelta(hours=48),
        split_duration="day",
        poll_s=10.0,
    )

    if df_defs_all is None or df_defs_all.empty:
        print("no definitions returned")
        return

    print(f"defs rows: {len(df_defs_all):,}")

    if "raw_symbol" not in df_defs_all.columns:
        print("raw_symbol column missing")
        print(f"columns: {sorted(df_defs_all.columns.tolist())}")
        return

    unique_raw = df_defs_all["raw_symbol"].nunique(dropna=True)
    print(f"unique raw_symbol: {unique_raw:,}")
    print(f"duplicate rows by raw_symbol: {len(df_defs_all) - unique_raw:,}")

    timestamp_col = None
    for candidate in ["ts_event", "timestamp", "ts_recv"]:
        if candidate in df_defs_all.columns:
            timestamp_col = candidate
            break

    if timestamp_col is None:
        print("no timestamp-like column found, so dedupe test will use current row order")
        deduped = df_defs_all.drop_duplicates(subset=["raw_symbol"], keep="last")
    else:
        print(f"timestamp column used for ordering: {timestamp_col}")
        ordered = df_defs_all.sort_values(timestamp_col)
        deduped = ordered.drop_duplicates(subset=["raw_symbol"], keep="last")

    print(f"rows after dedupe: {len(deduped):,}")
    print(f"rows removed by dedupe: {len(df_defs_all) - len(deduped):,}")

    dup_counts = (
        df_defs_all["raw_symbol"]
        .value_counts(dropna=True)
        .loc[lambda s: s > 1]
        .head(10)
    )

    if dup_counts.empty:
        print("no duplicate raw_symbol values found")
    else:
        print("\nTop duplicate raw_symbol counts:")
        print(dup_counts.to_string())


if __name__ == "__main__":
    main()
