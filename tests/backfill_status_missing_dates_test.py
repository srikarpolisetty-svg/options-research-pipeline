import argparse

import _path_setup  # noqa: F401
from databasefunctions import get_sp500_symbols
from databentodatabasebackfillworkingversion import (
    fetch_last_days,
    filter_supported_option_chain_symbols,
    get_existing_dates,
    last_completed_market_date,
)


def format_dates(dates: set, limit: int) -> str:
    ordered = sorted(dates)
    if not ordered:
        return "none"

    preview = ", ".join(d.isoformat() for d in ordered[:limit])
    if len(ordered) > limit:
        preview += ", ..."
    return preview


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("symbols", nargs="*", help="Optional list of symbols to inspect")
    parser.add_argument("--days-back", type=int, default=35)
    parser.add_argument("--only-missing", action="store_true", help="Print only symbols with missing dates")
    parser.add_argument("--show-dates", action="store_true", help="Show the actual missing date list preview")
    parser.add_argument("--date-limit", type=int, default=10, help="Max missing dates to print per symbol preview")
    args = parser.parse_args()

    if args.symbols:
        symbols = filter_supported_option_chain_symbols(args.symbols)
    else:
        symbols = filter_supported_option_chain_symbols(get_sp500_symbols())

    latest_trade_date = last_completed_market_date()
    daily_underlying = fetch_last_days(symbols, args.days_back)
    if not daily_underlying:
        print("[TEST] no symbols with valid underlying data")
        return

    eligible_syms = sorted(daily_underlying.keys())
    existing_dates_by_symbol = get_existing_dates(eligible_syms, args.days_back)

    total_target = 0
    total_covered = 0
    total_missing = 0
    printed = 0

    print(
        f"[TEST] symbols={len(eligible_syms)} days_back={args.days_back} "
        f"latest_trade_date={latest_trade_date.isoformat()}"
    )

    for sym in eligible_syms:
        target_dates = {
            ts.date()
            for ts in daily_underlying[sym].index
            if ts.date() <= latest_trade_date
        }
        covered_dates = existing_dates_by_symbol.get(sym, set()) & target_dates
        missing_dates = target_dates - covered_dates

        total_target += len(target_dates)
        total_covered += len(covered_dates)
        total_missing += len(missing_dates)

        if args.only_missing and not missing_dates:
            continue

        line = (
            f"[STATUS] {sym}: target={len(target_dates)} "
            f"covered={len(covered_dates)} missing={len(missing_dates)}"
        )
        if args.show_dates and missing_dates:
            line += f" | missing_dates={format_dates(missing_dates, args.date_limit)}"
        print(line)
        printed += 1

    print(
        f"[TEST] totals: target={total_target:,} covered={total_covered:,} "
        f"missing={total_missing:,} printed={printed:,}"
    )


if __name__ == "__main__":
    main()
