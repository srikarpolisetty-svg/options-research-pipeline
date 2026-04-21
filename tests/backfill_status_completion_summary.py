import argparse

import _path_setup  # noqa: F401
from databasefunctions import get_sp500_symbols
from databentodatabasebackfillworkingversion import (
    fetch_last_days,
    filter_supported_option_chain_symbols,
    get_existing_dates,
    last_completed_market_date,
)


def format_symbol_list(symbols: list[str], limit: int) -> str:
    if not symbols:
        return "none"

    preview = ", ".join(symbols[:limit])
    if len(symbols) > limit:
        preview += ", ..."
    return preview


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("symbols", nargs="*", help="Optional list of symbols to inspect")
    parser.add_argument("--days-back", type=int, default=35)
    parser.add_argument(
        "--show-symbols",
        action="store_true",
        help="Print a preview of which symbols are complete vs incomplete",
    )
    parser.add_argument(
        "--symbol-limit",
        type=int,
        default=25,
        help="Max symbols to show in each preview list",
    )
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

    complete_symbols: list[str] = []
    incomplete_symbols: list[str] = []
    total_target = 0
    total_covered = 0
    total_missing = 0

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

        if missing_dates:
            incomplete_symbols.append(sym)
        else:
            complete_symbols.append(sym)

    print(
        f"[TEST] symbols={len(eligible_syms)} days_back={args.days_back} "
        f"latest_trade_date={latest_trade_date.isoformat()}"
    )
    print(
        f"[TEST] complete_symbols={len(complete_symbols)} "
        f"incomplete_symbols={len(incomplete_symbols)}"
    )
    print(
        f"[TEST] totals: target={total_target:,} covered={total_covered:,} "
        f"missing={total_missing:,}"
    )

    if args.show_symbols:
        print(
            f"[TEST] complete_list={format_symbol_list(complete_symbols, args.symbol_limit)}"
        )
        print(
            f"[TEST] incomplete_list={format_symbol_list(incomplete_symbols, args.symbol_limit)}"
        )


if __name__ == "__main__":
    main()
