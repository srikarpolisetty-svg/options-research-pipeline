import argparse
from datetime import date, datetime, timedelta, timezone

import _path_setup  # noqa: F401
import pandas as pd

from databasefunctions import get_sp500_symbols
from databentodatabasebackfillworkingversion import (
    clamp_end,
    databento_parent_symbol,
    filter_supported_option_chain_symbols,
    last_completed_market_date,
    prepare_definition_snapshot_with_stats,
    run_definition_timeseries_requests,
)
from policy.expiration import is_third_friday


def pick_test_week(reference_date: date) -> tuple[date, date]:
    days_since_friday = (reference_date.weekday() - 4) % 7
    friday = reference_date - timedelta(days=days_since_friday)

    if reference_date.weekday() < 4:
        friday -= timedelta(days=7)

    while is_third_friday(friday):
        friday -= timedelta(days=7)

    monday = friday - timedelta(days=4)
    return monday, friday


def choose_snapshot_day(monday: date, friday: date, day_name: str) -> date:
    offsets = {
        "monday": 0,
        "tuesday": 1,
        "wednesday": 2,
        "thursday": 3,
        "friday": 4,
    }
    day_name = day_name.lower()
    if day_name not in offsets:
        raise ValueError(f"Unsupported snapshot day: {day_name}")

    snapshot_day = monday + timedelta(days=offsets[day_name])
    if snapshot_day > friday:
        raise ValueError(f"Snapshot day {snapshot_day} is after Friday {friday}")
    return snapshot_day


def utc_midnight(d: date) -> pd.Timestamp:
    return pd.Timestamp(datetime.combine(d, datetime.min.time(), tzinfo=timezone.utc))


def build_requests(symbols: list[str], snapshot_day: date, expiration_date: date) -> list[dict]:
    start_ts = utc_midnight(snapshot_day)
    end_ts = clamp_end("OPRA.PILLAR", utc_midnight(snapshot_day + timedelta(days=1)).to_pydatetime())

    requests = []
    for symbol in symbols:
        requests.append(
            {
                "symbol": symbol,
                "parent": databento_parent_symbol(symbol),
                "expiration_date": expiration_date,
                "snapshot_day": snapshot_day,
                "trade_dates": [snapshot_day],
                "start": start_ts,
                "end": end_ts,
            }
        )
    return requests


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("symbols", nargs="*", help="Optional list of symbols to test")
    parser.add_argument("--reference-date", type=str, default=None, help="YYYY-MM-DD; defaults to latest completed market date")
    parser.add_argument(
        "--snapshot-day",
        type=str,
        default="monday",
        choices=["monday", "tuesday", "wednesday", "thursday", "friday"],
        help="Which day in the test week to use for the one-day definition request",
    )
    parser.add_argument("--verbose", action="store_true", help="Print one result line per successful request")
    parser.add_argument("--show-empty-prepared", action="store_true", help="Print symbols whose request succeeded but target expiration filtered to empty")
    args = parser.parse_args()

    if args.reference_date:
        reference_date = datetime.strptime(args.reference_date, "%Y-%m-%d").date()
    else:
        reference_date = last_completed_market_date()

    if args.symbols:
        symbols = filter_supported_option_chain_symbols(args.symbols)
    else:
        symbols = filter_supported_option_chain_symbols(get_sp500_symbols())

    monday, friday = pick_test_week(reference_date)
    snapshot_day = choose_snapshot_day(monday, friday, args.snapshot_day)
    requests = build_requests(symbols, snapshot_day, friday)

    print(f"[TEST] symbols={len(symbols)}")
    print(f"[TEST] chosen_week monday={monday.isoformat()} friday={friday.isoformat()}")
    print(f"[TEST] snapshot_day={snapshot_day.isoformat()} target_expiration={friday.isoformat()}")
    print(f"[TEST] one_day_range {snapshot_day.isoformat()} -> {snapshot_day.isoformat()} inclusive")

    results = run_definition_timeseries_requests(requests)
    successful_symbols = {request["symbol"] for request, _df in results}
    failed_symbols = sorted(set(symbols) - successful_symbols)

    raw_nonempty = 0
    raw_empty = 0
    prepared_nonempty = 0
    prepared_empty = 0
    total_raw_rows = 0
    total_prepared_rows = 0
    empty_prepared_symbols: list[str] = []

    for request, df_defs_raw in results:
        raw_rows = len(df_defs_raw)
        total_raw_rows += raw_rows
        if raw_rows > 0:
            raw_nonempty += 1
        else:
            raw_empty += 1

        df_defs, prep_stats = prepare_definition_snapshot_with_stats(
            df_defs_raw,
            symbol=request["symbol"],
            expiration_date=request["expiration_date"],
        )
        prepared_rows = len(df_defs)
        total_prepared_rows += prepared_rows
        if prepared_rows > 0:
            prepared_nonempty += 1
        else:
            prepared_empty += 1
            empty_prepared_symbols.append(request["symbol"])

        if args.verbose:
            print(
                f"[SYM] {request['symbol']}: raw={raw_rows:,} prepared={prepared_rows:,} "
                f"raw_sym={prep_stats['symbol_rows']:,} raw_exp={prep_stats['expiration_rows']:,}"
            )

    print(
        f"[TEST] request_success={len(results):,} request_fail={len(failed_symbols):,} "
        f"total={len(requests):,}"
    )
    print(
        f"[TEST] raw_nonempty={raw_nonempty:,} raw_empty={raw_empty:,} "
        f"prepared_nonempty={prepared_nonempty:,} prepared_empty={prepared_empty:,}"
    )
    print(
        f"[TEST] total_raw_rows={total_raw_rows:,} total_prepared_rows={total_prepared_rows:,}"
    )

    if failed_symbols:
        preview = ", ".join(failed_symbols[:25])
        if len(failed_symbols) > 25:
            preview += ", ..."
        print(f"[TEST] failed_symbols={preview}")

    if args.show_empty_prepared and empty_prepared_symbols:
        preview = ", ".join(sorted(empty_prepared_symbols)[:50])
        if len(empty_prepared_symbols) > 50:
            preview += ", ..."
        print(f"[TEST] empty_prepared_symbols={preview}")


if __name__ == "__main__":
    main()
