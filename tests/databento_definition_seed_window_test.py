import argparse
from datetime import date, datetime, timedelta, timezone

import _path_setup  # noqa: F401
import databento as db
import pandas as pd

from config import DATABENTO_API_KEY
from databentodatabasebackfillworkingversion import (
    clamp_end,
    databento_parent_symbol,
    prepare_definition_snapshot,
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


def utc_midnight(d: date) -> pd.Timestamp:
    return pd.Timestamp(datetime.combine(d, datetime.min.time(), tzinfo=timezone.utc))


def choose_snapshot_day(monday: date, friday: date, day_name: str) -> date:
    day_name = day_name.lower()
    offsets = {
        "monday": 0,
        "tuesday": 1,
        "wednesday": 2,
        "thursday": 3,
        "friday": 4,
    }
    if day_name not in offsets:
        raise ValueError(f"Unsupported snapshot day: {day_name}")

    snapshot_day = monday + timedelta(days=offsets[day_name])
    if snapshot_day > friday:
        raise ValueError(f"Snapshot day {snapshot_day} is after Friday {friday}")
    return snapshot_day


def load_definition_window(
    hist: db.Historical,
    *,
    parent: str,
    start_day: date,
    as_of_day: date,
) -> pd.DataFrame:
    start = utc_midnight(start_day)
    end = clamp_end("OPRA.PILLAR", utc_midnight(as_of_day + timedelta(days=1)).to_pydatetime())
    return hist.timeseries.get_range(
        dataset="OPRA.PILLAR",
        schema="definition",
        symbols=[parent],
        stype_in="parent",
        start=start,
        end=end,
    ).to_df()


def summarize_ts_event(label: str, df: pd.DataFrame) -> None:
    if df is None or df.empty:
        print(f"[TEST] {label}: raw rows=0")
        return

    time_col = "ts_event" if "ts_event" in df.columns else ("timestamp" if "timestamp" in df.columns else None)
    if time_col is None:
        print(f"[TEST] {label}: raw rows={len(df):,} no timestamp column")
        return

    ts = pd.to_datetime(df[time_col], utc=True, errors="coerce").dropna().sort_values()
    if ts.empty:
        print(f"[TEST] {label}: raw rows={len(df):,} no valid {time_col}")
        return

    print(
        f"[TEST] {label}: raw rows={len(df):,} "
        f"{time_col}_first={ts.iloc[0]} {time_col}_last={ts.iloc[-1]}"
    )


def summarize_symbol(
    *,
    symbol: str,
    target_expiration: date,
    one_day_df: pd.DataFrame,
    seeded_df: pd.DataFrame,
    show_limit: int,
) -> None:
    one_day_raw = set(one_day_df["raw_symbol"].astype(str)) if not one_day_df.empty else set()
    seeded_raw = set(seeded_df["raw_symbol"].astype(str)) if not seeded_df.empty else set()

    missing_in_one_day = sorted(seeded_raw - one_day_raw)
    extra_in_one_day = sorted(one_day_raw - seeded_raw)
    common = one_day_raw & seeded_raw

    print(
        f"[TEST] {symbol} exp={target_expiration.isoformat()}: "
        f"one_day={len(one_day_raw):,} seeded={len(seeded_raw):,} "
        f"common={len(common):,} missing_in_one_day={len(missing_in_one_day):,} "
        f"extra_in_one_day={len(extra_in_one_day):,}"
    )

    if missing_in_one_day:
        preview = ", ".join(missing_in_one_day[:show_limit])
        if len(missing_in_one_day) > show_limit:
            preview += ", ..."
        print(f"[TEST] {symbol}: missing_in_one_day sample={preview}")
    else:
        print(f"[TEST] {symbol}: no seeded-only raw symbols")

    if extra_in_one_day:
        preview = ", ".join(extra_in_one_day[:show_limit])
        if len(extra_in_one_day) > show_limit:
            preview += ", ..."
        print(f"[TEST] {symbol}: extra_in_one_day sample={preview}")
    else:
        print(f"[TEST] {symbol}: no one_day-only raw symbols")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("symbols", nargs="*", default=["AAPL"])
    parser.add_argument("--reference-date", type=str, default=None, help="YYYY-MM-DD; defaults to today")
    parser.add_argument(
        "--snapshot-day",
        type=str,
        default="monday",
        choices=["monday", "tuesday", "wednesday", "thursday", "friday"],
        help="Which trading day in the test week to use as the one-day snapshot",
    )
    parser.add_argument("--seed-buffer-days", type=int, default=7, help="How many days before Monday to seed")
    parser.add_argument("--show-limit", type=int, default=25)
    args = parser.parse_args()

    if args.reference_date:
        reference_date = datetime.strptime(args.reference_date, "%Y-%m-%d").date()
    else:
        reference_date = date.today()

    monday, friday = pick_test_week(reference_date)
    snapshot_day = choose_snapshot_day(monday, friday, args.snapshot_day)
    seed_start_day = monday - timedelta(days=args.seed_buffer_days)
    hist = db.Historical(DATABENTO_API_KEY)

    symbols = [symbol.strip().upper() for symbol in args.symbols if symbol.strip()]

    print(f"[TEST] symbols={symbols}")
    print(f"[TEST] chosen week: monday={monday.isoformat()} friday={friday.isoformat()}")
    print(f"[TEST] snapshot_day={snapshot_day.isoformat()} target_expiration={friday.isoformat()}")
    print(f"[TEST] one-day range: {snapshot_day.isoformat()} -> {snapshot_day.isoformat()} inclusive")
    print(f"[TEST] seeded range: {seed_start_day.isoformat()} -> {snapshot_day.isoformat()} inclusive")

    for symbol in symbols:
        parent = databento_parent_symbol(symbol)
        print(f"[TEST] {symbol}: parent={parent}")

        one_day_raw = load_definition_window(
            hist,
            parent=parent,
            start_day=snapshot_day,
            as_of_day=snapshot_day,
        )
        seeded_raw = load_definition_window(
            hist,
            parent=parent,
            start_day=seed_start_day,
            as_of_day=snapshot_day,
        )

        summarize_ts_event(f"{symbol} one_day", one_day_raw)
        summarize_ts_event(f"{symbol} seeded", seeded_raw)

        one_day_prepared = prepare_definition_snapshot(
            one_day_raw,
            symbol=symbol,
            expiration_date=friday,
        )
        seeded_prepared = prepare_definition_snapshot(
            seeded_raw,
            symbol=symbol,
            expiration_date=friday,
        )

        print(
            f"[TEST] {symbol}: prepared rows one_day={len(one_day_prepared):,} "
            f"seeded={len(seeded_prepared):,}"
        )
        summarize_symbol(
            symbol=symbol,
            target_expiration=friday,
            one_day_df=one_day_prepared,
            seeded_df=seeded_prepared,
            show_limit=args.show_limit,
        )


if __name__ == "__main__":
    main()
