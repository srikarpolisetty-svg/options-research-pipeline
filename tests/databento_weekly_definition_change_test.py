import argparse
from datetime import date, datetime, timedelta, timezone

import _path_setup  # noqa: F401
import pandas as pd

from databentodatabasebackfillworkingversion import batch_get_df_chunked, clamp_end
from policy.expiration import is_third_friday


def pick_test_week(reference_date: date) -> tuple[date, date]:
    """
    Pick the most recent completed Friday week, skipping third-Friday weeks.
    Returns (monday, friday).
    """
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


def load_definition_snapshot(
    *,
    parents: list[str],
    seed_start_day: date,
    as_of_day: date,
) -> pd.DataFrame:
    start = utc_midnight(seed_start_day)
    end = clamp_end("OPRA.PILLAR", utc_midnight(as_of_day + timedelta(days=1)).to_pydatetime())

    df = batch_get_df_chunked(
        dataset="OPRA.PILLAR",
        schema="definition",
        stype_in="parent",
        symbols=parents,
        start=start,
        end=end,
        split_duration="day",
        poll_s=10.0,
    )
    if df is None or df.empty:
        return pd.DataFrame()

    if "underlying" not in df.columns:
        raise RuntimeError(f"Expected definition column 'underlying' not found. cols={list(df.columns)}")
    if "raw_symbol" not in df.columns:
        raise RuntimeError(f"Expected definition column 'raw_symbol' not found. cols={list(df.columns)}")

    time_col = "ts_event" if "ts_event" in df.columns else ("timestamp" if "timestamp" in df.columns else None)
    if time_col is None:
        raise RuntimeError(f"No timestamp-like definition column found. cols={list(df.columns)}")

    out = df.copy()
    out[time_col] = pd.to_datetime(out[time_col], utc=True, errors="coerce")
    out["exp_date"] = pd.to_datetime(out["expiration"], errors="coerce").dt.date
    out = out[out[time_col].notna() & out["raw_symbol"].notna()].copy()
    out = out.sort_values(time_col).drop_duplicates(subset=["raw_symbol"], keep="last").copy()
    return out


def summarize_changes(
    monday_df: pd.DataFrame,
    friday_df: pd.DataFrame,
    *,
    target_expiration: date,
    show_limit: int,
) -> None:
    monday_exp = monday_df[monday_df["exp_date"] == target_expiration].copy()
    friday_exp = friday_df[friday_df["exp_date"] == target_expiration].copy()

    monday_symbols = set(monday_exp["raw_symbol"].astype(str))
    friday_symbols = set(friday_exp["raw_symbol"].astype(str))

    added = sorted(friday_symbols - monday_symbols)
    removed = sorted(monday_symbols - friday_symbols)
    common = monday_symbols & friday_symbols

    print(
        f"[TEST] target expiration {target_expiration.isoformat()}: "
        f"monday_raw={len(monday_symbols):,} friday_raw={len(friday_symbols):,} "
        f"common={len(common):,} added_by_friday={len(added):,} removed_by_friday={len(removed):,}"
    )

    monday_by_underlying = monday_exp.groupby("underlying")["raw_symbol"].nunique().sort_index()
    friday_by_underlying = friday_exp.groupby("underlying")["raw_symbol"].nunique().sort_index()
    all_underlyings = sorted(set(monday_by_underlying.index) | set(friday_by_underlying.index))

    if all_underlyings:
        print("[TEST] per-underlying raw_symbol counts:")
        for underlying in all_underlyings:
            monday_count = int(monday_by_underlying.get(underlying, 0))
            friday_count = int(friday_by_underlying.get(underlying, 0))
            delta = friday_count - monday_count
            print(
                f"  {underlying}: monday={monday_count:,} friday={friday_count:,} "
                f"delta={delta:+,}"
            )

    if added:
        print(f"[TEST] raw symbols added by Friday (showing up to {show_limit}):")
        for raw_symbol in added[:show_limit]:
            print(f"  + {raw_symbol}")
        if len(added) > show_limit:
            print(f"  ... {len(added) - show_limit:,} more")
    else:
        print("[TEST] no raw symbols were added by Friday")

    if removed:
        print(f"[TEST] raw symbols missing by Friday (showing up to {show_limit}):")
        for raw_symbol in removed[:show_limit]:
            print(f"  - {raw_symbol}")
        if len(removed) > show_limit:
            print(f"  ... {len(removed) - show_limit:,} more")
    else:
        print("[TEST] no Monday raw symbols disappeared by Friday")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("symbols", nargs="*", default=["AAPL"])
    parser.add_argument("--reference-date", type=str, default=None, help="YYYY-MM-DD; defaults to today in local system date")
    parser.add_argument("--seed-buffer-days", type=int, default=7, help="Extra days before Monday to rebuild Monday snapshot safely")
    parser.add_argument("--show-limit", type=int, default=100)
    args = parser.parse_args()

    if args.reference_date:
        reference_date = datetime.strptime(args.reference_date, "%Y-%m-%d").date()
    else:
        reference_date = date.today()

    symbols = [symbol.strip().upper() for symbol in args.symbols if symbol.strip()]
    parents = [f"{symbol}.OPT" for symbol in symbols]

    monday, friday = pick_test_week(reference_date)
    seed_start_day = monday - timedelta(days=args.seed_buffer_days)

    print(f"[TEST] symbols={symbols}")
    print(f"[TEST] chosen week: monday={monday.isoformat()} friday={friday.isoformat()}")
    print(f"[TEST] monday snapshot range: {seed_start_day.isoformat()} -> {monday.isoformat()} inclusive")
    print(f"[TEST] friday snapshot range: {seed_start_day.isoformat()} -> {friday.isoformat()} inclusive")

    monday_df = load_definition_snapshot(
        parents=parents,
        seed_start_day=seed_start_day,
        as_of_day=monday,
    )
    friday_df = load_definition_snapshot(
        parents=parents,
        seed_start_day=seed_start_day,
        as_of_day=friday,
    )

    if monday_df.empty:
        print("[TEST] monday snapshot returned no definitions")
        return
    if friday_df.empty:
        print("[TEST] friday snapshot returned no definitions")
        return

    print(f"[TEST] monday snapshot rows after dedupe: {len(monday_df):,}")
    print(f"[TEST] friday snapshot rows after dedupe: {len(friday_df):,}")

    summarize_changes(
        monday_df,
        friday_df,
        target_expiration=friday,
        show_limit=args.show_limit,
    )


if __name__ == "__main__":
    main()
