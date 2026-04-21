import argparse
from pathlib import Path

import duckdb
import pandas as pd

import _path_setup  # noqa: F401
from policy.expiration import weekly_expiration_anchor


ROOT = Path(__file__).resolve().parents[1]
DB_PATH = ROOT / "definitioncache.duckdb"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Audit expiration coverage in definition_cache by symbol."
    )
    parser.add_argument(
        "--sample-size",
        type=int,
        default=10,
        help="How many sample symbols to print for each failing bucket.",
    )
    return parser.parse_args()


def load_definition_expirations() -> pd.DataFrame:
    con = duckdb.connect(str(DB_PATH), read_only=True)
    try:
        return con.execute(
            """
            SELECT symbol, expiration
            FROM definition_cache
            """
        ).fetchdf()
    finally:
        con.close()


def to_expiration_dates(expirations: pd.Series) -> list:
    parsed = (
        pd.to_datetime(expirations, errors="coerce", utc=True)
        .dropna()
        .dt.date
        .drop_duplicates()
        .tolist()
    )
    return sorted(parsed)


def main() -> None:
    args = parse_args()
    df = load_definition_expirations()

    if df.empty:
        print("[TEST] definition_cache is empty")
        return

    total_symbols = 0
    friday_symbols = 0
    no_friday_symbols: list[str] = []
    weekly_symbols = 0
    no_weekly_symbols: list[str] = []
    invalid_only_symbols = 0

    for symbol, symbol_df in df.groupby("symbol", sort=True):
        total_symbols += 1
        expiration_dates = to_expiration_dates(symbol_df["expiration"])
        if not expiration_dates:
            invalid_only_symbols += 1
            no_friday_symbols.append(str(symbol))
            no_weekly_symbols.append(str(symbol))
            continue

        has_friday = any(exp_date.weekday() == 4 for exp_date in expiration_dates)
        has_weekly_style = any(weekly_expiration_anchor(exp_date) is not None for exp_date in expiration_dates)

        if has_friday:
            friday_symbols += 1
        else:
            no_friday_symbols.append(str(symbol))

        if has_weekly_style:
            weekly_symbols += 1
        else:
            no_weekly_symbols.append(str(symbol))

    print(f"[TEST] symbols={total_symbols} rows={len(df):,}")
    print(
        f"[TEST] literal_friday_symbols={friday_symbols} "
        f"no_literal_friday_symbols={total_symbols - friday_symbols}"
    )
    print(
        f"[TEST] weekly_style_symbols={weekly_symbols} "
        f"no_weekly_style_symbols={total_symbols - weekly_symbols}"
    )
    print(f"[TEST] invalid_only_symbols={invalid_only_symbols}")

    if no_friday_symbols:
        print(
            f"[TEST] no_literal_friday_samples="
            f"{', '.join(no_friday_symbols[:args.sample_size])}"
        )
    if no_weekly_symbols:
        print(
            f"[TEST] no_weekly_style_samples="
            f"{', '.join(no_weekly_symbols[:args.sample_size])}"
        )


if __name__ == "__main__":
    main()
