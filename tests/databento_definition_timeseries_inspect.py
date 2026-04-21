import argparse
from datetime import datetime, timedelta, timezone

import _path_setup  # noqa: F401
import databento as db
import pandas as pd

from config import DATABENTO_API_KEY


def databento_parent_symbol(symbol: str) -> str:
    cleaned = symbol.strip().upper().replace("-", "").replace(".", "")
    return f"{cleaned}.OPT"


def default_time_range() -> tuple[str, str]:
    end = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
    start = end - timedelta(days=2)
    return start.isoformat(), end.isoformat()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("symbols", nargs="*", default=["AAPL"], help="Underlying symbols like AAPL or BRK-B")
    parser.add_argument("--start", type=str, default=None, help="ISO timestamp/date for Databento start")
    parser.add_argument("--end", type=str, default=None, help="ISO timestamp/date for Databento end")
    args = parser.parse_args()

    default_start, default_end = default_time_range()
    start = args.start or default_start
    end = args.end or default_end

    symbols = [s.strip().upper() for s in args.symbols if s.strip()]
    parents = [databento_parent_symbol(symbol) for symbol in symbols]

    print(f"[TEST] symbols={symbols}")
    print(f"[TEST] parents={parents}")
    print(f"[TEST] start={start}")
    print(f"[TEST] end={end}")

    hist = db.Historical(DATABENTO_API_KEY)
    data = hist.timeseries.get_range(
        dataset="OPRA.PILLAR",
        schema="definition",
        symbols=parents,
        stype_in="parent",
        start=start,
        end=end,
    )
    df = data.to_df()

    if df is None or df.empty:
        print("[TEST] definition timeseries returned no rows")
        return

    print(f"[TEST] rows={len(df):,}")
    print("[TEST] columns:")
    for col in df.columns:
        print(f"  - {col}")

    if "ts_event" in df.columns:
        ts_event = pd.to_datetime(df["ts_event"], utc=True, errors="coerce").dropna().sort_values()
        if not ts_event.empty:
            sample = ", ".join(str(ts) for ts in ts_event.head(5).tolist())
            print(
                f"[TEST] ts_event: first={ts_event.iloc[0]} last={ts_event.iloc[-1]} "
                f"sample={sample}"
            )


if __name__ == "__main__":
    main()
