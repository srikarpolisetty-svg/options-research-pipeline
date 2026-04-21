import argparse
import time
from collections import deque
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from datetime import datetime, timedelta, timezone

import _path_setup  # noqa: F401
import databento as db
import pandas as pd

from config import DATABENTO_API_KEY


DEFAULT_SYMBOLS = [
    "AAPL", "MSFT", "AMZN", "NVDA", "META",
    "GOOGL", "TSLA", "AMD", "NFLX", "JPM",
    "BAC", "XOM", "CVX", "WMT", "COST",
    "HD", "PG", "KO", "PEP", "DIS",
    "UNH", "CRM", "ORCL", "QCOM", "GS",
    "ABBV", "ADBE", "AXP", "BKNG", "C",
    "CAT", "CMCSA", "CSCO", "CVS", "DE",
    "DHR", "GE", "INTC", "IBM", "LIN",
    "LLY", "LOW", "MCD", "MRK", "MS",
    "NOW", "PLTR", "RTX", "TMUS", "TMO",
]
RATE_LIMIT_COUNT_DEFAULT = 50
RATE_LIMIT_WINDOW_S_DEFAULT = 2.0


def databento_parent_symbol(symbol: str) -> str:
    cleaned = symbol.strip().upper().replace("-", "").replace(".", "")
    return f"{cleaned}.OPT"


def default_time_range() -> tuple[str, str]:
    end = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
    start = end - timedelta(days=2)
    return start.isoformat(), end.isoformat()


def run_one_request(request: dict) -> tuple[dict, pd.DataFrame, str | None, float]:
    start_t = time.monotonic()
    client = db.Historical(DATABENTO_API_KEY)
    try:
        df = client.timeseries.get_range(
            dataset=request["dataset"],
            schema=request["schema"],
            symbols=[request["parent"]],
            stype_in=request["stype_in"],
            start=request["start"],
            end=request["end"],
        ).to_df()
        return request, df, None, time.monotonic() - start_t
    except Exception as exc:
        return request, pd.DataFrame(), str(exc), time.monotonic() - start_t


def summarize_df(df: pd.DataFrame) -> str:
    if df is None or df.empty:
        return "rows=0"

    time_col = "ts_event" if "ts_event" in df.columns else ("timestamp" if "timestamp" in df.columns else None)
    if time_col is None:
        return f"rows={len(df):,}"

    ts = pd.to_datetime(df[time_col], utc=True, errors="coerce").dropna().sort_values()
    if ts.empty:
        return f"rows={len(df):,} {time_col}=none"

    return (
        f"rows={len(df):,} "
        f"{time_col}_first={ts.iloc[0]} "
        f"{time_col}_last={ts.iloc[-1]}"
    )


def _prune_submit_times(submit_times: deque[float], now: float, window_s: float) -> None:
    while submit_times and now - submit_times[0] >= window_s:
        submit_times.popleft()


def _wait_for_submit_slot(submit_times: deque[float], limit_count: int, window_s: float) -> float:
    while True:
        now = time.monotonic()
        _prune_submit_times(submit_times, now, window_s)
        if len(submit_times) < limit_count:
            return time.monotonic()

        wait_s = window_s - (now - submit_times[0]) + 0.05
        if wait_s > 0:
            time.sleep(wait_s)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("symbols", nargs="*", help="Optional list of underlying symbols")
    parser.add_argument("--concurrent", type=int, default=50, help="How many requests to run at once")
    parser.add_argument("--request-count", type=int, default=50, help="How many total requests to issue")
    parser.add_argument("--rate-limit-count", type=int, default=RATE_LIMIT_COUNT_DEFAULT, help="Max submits inside the rolling window")
    parser.add_argument("--rate-limit-window-s", type=float, default=RATE_LIMIT_WINDOW_S_DEFAULT, help="Rolling submit window in seconds")
    parser.add_argument("--schema", type=str, default="definition")
    parser.add_argument("--dataset", type=str, default="OPRA.PILLAR")
    parser.add_argument("--stype-in", type=str, default="parent")
    parser.add_argument("--start", type=str, default=None, help="ISO timestamp/date")
    parser.add_argument("--end", type=str, default=None, help="ISO timestamp/date")
    args = parser.parse_args()

    symbols = [s.strip().upper() for s in args.symbols if s.strip()] or DEFAULT_SYMBOLS
    if len(symbols) < args.request_count:
        raise ValueError(f"Need at least {args.request_count} symbols, got {len(symbols)}")

    selected_symbols = symbols[:args.request_count]
    default_start, default_end = default_time_range()
    start = args.start or default_start
    end = args.end or default_end

    requests = []
    for idx, symbol in enumerate(selected_symbols, start=1):
        requests.append({
            "idx": idx,
            "symbol": symbol,
            "parent": databento_parent_symbol(symbol),
            "dataset": args.dataset,
            "schema": args.schema,
            "stype_in": args.stype_in,
            "start": start,
            "end": end,
        })

    print(
        f"[TEST] concurrent={args.concurrent} built_requests={len(requests)} "
        f"rate<={args.rate_limit_count}/{args.rate_limit_window_s:g}s "
        f"schema={args.schema} dataset={args.dataset}"
    )
    print(f"[TEST] start={start}")
    print(f"[TEST] end={end}")
    print(f"[TEST] symbols={selected_symbols}")

    overall_start = time.monotonic()
    active_futures = {}
    submit_times: deque[float] = deque()
    successes = 0
    failures = 0

    with ThreadPoolExecutor(max_workers=args.concurrent) as executor:
        for request in requests:
            submitted_at = _wait_for_submit_slot(
                submit_times,
                args.rate_limit_count,
                args.rate_limit_window_s,
            )
            submit_times.append(submitted_at)
            print(f"[REQ] submit {request['idx']}/{len(requests)} {request['symbol']} parent={request['parent']}")
            future = executor.submit(run_one_request, request)
            active_futures[future] = request

        while active_futures:
            done, _ = wait(active_futures, return_when=FIRST_COMPLETED)
            for future in done:
                request = active_futures.pop(future)
                req, df, error, elapsed = future.result()
                if error:
                    failures += 1
                    print(
                        f"[REQ] fail {req['idx']}/{len(requests)} {req['symbol']} "
                        f"elapsed={elapsed:.2f}s error={error}"
                    )
                else:
                    successes += 1
                    print(
                        f"[REQ] ok {req['idx']}/{len(requests)} {req['symbol']} "
                        f"elapsed={elapsed:.2f}s {summarize_df(df)}"
                    )

    overall_elapsed = time.monotonic() - overall_start
    print(
        f"[TEST] done success={successes} fail={failures} "
        f"total={len(requests)} elapsed={overall_elapsed:.2f}s"
    )


if __name__ == "__main__":
    main()
