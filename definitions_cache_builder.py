# definition_cache_builder.py
# Pull OPRA.PILLAR definition data once/day and store as rows in DuckDB (not JSON blobs).

from __future__ import annotations

import datetime as dt
import threading
import time
import warnings
from collections import deque
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
import pytz
import pandas as pd
import databento as db
import duckdb
from databento.common.error import BentoWarning

from databasefunctions import get_sp500_symbols
from config import DATABENTO_API_KEY
from policy.expiration import is_nyse_market_holiday
from policy.option_symbols import (
    UNSUPPORTED_OPTION_CHAIN_SYMBOLS,
    databento_parent_symbol,
    filter_supported_option_chain_symbols,
)


DB_PATH = "definitioncache.duckdb"
SLEEP_SEC = 0.10  # small throttle so you don't slam the API
DEF_MAX_WORKERS = 50
DEF_RATE_LIMIT_COUNT = 50
DEF_RATE_LIMIT_WINDOW_S = 2.0
DEF_PROGRESS_EVERY = 25
DEF_SUBMIT_PROGRESS_EVERY = 25
DEF_REQUEST_MAX_ATTEMPTS = 3
DEF_REQUEST_RETRY_DELAY_S = 1.0
DEF_REQUEST_RETRY_BACKOFF = 2.0

_THREAD_LOCAL = threading.local()
NY_TZ = pytz.timezone("America/New_York")


def _definition_hist_client() -> db.Historical:
    hist = getattr(_THREAD_LOCAL, "hist_client", None)
    if hist is None:
        hist = db.Historical(DATABENTO_API_KEY)
        _THREAD_LOCAL.hist_client = hist
    return hist


def _prune_submit_times(submit_times: deque[float], now: float) -> None:
    while submit_times and (now - submit_times[0]) >= DEF_RATE_LIMIT_WINDOW_S:
        submit_times.popleft()


def _wait_for_submit_slot(submit_times: deque[float], last_submit_at: float | None) -> float:
    while True:
        now = time.monotonic()
        _prune_submit_times(submit_times, now)

        wait_for_window = 0.0
        if len(submit_times) >= DEF_RATE_LIMIT_COUNT:
            wait_for_window = DEF_RATE_LIMIT_WINDOW_S - (now - submit_times[0]) + 0.05

        wait_for_spacing = 0.0
        if last_submit_at is not None and len(submit_times) >= DEF_RATE_LIMIT_COUNT:
            wait_for_spacing = max(0.0, DEF_RATE_LIMIT_WINDOW_S / DEF_RATE_LIMIT_COUNT - (now - last_submit_at))

        wait_s = max(wait_for_window, wait_for_spacing, 0.0)
        if wait_s <= 0:
            return time.monotonic()

        time.sleep(wait_s)


def is_retryable_definition_error(exc: Exception) -> bool:
    message = str(exc).lower()
    retryable_fragments = (
        "504",
        "gateway timed out",
        "timed out",
        "timeout",
        "temporarily unavailable",
        "service unavailable",
        "connection reset",
        "connection aborted",
    )
    return any(fragment in message for fragment in retryable_fragments)


def fetch_definition_rows_for_symbol(
    symbol: str,
    start: dt.datetime,
    end: dt.datetime,
) -> tuple[str, str, pd.DataFrame | None, str | None]:
    hist = _definition_hist_client()
    parent_sym = databento_parent_symbol(symbol)

    last_error: Exception | None = None
    for attempt in range(1, DEF_REQUEST_MAX_ATTEMPTS + 1):
        try:
            with warnings.catch_warnings():
                warnings.filterwarnings(
                    "ignore",
                    message="No data found for the request you submitted.",
                    category=BentoWarning,
                )
                chain_df = hist.timeseries.get_range(
                    dataset="OPRA.PILLAR",
                    schema="definition",
                    symbols=parent_sym,
                    stype_in="parent",
                    start=start,
                    end=end,
                ).to_df()
            return symbol, parent_sym, chain_df, None
        except Exception as exc:
            last_error = exc
            if attempt >= DEF_REQUEST_MAX_ATTEMPTS or not is_retryable_definition_error(exc):
                break

            sleep_s = DEF_REQUEST_RETRY_DELAY_S * (DEF_REQUEST_RETRY_BACKOFF ** (attempt - 1))
            print(
                f"[RETRY] defs {symbol} ({parent_sym}) attempt {attempt}/{DEF_REQUEST_MAX_ATTEMPTS} "
                f"failed: {exc} | retrying in {sleep_s:.1f}s"
            )
            time.sleep(sleep_s)

    return symbol, parent_sym, None, str(last_error) if last_error is not None else "unknown error"


def live_trading_day(now_utc: dt.datetime | None = None) -> dt.date:
    if now_utc is None:
        now_utc = dt.datetime.now(tz=dt.timezone.utc)

    now_ny = now_utc.astimezone(NY_TZ)
    candidate = now_ny.date()

    while candidate.weekday() >= 5 or is_nyse_market_holiday(candidate):
        candidate -= dt.timedelta(days=1)

    return candidate


def definition_query_window_for_live_day(
    now_utc: dt.datetime | None = None,
) -> tuple[dt.datetime, dt.datetime, dt.date]:
    if now_utc is None:
        now_utc = dt.datetime.now(tz=dt.timezone.utc)

    market_date = live_trading_day(now_utc)
    start = dt.datetime.combine(market_date, dt.time.min, tzinfo=dt.timezone.utc)
    cutoff_ny = NY_TZ.localize(dt.datetime.combine(market_date, dt.time(hour=9, minute=0)))
    cutoff_utc = cutoff_ny.astimezone(dt.timezone.utc)
    is_current_live_day = market_date == now_utc.astimezone(NY_TZ).date()
    end = min(cutoff_utc, now_utc) if is_current_live_day else cutoff_utc
    return start, end, market_date


def build_definition_cache():
    start, end, market_date = definition_query_window_for_live_day()

    raw_symbols = list(get_sp500_symbols())
    symbols = filter_supported_option_chain_symbols(raw_symbols)
    skipped_symbols = sorted({
        str(symbol).strip().upper()
        for symbol in raw_symbols
        if isinstance(symbol, str) and str(symbol).strip().upper() in UNSUPPORTED_OPTION_CHAIN_SYMBOLS
    })
    print(f"[DEFS] total symbols={len(symbols)}", flush=True)
    if skipped_symbols:
        print(
            f"[DEFS] skipping unsupported symbols={len(skipped_symbols)} "
            f"symbols={', '.join(skipped_symbols)}"
        )
    print(
        f"[DEFS] query range market_date={market_date.isoformat()} "
        f"start={start.isoformat()} end={end.isoformat()}"
    )

    con = duckdb.connect(DB_PATH)
        # Replace old cache each run
    con.execute("DROP TABLE IF EXISTS definition_cache;")


    created_table = False
    inserted_total = 0
    skipped = 0
    errors = 0
    submitted = 0
    completed = 0
    pending_symbols = deque(symbols)
    active_futures = {}
    submit_times: deque[float] = deque()
    last_submit_at: float | None = None
    max_workers = min(DEF_MAX_WORKERS, len(symbols))

    print(
        f"[DEFS] requests={len(symbols)} workers={max_workers} "
        f"rate<={DEF_RATE_LIMIT_COUNT}/{int(DEF_RATE_LIMIT_WINDOW_S)}s"
    )

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        while pending_symbols or active_futures:
            while pending_symbols and len(active_futures) < max_workers:
                last_submit_at = _wait_for_submit_slot(submit_times, last_submit_at)
                sym = pending_symbols.popleft()
                future = executor.submit(fetch_definition_rows_for_symbol, sym, start, end)
                active_futures[future] = sym
                submit_times.append(last_submit_at)
                submitted += 1
                if submitted % DEF_SUBMIT_PROGRESS_EVERY == 0 or submitted == len(symbols):
                    print(f"[DEFS] submitted {submitted}/{len(symbols)}")

            if not active_futures:
                continue

            done, _ = wait(active_futures, return_when=FIRST_COMPLETED)
            for future in done:
                requested_sym = active_futures.pop(future)
                symbol, parent_sym, chain_df, error = future.result()
                completed += 1

                if error is not None:
                    errors += 1
                    print(f"[{completed}/{len(symbols)}] ERROR {symbol} ({parent_sym}): {error}")
                    continue

                if chain_df is None or chain_df.empty:
                    skipped += 1
                else:
                    chain_df = chain_df.copy()
                    chain_df["symbol"] = symbol

                    if not created_table:
                        con.register("tmp_def", chain_df)
                        con.execute("""
                            CREATE TABLE IF NOT EXISTS definition_cache AS
                            SELECT * FROM tmp_def WHERE 0=1
                        """)
                        con.unregister("tmp_def")
                        created_table = True

                    con.append("definition_cache", chain_df)
                    inserted_total += len(chain_df)

                if completed % DEF_PROGRESS_EVERY == 0 or completed == len(symbols):
                    print(
                        f"[DEFS] finished {completed}/{len(symbols)} "
                        f"inserted_total={inserted_total} skipped={skipped} errors={errors}"
                    )

    con.close()
    print("[DEFS] done", flush=True)
    print(f"[DEFS] inserted_rows={inserted_total}", flush=True)
    print(f"[DEFS] skipped_empty={skipped}", flush=True)
    print(f"[DEFS] errors={errors}", flush=True)


if __name__ == "__main__":
    build_definition_cache()
