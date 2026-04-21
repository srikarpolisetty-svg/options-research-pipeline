import argparse
import shutil
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import timedelta
from pathlib import Path

import _path_setup  # noqa: F401
import databento as db
import pandas as pd

from config import DATABENTO_API_KEY
from databentodatabasebackfillworkingversion import (
    BATCH_DIR,
    POLL_S,
    batch_get_df_chunked,
    build_daily_leg_map,
    build_def_map,
    clamp_end,
    db_end_utc_day,
    detect_parent_col,
    fetch_last_days,
    parent_to_underlying,
)


def wait_for_batch_job(client: db.Historical, job_id: str, *, schema: str, symbol_count: int, poll_s: float) -> None:
    last_state = None
    last_progress = None

    while True:
        jobs = client.batch.list_jobs(states=["queued", "processing", "done", "expired"])
        details = next((job for job in jobs if job.get("id") == job_id), None)

        if details is None:
            time.sleep(poll_s)
            continue

        state = details.get("state")
        progress = details.get("progress")

        if state != last_state or progress != last_progress:
            if progress is None:
                print(f"[PARALLEL] {schema} job {job_id} ({symbol_count} symbols): state={state}")
            else:
                print(f"[PARALLEL] {schema} job {job_id} ({symbol_count} symbols): state={state} progress={progress}%")
            last_state = state
            last_progress = progress

        if state == "done":
            return

        if state == "expired":
            raise RuntimeError(f"Batch job expired: schema={schema} job_id={job_id}")

        time.sleep(poll_s)


def _to_iso(x) -> str:
    return pd.Timestamp(x).to_pydatetime().isoformat()


def parallel_batch_get_df(
    *,
    dataset: str,
    schema: str,
    symbols: list[str],
    start,
    end,
    stype_in: str,
    split_duration: str,
    poll_s: float,
) -> pd.DataFrame:
    client = db.Historical(DATABENTO_API_KEY)
    job = client.batch.submit_job(
        dataset=dataset,
        start=_to_iso(start),
        end=_to_iso(end),
        symbols=symbols,
        schema=schema,
        split_duration=split_duration,
        stype_in=stype_in,
    )
    job_id = job["id"]
    print(f"[PARALLEL] submitted {schema} job {job_id} for {len(symbols)} symbols")
    wait_for_batch_job(client, job_id, schema=schema, symbol_count=len(symbols), poll_s=poll_s)

    out_dir = Path(BATCH_DIR) / "parallel_test" / job_id
    out_dir.mkdir(parents=True, exist_ok=True)
    try:
        files = client.batch.download(job_id=job_id, output_dir=out_dir)
        dfs = []
        for f in sorted(files):
            if str(f).endswith(".dbn.zst"):
                store = db.DBNStore.from_file(f)
                dfs.append(store.to_df())
        return pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()
    finally:
        shutil.rmtree(out_dir, ignore_errors=True)


def build_latest_day_raw_symbols(symbols: list[str], days_back: int) -> list[str]:
    end = clamp_end("OPRA.PILLAR", db_end_utc_day())
    start = end - timedelta(days=days_back)

    daily_underlying = fetch_last_days(symbols, days_back)
    if not daily_underlying:
        raise RuntimeError("No underlying data returned from Yahoo.")

    parents = [f"{symbol}.OPT" for symbol in sorted(daily_underlying)]
    df_defs_all = batch_get_df_chunked(
        dataset="OPRA.PILLAR",
        schema="definition",
        stype_in="parent",
        symbols=parents,
        start=start,
        end=end - timedelta(hours=48),
        split_duration="day",
        poll_s=POLL_S,
    )
    if df_defs_all is None or df_defs_all.empty:
        raise RuntimeError("No option definitions returned from Databento.")

    parent_col = detect_parent_col(df_defs_all)
    raw_needed: set[str] = set()

    for parent_val, g in df_defs_all.groupby(parent_col):
        sym = parent_to_underlying(parent_val)
        open_price_schedule = daily_underlying.get(sym)
        if open_price_schedule is None or open_price_schedule.empty:
            continue

        df_defs = g.copy()
        strikes = df_defs["strike_price"].dropna().astype(float).unique().tolist()
        strikes.sort()
        expirations = pd.to_datetime(df_defs["expiration"]).dt.strftime("%Y%m%d").dropna().unique().tolist()
        daily_leg_map = build_daily_leg_map(open_price_schedule, strikes, expirations)
        if not daily_leg_map:
            continue

        latest_trade_date = max(daily_leg_map)
        exp_date, _days_to_expiry, strike_sides = daily_leg_map[latest_trade_date]
        def_map = build_def_map(df_defs)

        for strike, side in strike_sides:
            raw_symbol = def_map.get((float(strike), side, exp_date))
            if raw_symbol:
                raw_needed.add(raw_symbol)

    if not raw_needed:
        raise RuntimeError("No raw symbols produced for the latest eligible date.")

    return sorted(raw_needed)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("symbols", nargs="*", default=["AAPL", "TSLA"])
    parser.add_argument("--days-back", type=int, default=35)
    parser.add_argument("--market-days", type=int, default=5)
    parser.add_argument("--poll-s", type=float, default=POLL_S)
    args = parser.parse_args()

    symbols = [symbol.strip().upper() for symbol in args.symbols if symbol.strip()]
    end = clamp_end("OPRA.PILLAR", db_end_utc_day())
    end_batch = end - timedelta(hours=48)
    start_batch = end_batch - timedelta(days=args.market_days)

    overall_start = time.time()
    raw_symbols = build_latest_day_raw_symbols(symbols, args.days_back)
    print(f"[PARALLEL] testing underlyings={symbols}")
    print(f"[PARALLEL] raw symbols selected ({len(raw_symbols)}): {raw_symbols}")
    print(f"[PARALLEL] start={start_batch} end={end_batch}")

    schema_requests = [
        {
            "dataset": "OPRA.PILLAR",
            "schema": "cbbo-1s",
            "symbols": raw_symbols,
            "start": start_batch,
            "end": end_batch,
            "stype_in": "raw_symbol",
            "split_duration": "day",
            "poll_s": args.poll_s,
        },
        {
            "dataset": "OPRA.PILLAR",
            "schema": "trades",
            "symbols": raw_symbols,
            "start": start_batch,
            "end": end_batch,
            "stype_in": "raw_symbol",
            "split_duration": "day",
            "poll_s": args.poll_s,
        },
        {
            "dataset": "OPRA.PILLAR",
            "schema": "statistics",
            "symbols": raw_symbols,
            "start": start_batch - pd.Timedelta(days=1),
            "end": end_batch,
            "stype_in": "raw_symbol",
            "split_duration": "day",
            "poll_s": args.poll_s,
        },
    ]

    results = {}
    with ThreadPoolExecutor(max_workers=len(schema_requests)) as executor:
        start_times = {}
        future_to_schema = {
            executor.submit(parallel_batch_get_df, **request): request["schema"]
            for request in schema_requests
        }
        for future in future_to_schema:
            start_times[future] = time.time()

        for future in as_completed(future_to_schema):
            schema = future_to_schema[future]
            try:
                df = future.result()
                results[schema] = df
                elapsed = time.time() - start_times[future]
                print(f"[PARALLEL] {schema}: rows={len(df):,} elapsed={elapsed:.2f}s")
            except Exception as e:
                print(f"[PARALLEL] {schema}: failed with {type(e).__name__}: {e}")
                raise

    total_elapsed = time.time() - overall_start
    print(f"[PARALLEL] complete in {total_elapsed:.2f}s")
    for schema, df in results.items():
        print(f"[PARALLEL] summary {schema}: columns={sorted(df.columns.tolist())} rows={len(df):,}")


if __name__ == "__main__":
    main()
