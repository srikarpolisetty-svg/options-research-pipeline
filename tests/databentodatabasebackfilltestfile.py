"""
TEST / TRANSPARENCY VERSION (VERBOSE)

What this gives you:
- Loud, step-by-step logs for each phase (Underlying -> Definitions -> Planning -> Batch Download -> Compute -> Insert)
- Timings per step + totals
- Counts: symbols attempted/kept/skipped, raw_symbols union size, rows inserted, etc.
- Optional "test knobs" so you can run small:
    --max-symbols 10
    --max-plan-symbols 3
    --max-hours 2
    --dry-run (no DB insert)
    --debug-dump (write CSVs of big dfs)
"""

import databento as db
import yfinance as yf
import pandas as pd
from datetime import datetime, timedelta, timezone
import _path_setup  # noqa: F401
from config import DATABENTO_API_KEY
import math
import duckdb
import argparse
import zlib
import time
import operator
import pathlib
import traceback

from execution_functions import get_all_symbolsTester

client = db.Historical(DATABENTO_API_KEY)

DB_PATH = "options_data.db"
BATCH_DIR = pathlib.Path("batch_downloads")
DEBUG_DIR = pathlib.Path("debug_dumps")

# Databento batch symbol cap (hard limit)
MAX_SYMBOLS_PER_JOB = 2000
POLL_S = 2.0

# Definitions batching (practical safe chunk)
DEF_CHUNK_SIZE = 100


# ---------------------------
# Logging / timing helpers
# ---------------------------
def ts_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3] + "Z"

def log(msg: str):
    print(f"[{ts_now()}] {msg}", flush=True)

class Timer:
    def __init__(self, name: str):
        self.name = name
        self.t0 = None

    def __enter__(self):
        self.t0 = time.perf_counter()
        log(f"▶ START {self.name}")
        return self

    def __exit__(self, exc_type, exc, tb):
        dt = time.perf_counter() - self.t0
        log(f"◀ END   {self.name} | {dt:.3f}s")
        return False


# ---------- TIME RANGE (Databento-safe end boundary) ----------
def db_end_utc_day() -> datetime:
    """
    Use a 1-day buffer to avoid Databento 'end after available end' (422).
    Returns start of yesterday (00:00:00 UTC).
    """
    today_utc_0 = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
    return today_utc_0 - timedelta(days=1)


# ---------- DB ----------
def ensure_table():
    con = duckdb.connect(DB_PATH)
    try:
        con.execute("""
            CREATE TABLE IF NOT EXISTS option_snapshots_raw (
                snapshot_id TEXT,
                timestamp TIMESTAMP,
                symbol TEXT,
                underlying_price DOUBLE,
                strike DOUBLE,
                call_put TEXT,
                days_to_expiry INTEGER,
                expiration_date DATE,
                moneyness_bucket TEXT,
                bid DOUBLE,
                ask DOUBLE,
                mid DOUBLE,
                volume INTEGER,
                open_interest INTEGER,
                iv DOUBLE,
                spread DOUBLE,
                spread_pct DOUBLE,
                time_decay_bucket TEXT
            );
        """)
    finally:
        con.close()


# ---------- SHARDING ----------
def stable_shard(symbol: str, n_shards: int) -> int:
    return zlib.crc32(symbol.encode("utf-8")) % n_shards


# ---------- SMALL UTILS ----------
def chunk_list(xs: list, n: int):
    for i in range(0, len(xs), n):
        yield xs[i:i + n]


def _ensure_utc_col(df: pd.DataFrame, col: str) -> None:
    if df is None or df.empty or col not in df.columns:
        return
    df[col] = pd.to_datetime(df[col], utc=True, errors="coerce")


def get_closest_strike(target: float, strikes: list[float]) -> float:
    if not strikes:
        raise RuntimeError("No strikes available.")
    return float(min(strikes, key=lambda s: abs(float(s) - float(target))))


def is_third_friday(d):
    return d.weekday() == 4 and (15 <= d.day <= 21)


def has_any_weekly_expiration(expirations: list[str]) -> bool:
    # True if any Friday expiry that is NOT the 3rd Friday (i.e., a weekly)
    for exp in expirations:
        d = datetime.strptime(exp, "%Y%m%d").date()
        if d.weekday() == 4 and not is_third_friday(d):
            return True
    return False


def get_friday_within_4_days(expirations: list[str], now_date):
    for exp in sorted(expirations):
        d = datetime.strptime(exp, "%Y%m%d").date()
        if (
            d.weekday() == 4 and
            0 <= (d - now_date).days <= 4 and
            not is_third_friday(d)
        ):
            return exp
    return None


# ---------- UNDERLYING ----------
def fetch_last_days(symbol: str, days: int):
    end = db_end_utc_day()
    start = end - timedelta(days=days)

    df = yf.download(symbol, start=start, end=end, interval="5m", progress=False)
    if df is None or df.empty:
        return df

    # force UTC-aware index (UTC-only pipeline)
    df.index = pd.to_datetime(df.index, utc=True, errors="coerce")

    # market-hours filter (temporary ET), then back to UTC
    df = df.tz_convert("US/Eastern")
    df = df.between_time("09:30", "16:00")
    df = df.tz_convert("UTC")
    return df


# ---------- DEFINITIONS MAP + NEEDED SYMBOLS (FAST) ----------
def build_def_map(df_defs: pd.DataFrame) -> dict[tuple[float, str, datetime.date], str]:
    """
    Map (strike_f, side, exp_date) -> raw_symbol for fast lookup.
    """
    d = df_defs.copy()
    d["strike_f"] = d["strike_price"].astype(float)
    d["exp_date"] = pd.to_datetime(d["expiration"]).dt.date

    out = {}
    for _, r in d.iterrows():
        k = (float(r["strike_f"]), str(r["instrument_class"]), r["exp_date"])
        rs = r.get("raw_symbol", None)
        if rs is not None and rs != "":
            out[k] = str(rs)
    return out


def build_needed_raw_symbols_from_map(
    data_10m: pd.DataFrame,
    def_map: dict[tuple[float, str, datetime.date], str],
    strikes: list[float],
    expirations: list[str],
) -> list[str]:
    """
    Pre-pass over your 10m timestamps using SAME selection logic (ATM/±1.5/±3.5 + weekly expiry within 4 days).
    Uses def_map for fast (strike, side, exp_date) -> raw_symbol.
    """
    if data_10m is None or data_10m.empty:
        return []

    needed: set[str] = set()

    for ts, row in data_10m.iterrows():
        underlying_price = float(row["Close"])
        now_date = ts.date()

        exp = get_friday_within_4_days(expirations, now_date)
        if exp is None:
            continue

        exp_date = datetime.strptime(exp, "%Y%m%d").date()
        dte = (exp_date - now_date).days
        if dte <= 0:
            continue

        atm = get_closest_strike(underlying_price, strikes)
        c1 = get_closest_strike(underlying_price * 1.015, strikes)
        p1 = get_closest_strike(underlying_price * 0.985, strikes)
        c2 = get_closest_strike(underlying_price * 1.035, strikes)
        p2 = get_closest_strike(underlying_price * 0.965, strikes)

        strike_sides = [
            (atm, "C"), (atm, "P"),
            (c1, "C"), (p1, "P"),
            (c2, "C"), (p2, "P"),
        ]

        for strike, side in strike_sides:
            rs = def_map.get((float(strike), side, exp_date))
            if rs:
                needed.add(rs)

    return sorted(needed)


def get_contract_data_from_dfs_fast(
    strike_sides, days_to_expiry, def_map, exp_date, ts, underlying_price,
    mkt_df, trd_df, oi_df
):
    """
    Same output as get_contract_data_from_dfs, but uses def_map instead of df_defs filtering.
    """
    symbols = {}
    for strike, side in strike_sides:
        symbols[(strike, side)] = def_map.get((float(strike), str(side), exp_date))

    mkt_tcol = "ts_event" if "ts_event" in mkt_df.columns else ("timestamp" if "timestamp" in mkt_df.columns else None)
    trd_tcol = "ts_event" if "ts_event" in trd_df.columns else ("timestamp" if "timestamp" in trd_df.columns else None)
    oi_tcol  = "ts_event" if "ts_event" in oi_df.columns else ("timestamp" if "timestamp" in oi_df.columns else None)

    if mkt_tcol: _ensure_utc_col(mkt_df, mkt_tcol)
    if trd_tcol: _ensure_utc_col(trd_df, trd_tcol)
    if oi_tcol:  _ensure_utc_col(oi_df, oi_tcol)

    mkt_groups = dict(tuple(mkt_df.groupby("symbol"))) if not mkt_df.empty else {}
    trd_groups = dict(tuple(trd_df.groupby("symbol"))) if not trd_df.empty else {}
    oi_groups  = dict(tuple(oi_df.groupby("symbol"))) if not oi_df.empty else {}

    out = {}

    for strike, side in strike_sides:
        rs = symbols.get((strike, side))

        bid = ask = mid = spread = spread_pct = iv = None
        volume = 0.0
        open_interest = None

        if rs is None:
            out[(strike, side)] = (bid, ask, mid, open_interest, volume, iv, spread, spread_pct)
            continue

        g_oi = oi_groups.get(rs)
        if g_oi is not None and not g_oi.empty and oi_tcol:
            g_oi2 = g_oi[g_oi[oi_tcol] <= ts]
            if not g_oi2.empty:
                open_interest = g_oi2.iloc[-1].get("open_interest", None)

        g_trd = trd_groups.get(rs)
        if g_trd is not None and not g_trd.empty and "size" in g_trd.columns and trd_tcol:
            mask = (g_trd[trd_tcol] >= ts - pd.Timedelta(minutes=5)) & (g_trd[trd_tcol] <= ts + pd.Timedelta(minutes=5))
            volume = float(g_trd.loc[mask, "size"].sum())
        else:
            volume = 0.0

        g_mkt = mkt_groups.get(rs)
        if g_mkt is None or g_mkt.empty or not mkt_tcol:
            out[(strike, side)] = (bid, ask, mid, open_interest, volume, iv, spread, spread_pct)
            continue

        last_row = g_mkt[g_mkt[mkt_tcol] <= ts].tail(1)
        if last_row.empty:
            out[(strike, side)] = (bid, ask, mid, open_interest, volume, iv, spread, spread_pct)
            continue
        last = last_row.iloc[0]

        bid = float(last.get("bid_px", 0) or 0)
        ask = float(last.get("ask_px", 0) or 0)

        if bid and ask:
            mid = (bid + ask) / 2.0
            spread = ask - bid
            spread_pct = spread / mid if mid else None

        # IV solve (bisection)
        T = days_to_expiry / 365.0
        if mid and T > 0:
            S = float(underlying_price)
            K = float(strike)
            r = 0.01
            lo, hi = 1e-6, 5.0

            def N(x):
                return 0.5 * (1 + math.erf(x / math.sqrt(2)))

            def bs_price(sigma):
                d1 = (math.log(S / K) + (r + 0.5 * sigma**2) * T) / (sigma * math.sqrt(T))
                d2 = d1 - sigma * math.sqrt(T)
                if side == "C":
                    return S * N(d1) - K * math.exp(-r * T) * N(d2)
                else:
                    return K * math.exp(-r * T) * N(-d2) - S * N(-d1)

            for _ in range(60):
                mid_sigma = 0.5 * (lo + hi)
                if bs_price(mid_sigma) > mid:
                    hi = mid_sigma
                else:
                    lo = mid_sigma

            iv = 0.5 * (lo + hi)

        out[(strike, side)] = (bid, ask, mid, open_interest, volume, iv, spread, spread_pct)

    return out


# ---------- BATCH DOWNLOAD ----------
def _to_iso(x) -> str:
    return pd.Timestamp(x).to_pydatetime().isoformat()


def batch_get_df(
    dataset: str,
    schema: str,
    symbols: list[str],
    start,
    end,
    *,
    stype_in: str,
    split_duration: str = "day",
    poll_s: float = 2.0,
) -> pd.DataFrame:
    """
    submit batch job -> wait -> download -> load dbn -> to_df
    """
    if not symbols:
        return pd.DataFrame()

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
    log(f"[BATCH] submit_job ok | schema={schema} symbols={len(symbols)} job_id={job_id}")

    while True:
        done_ids = set(map(operator.itemgetter("id"), client.batch.list_jobs("done")))
        if job_id in done_ids:
            break
        time.sleep(poll_s)

    out_dir = BATCH_DIR / job_id
    out_dir.mkdir(parents=True, exist_ok=True)
    files = client.batch.download(job_id=job_id, output_dir=out_dir)
    log(f"[BATCH] download done | schema={schema} files={len(files)} out_dir={out_dir}")

    dfs = []
    for f in sorted(files):
        if str(f).endswith(".dbn.zst"):
            store = db.DBNStore.from_file(f)
            dfs.append(store.to_df())

    df = pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()
    log(f"[BATCH] loaded df | schema={schema} rows={len(df):,} cols={list(df.columns)[:10]}{'...' if len(df.columns)>10 else ''}")
    return df


def batch_get_df_chunked(
    dataset: str,
    schema: str,
    symbols: list[str],
    start,
    end,
    *,
    stype_in: str,
    split_duration: str = "day",
    poll_s: float = 2.0,
) -> pd.DataFrame:
    """
    Minimal chunking:
    - If <= 2000 symbols: exactly one batch job
    - If > 2000: run ceil(n/2000) jobs, then concat
    """
    if not symbols:
        return pd.DataFrame()

    if len(symbols) <= MAX_SYMBOLS_PER_JOB:
        return batch_get_df(
            dataset=dataset,
            schema=schema,
            symbols=symbols,
            start=start,
            end=end,
            stype_in=stype_in,
            split_duration=split_duration,
            poll_s=poll_s,
        )

    n_chunks = (len(symbols) + MAX_SYMBOLS_PER_JOB - 1) // MAX_SYMBOLS_PER_JOB
    log(f"[BATCH] schema={schema} too many symbols ({len(symbols)}). Splitting into {n_chunks} chunk(s) of {MAX_SYMBOLS_PER_JOB}...")

    parts = []
    for idx, sym_chunk in enumerate(chunk_list(symbols, MAX_SYMBOLS_PER_JOB), start=1):
        log(f"[BATCH] schema={schema} chunk {idx}/{n_chunks}: symbols={len(sym_chunk)}")
        df_part = batch_get_df(
            dataset=dataset,
            schema=schema,
            symbols=sym_chunk,
            start=start,
            end=end,
            stype_in=stype_in,
            split_duration=split_duration,
            poll_s=poll_s,
        )
        parts.append(df_part)

    df = pd.concat(parts, ignore_index=True) if parts else pd.DataFrame()
    log(f"[BATCH] schema={schema} combined rows={len(df):,}")
    return df


# ---------- HELPERS ----------
def append_row(
    results, snapshot_id, ts, symbol, underlying_price, days_till_expiry, exp_date,
    time_decay_bucket, strike, bucket, side, bid, ask, mid, vol, oi, iv, spread, spread_pct
):
    results.append({
        "snapshot_id": snapshot_id,
        "timestamp": ts,
        "symbol": symbol,
        "underlying_price": underlying_price,
        "strike": strike,
        "call_put": side,
        "days_to_expiry": days_till_expiry,
        "expiration_date": exp_date,
        "moneyness_bucket": bucket,
        "bid": bid,
        "ask": ask,
        "mid": mid,
        "volume": int(vol) if vol is not None else 0,
        "open_interest": int(oi) if oi is not None else None,
        "iv": iv,
        "spread": spread,
        "spread_pct": spread_pct,
        "time_decay_bucket": time_decay_bucket,
    })


# ---------- DEFINITIONS BATCHING HELPERS ----------
def detect_parent_col(df_defs: pd.DataFrame) -> str:
    """
    We requested stype_in='parent'. The returned definition df usually carries the parent/root in a column.
    Try common candidates; fall back to 'symbol' if it looks like 'AAPL.OPT'.
    """
    cols = set(df_defs.columns)

    for c in ["parent", "underlying", "root", "sym_root", "ticker"]:
        if c in cols:
            return c

    if "symbol" in cols:
        sample = df_defs["symbol"].dropna().astype(str).head(50).tolist()
        if any(s.endswith(".OPT") for s in sample):
            return "symbol"

    raise RuntimeError(f"Cannot find parent column in definition df. cols={list(df_defs.columns)}")


def parent_to_underlying(parent_val: str) -> str:
    s = str(parent_val)
    if s.endswith(".OPT"):
        return s[:-4]
    return s


# ---------- ✅ SHARD TWO-PHASE (defs batched; data batched) ----------
def run_shard_two_phase(
    symbols: list[str],
    days_back: int,
    *,
    max_symbols: int | None = None,
    max_plan_symbols: int | None = None,
    max_hours: int | None = None,
    dry_run: bool = False,
    debug_dump: bool = False,
):
    DEBUG_DIR.mkdir(parents=True, exist_ok=True)
    BATCH_DIR.mkdir(parents=True, exist_ok=True)

    end = db_end_utc_day()
    start = end - timedelta(days=days_back)

    log(f"[PARAM] days_back={days_back} start={start.isoformat()} end={end.isoformat()}")
    log(f"[PARAM] DEF_CHUNK_SIZE={DEF_CHUNK_SIZE} MAX_SYMBOLS_PER_JOB={MAX_SYMBOLS_PER_JOB}")
    log(f"[PARAM] max_symbols={max_symbols} max_plan_symbols={max_plan_symbols} max_hours={max_hours} dry_run={dry_run} debug_dump={debug_dump}")

    # Apply test cap
    symbols_in = list(symbols)
    if max_symbols is not None:
        symbols_in = symbols_in[:max_symbols]

    # ---------- PHASE 1A: underlying per symbol (still per-symbol; unavoidable) ----------
    under_10m: dict[str, pd.DataFrame] = {}

    counters = {
        "sym_total_in": len(symbols_in),
        "sym_under_ok": 0,
        "sym_under_skip": 0,
        "sym_defs_ok": 0,
        "sym_defs_skip": 0,
        "sym_weekly_skip": 0,
        "sym_raw_needed_skip": 0,
        "sym_planned": 0,
        "union_raw": 0,
        "rows_inserted": 0,
    }

    with Timer("PHASE1A underlying fetch"):
        for i, symbol in enumerate(symbols_in, start=1):
            with Timer(f"underlying {symbol} ({i}/{len(symbols_in)})"):
                data = fetch_last_days(symbol, days_back)
                if data is None or data.empty:
                    counters["sym_under_skip"] += 1
                    log(f"⏭️ {symbol}: no underlying data")
                    continue

                if isinstance(data.columns, pd.MultiIndex):
                    data.columns = data.columns.get_level_values(0)

                data.index = pd.to_datetime(data.index, utc=True, errors="coerce")
                data_10m = data.resample("10min").last().dropna()
                if data_10m.empty:
                    counters["sym_under_skip"] += 1
                    log(f"⏭️ {symbol}: no 10m bars")
                    continue

                if max_hours is not None:
                    # keep only the first N hourly groups (test mode)
                    # we slice by time range: first N hours from min timestamp
                    t0 = data_10m.index.min()
                    t1 = t0 + pd.Timedelta(hours=max_hours)
                    data_10m = data_10m[(data_10m.index >= t0) & (data_10m.index < t1)]
                    log(f"[TEST] {symbol}: trunc 10m bars to first {max_hours} hour(s) | bars={len(data_10m)}")

                under_10m[symbol] = data_10m
                counters["sym_under_ok"] += 1
                log(f"[UNDER] {symbol}: 10m_bars={len(data_10m):,} from {data_10m.index.min()} to {data_10m.index.max()}")

    if not under_10m:
        log("[STOP] no symbols with valid underlying data")
        log(f"[SUMMARY] {counters}")
        return

    eligible_syms = sorted(under_10m.keys())
    log(f"[INFO] underlying ok: {len(eligible_syms)} symbol(s). batching definitions...")

    # ---------- PHASE 1B: definitions in chunks; build plans + union raw_symbols ----------
    plans: dict[str, dict] = {}
    union_raw: set[str] = set()

    if max_plan_symbols is not None:
        eligible_syms = eligible_syms[:max_plan_symbols]
        log(f"[TEST] limiting planning to max_plan_symbols={max_plan_symbols} -> {len(eligible_syms)} symbol(s)")

    with Timer("PHASE1B definitions + planning"):
        for chunk_idx, sym_chunk in enumerate(chunk_list(eligible_syms, DEF_CHUNK_SIZE), start=1):
            parents = [f"{s}.OPT" for s in sym_chunk]
            log(f"[DEFS] chunk {chunk_idx}: parents={len(parents)} sample={parents[:5]}")

            try:
                with Timer(f"defs get_range chunk {chunk_idx}"):
                    defs = client.timeseries.get_range(
                        dataset="OPRA.PILLAR",
                        schema="definition",
                        stype_in="parent",
                        symbols=parents,
                        start=start,
                        end=end - timedelta(hours=24),
                    )
                    df_defs_all = defs.to_df()
            except Exception as e:
                log(f"❌ defs chunk error chunk={chunk_idx} size={len(sym_chunk)}: {e}")
                log(traceback.format_exc())
                continue

            if df_defs_all is None or df_defs_all.empty:
                log(f"[DEFS] chunk {chunk_idx}: EMPTY df")
                continue

            parent_col = detect_parent_col(df_defs_all)
            log(f"[DEFS] chunk {chunk_idx}: rows={len(df_defs_all):,} parent_col='{parent_col}' cols={list(df_defs_all.columns)[:12]}{'...' if len(df_defs_all.columns)>12 else ''}")

            # group by underlying/parent
            for parent_val, g in df_defs_all.groupby(parent_col):
                sym = parent_to_underlying(parent_val)

                if sym not in under_10m:
                    continue

                df_defs = g.copy()
                if df_defs.empty:
                    counters["sym_defs_skip"] += 1
                    log(f"⏭️ {sym}: no options definitions")
                    continue

                counters["sym_defs_ok"] += 1

                strikes = df_defs["strike_price"].dropna().astype(float).unique().tolist()
                strikes.sort()

                expirations = pd.to_datetime(df_defs["expiration"]).dt.strftime("%Y%m%d").dropna().unique().tolist()
                log(f"[DEFS] {sym}: defs_rows={len(df_defs):,} strikes={len(strikes):,} expirations={len(expirations):,}")

                if not has_any_weekly_expiration(expirations):
                    counters["sym_weekly_skip"] += 1
                    log(f"⏭️ {sym}: only monthly expirations -> skip")
                    continue

                def_map = build_def_map(df_defs)

                with Timer(f"plan raw_needed {sym}"):
                    raw_needed = build_needed_raw_symbols_from_map(
                        data_10m=under_10m[sym],
                        def_map=def_map,
                        strikes=strikes,
                        expirations=expirations,
                    )

                if not raw_needed:
                    counters["sym_raw_needed_skip"] += 1
                    log(f"⏭️ {sym}: no needed raw_symbols produced")
                    continue

                plans[sym] = {
                    "data_10m": under_10m[sym],
                    "strikes": strikes,
                    "expirations": expirations,
                    "def_map": def_map,
                    "raw_needed": set(raw_needed),
                }
                union_raw.update(raw_needed)
                counters["sym_planned"] += 1
                log(f"[PLAN] {sym}: raw_needed={len(raw_needed):,} union_raw_now={len(union_raw):,}")

    if not plans or not union_raw:
        log("[STOP] nothing to do (no plans / no raw symbols).")
        log(f"[SUMMARY] {counters}")
        return

    union_raw_list = sorted(union_raw)
    counters["union_raw"] = len(union_raw_list)
    log(f"[INFO] shard union raw_symbols={len(union_raw_list):,} -> ONE batch per schema")
    log(f"[INFO] union_raw sample={union_raw_list[:10]}")

    # ---------- PHASE 2: ONE batch per schema ----------
    with Timer("PHASE2 batch downloads"):
        with Timer("batch cbbo-1m"):
            mkt_df_all = batch_get_df_chunked(
                dataset="OPRA.PILLAR",
                schema="cbbo-1m",
                symbols=union_raw_list,
                start=start,
                end=end,
                stype_in="raw_symbol",
                split_duration="day",
                poll_s=POLL_S,
            )

        with Timer("batch trades"):
            trd_df_all = batch_get_df_chunked(
                dataset="OPRA.PILLAR",
                schema="trades",
                symbols=union_raw_list,
                start=start,
                end=end,
                stype_in="raw_symbol",
                split_duration="day",
                poll_s=POLL_S,
            )

        with Timer("batch statistics"):
            oi_df_all = batch_get_df_chunked(
                dataset="OPRA.PILLAR",
                schema="statistics",
                symbols=union_raw_list,
                start=start - pd.Timedelta(days=1),
                end=end,
                stype_in="raw_symbol",
                split_duration="day",
                poll_s=POLL_S,
            )

    # timestamp cols
    mkt_tcol = "ts_event" if "ts_event" in mkt_df_all.columns else ("timestamp" if "timestamp" in mkt_df_all.columns else None)
    trd_tcol = "ts_event" if "ts_event" in trd_df_all.columns else ("timestamp" if "timestamp" in trd_df_all.columns else None)
    oi_tcol  = "ts_event" if "ts_event" in oi_df_all.columns else ("timestamp" if "timestamp" in oi_df_all.columns else None)

    log(f"[TSCOLS] mkt_tcol={mkt_tcol} trd_tcol={trd_tcol} oi_tcol={oi_tcol}")

    if mkt_tcol: _ensure_utc_col(mkt_df_all, mkt_tcol)
    if trd_tcol: _ensure_utc_col(trd_df_all, trd_tcol)
    if oi_tcol:  _ensure_utc_col(oi_df_all, oi_tcol)

    if debug_dump:
        log("[DUMP] writing big dfs to debug_dumps/")
        # keep dumps smaller in test mode if needed
        mkt_df_all.head(200000).to_csv(DEBUG_DIR / "mkt_df_all_head.csv", index=False)
        trd_df_all.head(200000).to_csv(DEBUG_DIR / "trd_df_all_head.csv", index=False)
        oi_df_all.head(200000).to_csv(DEBUG_DIR / "oi_df_all_head.csv", index=False)

    # ---------- PHASE 3: per symbol compute using global dfs ----------
    with Timer("PHASE3 compute + insert"):
        for si, (symbol, plan) in enumerate(plans.items(), start=1):
            data_10m = plan["data_10m"]
            strikes = plan["strikes"]
            expirations = plan["expirations"]
            def_map = plan["def_map"]
            raw_needed = plan["raw_needed"]

            log(f"[SYMBOL] {symbol} ({si}/{len(plans)}) | 10m_bars={len(data_10m):,} raw_needed={len(raw_needed):,}")

            results = []
            sym_rows_before = 0

            for window_idx, (window_start, window_df) in enumerate(data_10m.groupby(pd.Grouper(freq="1h")), start=1):
                if window_df.empty:
                    continue
                window_end = window_start + pd.Timedelta(hours=1)

                per_ts = {}
                for ts, row in window_df.iterrows():
                    underlying_price = float(row["Close"])

                    atm_strike = get_closest_strike(underlying_price, strikes)
                    c1 = get_closest_strike(underlying_price * 1.015, strikes)
                    p1 = get_closest_strike(underlying_price * 0.985, strikes)
                    c2 = get_closest_strike(underlying_price * 1.035, strikes)
                    p2 = get_closest_strike(underlying_price * 0.965, strikes)

                    now_date = ts.date()
                    exp = get_friday_within_4_days(expirations, now_date)
                    if exp is None:
                        continue

                    exp_date = datetime.strptime(exp, "%Y%m%d").date()
                    days_till_expiry = (exp_date - now_date).days
                    if days_till_expiry <= 0:
                        continue

                    strike_sides = [
                        (atm_strike, "C"), (atm_strike, "P"),
                        (c1, "C"), (p1, "P"),
                        (c2, "C"), (p2, "P"),
                    ]

                    per_ts[ts] = (underlying_price, days_till_expiry, exp_date, strike_sides)

                if not per_ts:
                    continue

                # filter GLOBAL dfs down to this symbol's raw_needed AND this hour
                with Timer(f"{symbol} window {window_idx} filter dfs"):
                    if mkt_tcol and not mkt_df_all.empty:
                        mkt_df_win = mkt_df_all[
                            (mkt_df_all[mkt_tcol] >= window_start) &
                            (mkt_df_all[mkt_tcol] < window_end) &
                            (mkt_df_all["symbol"].isin(raw_needed))
                        ].copy()
                    else:
                        mkt_df_win = pd.DataFrame()

                    if trd_tcol and not trd_df_all.empty:
                        trd_df_win = trd_df_all[
                            (trd_df_all[trd_tcol] >= window_start) &
                            (trd_df_all[trd_tcol] < window_end) &
                            (trd_df_all["symbol"].isin(raw_needed))
                        ].copy()
                    else:
                        trd_df_win = pd.DataFrame()

                    if oi_tcol and not oi_df_all.empty:
                        oi_start = window_start - pd.Timedelta(days=1)
                        oi_df_win = oi_df_all[
                            (oi_df_all[oi_tcol] >= oi_start) &
                            (oi_df_all[oi_tcol] < window_end) &
                            (oi_df_all["symbol"].isin(raw_needed))
                        ].copy()
                    else:
                        oi_df_win = pd.DataFrame()

                    log(
                        f"[WIN] {symbol} {window_start}..{window_end} | "
                        f"mkt_rows={len(mkt_df_win):,} trd_rows={len(trd_df_win):,} oi_rows={len(oi_df_win):,} per_ts={len(per_ts):,}"
                    )

                with Timer(f"{symbol} window {window_idx} compute ts"):
                    for ts, (underlying_price, days_till_expiry, exp_date, strike_sides) in per_ts.items():
                        if days_till_expiry <= 1:
                            time_decay_bucket = "EXTREME"
                        elif days_till_expiry <= 3:
                            time_decay_bucket = "HIGH"
                        elif days_till_expiry <= 7:
                            time_decay_bucket = "MEDIUM"
                        else:
                            time_decay_bucket = "LOW"

                        ts_utc = pd.Timestamp(ts).tz_convert("UTC")
                        snapshot_id = f"{symbol}_{ts_utc.strftime('%Y-%m-%dT%H:%M:%SZ')}"

                        out = get_contract_data_from_dfs_fast(
                            strike_sides,
                            days_till_expiry,
                            def_map,
                            exp_date,
                            ts,
                            underlying_price,
                            mkt_df_win,
                            trd_df_win,
                            oi_df_win,
                        )

                        for (strike, side) in strike_sides:
                            bid, ask, mid, oi, vol, iv, spread, spread_pct = out.get(
                                (strike, side), (None, None, None, None, 0.0, None, None, None)
                            )

                            if strike == strike_sides[0][0]:
                                bucket = "ATM"
                            elif strike in (strike_sides[2][0], strike_sides[3][0]):
                                bucket = "OTM_1"
                            else:
                                bucket = "OTM_2"

                            append_row(
                                results,
                                snapshot_id, ts, symbol, underlying_price,
                                days_till_expiry, exp_date, time_decay_bucket,
                                strike, bucket, side,
                                bid, ask, mid, vol, oi, iv, spread, spread_pct
                            )

                # progress per hour
                if len(results) - sym_rows_before > 0:
                    log(f"[PROG] {symbol} window {window_idx}: +{len(results)-sym_rows_before:,} rows (total {len(results):,})")
                    sym_rows_before = len(results)

            if not results:
                log(f"⏭️ {symbol}: no rows produced")
                continue

            df = pd.DataFrame(results)
            df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True, errors="coerce").dt.tz_convert("UTC").dt.tz_localize(None)

            if dry_run:
                log(f"[DRYRUN] {symbol}: would insert {len(df):,} rows (skipping DB insert)")
                if debug_dump:
                    df.head(50000).to_csv(DEBUG_DIR / f"{symbol}_rows_head.csv", index=False)
                continue

            with Timer(f"DB insert {symbol} rows={len(df):,}"):
                con = duckdb.connect(DB_PATH)
                try:
                    con.register("df_view", df)
                    cols = ",".join(df.columns)
                    con.execute(f"INSERT INTO option_snapshots_raw({cols}) SELECT {cols} FROM df_view")
                    con.unregister("df_view")
                finally:
                    con.close()

            counters["rows_inserted"] += len(df)
            log(f"✅ {symbol}: inserted {len(df):,} rows | cumulative_inserted={counters['rows_inserted']:,}")

    log(f"[SUMMARY] {counters}")


# ---------- MAIN ----------
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--shard-id", type=int, required=True)
    parser.add_argument("--n-shards", type=int, required=True)
    parser.add_argument("--days-back", type=int, default=35)

    # TEST knobs
    parser.add_argument("--max-symbols", type=int, default=None, help="limit symbols processed (after sharding)")
    parser.add_argument("--max-plan-symbols", type=int, default=None, help="limit symbols that proceed into defs/planning")
    parser.add_argument("--max-hours", type=int, default=None, help="limit underlying data to first N hours")
    parser.add_argument("--dry-run", action="store_true", help="do everything except DB insert")
    parser.add_argument("--debug-dump", action="store_true", help="write CSV debug dumps to debug_dumps/")

    args = parser.parse_args()

    ensure_table()

    # 🔹 Manual symbol universe (short list)
    symbols = [
        "AAPL",
        "MSFT",
        "NVDA",
        "AMD",
        "TSLA",
    ]

    symbols = [s.strip().upper() for s in symbols if s and isinstance(s, str)]

    # Sharding still works the same
    my_symbols = [s for s in symbols if stable_shard(s, args.n_shards) == args.shard_id]

    log(f"[BOOT] shard={args.shard_id}/{args.n_shards} symbols_in_shard={len(my_symbols)} days_back={args.days_back}")
    log(f"[BOOT] first10={my_symbols[:10]}")

    run_shard_two_phase(
        my_symbols,
        days_back=args.days_back,
        max_symbols=args.max_symbols,
        max_plan_symbols=args.max_plan_symbols,
        max_hours=args.max_hours,
        dry_run=args.dry_run,
        debug_dump=args.debug_dump,
    )

if __name__ == "__main__":
    main()
