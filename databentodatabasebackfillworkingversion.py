import databento as db
import yfinance as yf
import pandas as pd
from datetime import datetime, timedelta, timezone
from config import DATABENTO_API_KEY
import math
import duckdb
import argparse
import zlib
import time
import operator
import pathlib
import shutil


from databasefunctions import get_sp500_symbols
from policy.expiration import (
    LOOKAHEAD_DAYS_DEFAULT,
    find_first_eligible_friday,
    has_any_eligible_weekly_friday,
    is_third_friday as policy_is_third_friday,
)
from policy.strikes import build_strike_map

client = db.Historical(DATABENTO_API_KEY)

DB_PATH = "options_data.db"
BATCH_DIR = pathlib.Path("batch_downloads")

# Databento batch symbol cap (hard limit)
MAX_SYMBOLS_PER_JOB = 2000
POLL_S = 2.0

# Definitions batching (practical safe chunk)
DEF_CHUNK_SIZE = 100
# --- add these right under your existing constants (NO OTHER CHANGES) ---
QUOTE_LOOKBACK = pd.Timedelta(hours=1)
MAX_QUOTE_AGE  = pd.Timedelta(hours=1)



# ---------- TIME RANGE (Databento-safe end boundary) ----------
def db_end_utc_day() -> datetime:
    """
    Databento historical often seals availability at 00:00:00 UTC boundaries.
    Using 'now()' can exceed the available end and trigger 422.
    Clamp end to the start of the current UTC day.
    """
    return datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)


def clamp_end(dataset: str, end: datetime) -> datetime:
    """
    Clamp requested `end` to Databento's actual available dataset end to prevent 422.
    """
    rng = client.metadata.get_dataset_range(dataset)
    avail_end = pd.Timestamp(rng["end"]).to_pydatetime()
    if avail_end.tzinfo is None:
        avail_end = avail_end.replace(tzinfo=timezone.utc)
    if end.tzinfo is None:
        end = end.replace(tzinfo=timezone.utc)
    return min(end, avail_end)


# ---------- DB ----------
def ensure_table():
    con = duckdb.connect(DB_PATH)
    try:
        con.execute("""
            CREATE TABLE IF NOT EXISTS option_snapshots_raw (
                timestamp TIMESTAMP,
                parent_symbol TEXT,
                underlying_price DOUBLE,
                strike DOUBLE,
                side TEXT,
                days_to_expiry INTEGER,
                expiration_date DATE,
                grouping TEXT,
                mid DOUBLE,
                iv DOUBLE,
                time_decay_bucket TEXT
            );
        """)
        con.execute("""
            CREATE TABLE IF NOT EXISTS rolling_volume_history (
                timestamp TIMESTAMP,
                parent_symbol TEXT,
                side TEXT,
                days_to_expiry INTEGER,
                grouping TEXT,
                rolling_volume_10m INTEGER,
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
    return policy_is_third_friday(d)


def has_any_weekly_expiration(expirations: list[str]) -> bool:
    return has_any_eligible_weekly_friday(expirations, exclude_third_friday=True)


def get_friday_within_4_days(expirations: list[str], now_date):
    return find_first_eligible_friday(
        expirations,
        now_date,
        lookahead_days=LOOKAHEAD_DAYS_DEFAULT,
        exclude_third_friday=True,
    )


# ---------- UNDERLYING ----------
def fetch_last_days(symbol: str, days: int):
    end = db_end_utc_day()
    start = end - timedelta(days=days)

    df = yf.download(symbol, start=start, end=end, interval="1m", progress=False)
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
    data_1m: pd.DataFrame,
    def_map: dict[tuple[float, str, datetime.date], str],
    strikes: list[float],
    expirations: list[str],
) -> list[str]:
    """
    Pre-pass over your intraday timestamps using the same selection logic.
    Uses def_map for fast (strike, side, exp_date) -> raw_symbol.
    """
    if data_1m is None or data_1m.empty:
        return []

    needed: set[str] = set()
    daily_leg_map = build_daily_leg_map(data_1m, strikes, expirations)

    for exp_date, _dte, strike_sides in daily_leg_map.values():
        for strike, side in strike_sides:
            rs = def_map.get((float(strike), side, exp_date))
            if rs:
                needed.add(rs)

    return sorted(needed)


def build_daily_leg_map(
    data_1m: pd.DataFrame,
    strikes: list[float],
    expirations: list[str],
) -> dict[datetime.date, tuple[datetime.date, int, list[tuple[float, str]]]]:
    if data_1m is None or data_1m.empty:
        return {}

    daily_leg_map = {}

    for ts, row in data_1m.iterrows():
        now_date = ts.date()
        if now_date in daily_leg_map:
            continue

        underlying_price = float(row["Close"])
        exp = get_friday_within_4_days(expirations, now_date)
        if exp is None:
            continue

        exp_date = datetime.strptime(exp, "%Y%m%d").date()
        dte = (exp_date - now_date).days
        if dte <= 0:
            continue

        strike_map = build_strike_map(float(underlying_price), strikes)
        atm = strike_map["ATM"]
        c1 = strike_map["C1"]
        p1 = strike_map["P1"]
        c2 = strike_map["C2"]
        p2 = strike_map["P2"]

        strike_sides = [
            (atm, "C"), (atm, "P"),
            (c1, "C"), (p1, "P"),
            (c2, "C"), (p2, "P"),
        ]
        daily_leg_map[now_date] = (exp_date, dte, strike_sides)

    return daily_leg_map


def get_contract_data_from_dfs_fast(
    strike_sides, days_to_expiry, def_map, exp_date, ts, underlying_price,
    mkt_df, trd_df, oi_df
):
    """
    Same output as get_contract_data_from_dfs, but uses def_map instead of df_defs filtering.
    """
    # --- ensure ts is UTC-aware (NO OTHER CHANGES) ---
    ts = pd.Timestamp(ts)
    if ts.tzinfo is None:
        ts = ts.tz_localize("UTC")
    else:
        ts = ts.tz_convert("UTC")

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
                open_interest = g_oi2.loc[g_oi2[oi_tcol].idxmax()].get("open_interest", None)

        g_trd = trd_groups.get(rs)
        if g_trd is not None and not g_trd.empty and "size" in g_trd.columns and trd_tcol:
            mask = (g_trd[trd_tcol] >= ts - pd.Timedelta(minutes=10)) & (g_trd[trd_tcol] <= ts)
            volume = float(g_trd.loc[mask, "size"].sum())
        else:
            volume = 0.0

        g_mkt = mkt_groups.get(rs)
        if g_mkt is None or g_mkt.empty or not mkt_tcol:
            out[(strike, side)] = (bid, ask, mid, open_interest, volume, iv, spread, spread_pct)
            continue

        g_mkt2 = g_mkt[g_mkt[mkt_tcol] <= ts]
        if g_mkt2.empty:
            out[(strike, side)] = (bid, ask, mid, open_interest, volume, iv, spread, spread_pct)
            continue

        # ✅ pick the row with the latest timestamp (works even if g_mkt2 is unsorted)
        last = g_mkt2.loc[g_mkt2[mkt_tcol].idxmax()]

        bid = float(last.get("bid_px_00", 0) or 0)
        ask = float(last.get("ask_px_00", 0) or 0)

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

    while True:
        done_ids = set(map(operator.itemgetter("id"), client.batch.list_jobs("done")))
        if job_id in done_ids:
            break
        time.sleep(poll_s)

    out_dir = BATCH_DIR / job_id
    out_dir.mkdir(parents=True, exist_ok=True)
    files = client.batch.download(job_id=job_id, output_dir=out_dir)

    dfs = []
    for f in sorted(files):
        if str(f).endswith(".dbn.zst"):
            store = db.DBNStore.from_file(f)
            dfs.append(store.to_df())

    return pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()


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
    print(f"[BATCH] symbol list too large ({len(symbols)}). Splitting into {n_chunks} chunk(s) of {MAX_SYMBOLS_PER_JOB}...")

    parts = []
    for idx, sym_chunk in enumerate(chunk_list(symbols, MAX_SYMBOLS_PER_JOB), start=1):
        print(f"[BATCH] chunk {idx}/{n_chunks}: symbols={len(sym_chunk)}")
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

    return pd.concat(parts, ignore_index=True) if parts else pd.DataFrame()


# ---------- HELPERS ----------
def append_row(
    results, ts, parent_symbol, underlying_price, strike, side, days_till_expiry,
    exp_date, grouping, mid, iv, time_decay_bucket
):
    results.append({
        "timestamp": ts,
        "parent_symbol": parent_symbol,
        "underlying_price": underlying_price,
        "strike": strike,
        "side": side,
        "days_to_expiry": days_till_expiry,
        "expiration_date": exp_date,
        "grouping": grouping,
        "mid": mid,
        "iv": iv,
        "time_decay_bucket": time_decay_bucket,
    })


def append_volume_row(
    results, ts, parent_symbol, side, days_till_expiry,
    grouping, rolling_volume_10m, time_decay_bucket
):
    results.append({
        "timestamp": ts,
        "parent_symbol": parent_symbol,
        "side": side,
        "days_to_expiry": days_till_expiry,
        "grouping": grouping,
        "rolling_volume_10m": int(rolling_volume_10m) if rolling_volume_10m is not None else 0,
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
        # If values look like "AAPL.OPT" (because stype_in='parent'), use that.
        sample = df_defs["symbol"].dropna().astype(str).head(20).tolist()
        if any(s.endswith(".OPT") for s in sample):
            return "symbol"

    raise RuntimeError(f"Cannot find parent column in definition df. cols={list(df_defs.columns)}")


def parent_to_underlying(parent_val: str) -> str:
    s = str(parent_val)
    # parent often "AAPL.OPT"
    if s.endswith(".OPT"):
        return s[:-4]
    # sometimes might be just "AAPL"
    return s


# ---------- ✅ SHARD TWO-PHASE (defs batched; data batched) ----------
def run_shard_two_phase(symbols: list[str], days_back: int = 35):
    end = clamp_end("OPRA.PILLAR", db_end_utc_day())
    start = end - timedelta(days=days_back)

    # ---------- PHASE 1A: underlying per symbol (still per-symbol; unavoidable) ----------
    under_1m: dict[str, pd.DataFrame] = {}

    for symbol in symbols:
        data = fetch_last_days(symbol, days_back)
        if data is None or data.empty:
            print(f"⏭️ {symbol}: no underlying data")
            continue

        if isinstance(data.columns, pd.MultiIndex):
            data.columns = data.columns.get_level_values(0)

        data.index = pd.to_datetime(data.index, utc=True, errors="coerce")
        data_1m = data.resample("1min").last().dropna()
        if data_1m.empty:
            print(f"⏭️ {symbol}: no 1m bars")
            continue

        under_1m[symbol] = data_1m

    if not under_1m:
        print("[INFO] no symbols with valid underlying data")
        return

    eligible_syms = sorted(under_1m.keys())
    print(f"[INFO] underlying ok: {len(eligible_syms)} symbol(s). batching definitions...")

    # ---------- PHASE 1B: definitions in chunks; build plans + union raw_symbols ----------
    plans: dict[str, dict] = {}
    union_raw: set[str] = set()

    for sym_chunk in chunk_list(eligible_syms, DEF_CHUNK_SIZE):
        parents = [f"{s}.OPT" for s in sym_chunk]

        try:
            defs = client.timeseries.get_range(
                dataset="OPRA.PILLAR",
                schema="definition",
                stype_in="parent",
                symbols=parents,
                start=start,
                end=end - timedelta(hours=48),
            )
            df_defs_all = defs.to_df()
        except Exception as e:
            print(f"❌ defs chunk error (size={len(sym_chunk)}): {e}")
            # degrade strategy: retry smaller chunks
            if len(sym_chunk) > 1:
                half = max(1, len(sym_chunk) // 2)
                print(f"[DEGRADE] retrying in chunks of {half}")
                for sub in chunk_list(sym_chunk, half):
                    parents2 = [f"{s}.OPT" for s in sub]
                    try:
                        defs2 = client.timeseries.get_range(
                            dataset="OPRA.PILLAR",
                            schema="definition",
                            stype_in="parent",
                            symbols=parents2,
                            start=start,
                            end=end - timedelta(hours=24),
                        )
                        df2 = defs2.to_df()
                        if df2 is None or df2.empty:
                            continue
                        parent_col = detect_parent_col(df2)
                        for parent_val, g in df2.groupby(parent_col):
                            sym = parent_to_underlying(parent_val)
                            if sym not in under_1m:
                                continue
                            df_defs = g.copy()
                            if df_defs.empty:
                                continue

                            strikes = df_defs["strike_price"].dropna().astype(float).unique().tolist()
                            strikes.sort()

                            expirations = pd.to_datetime(df_defs["expiration"]).dt.strftime("%Y%m%d").dropna().unique().tolist()
                            if not has_any_weekly_expiration(expirations):
                                print(f"⏭️ {sym}: only monthly expirations -> skip")
                                continue

                            def_map = build_def_map(df_defs)

                            raw_needed = build_needed_raw_symbols_from_map(
                                data_1m=under_1m[sym],
                                def_map=def_map,
                                strikes=strikes,
                                expirations=expirations,
                            )
                            if not raw_needed:
                                print(f"⏭️ {sym}: no needed raw_symbols produced")
                                continue

                            plans[sym] = {
                                "data_1m": under_1m[sym],
                                "strikes": strikes,
                                "expirations": expirations,
                                "def_map": def_map,
                                "raw_needed": set(raw_needed),
                            }
                            union_raw.update(raw_needed)
                            print(f"[PLAN] {sym}: raw_needed={len(raw_needed):,}")
                    except Exception as e2:
                        print(f"❌ defs subchunk error (size={len(sub)}): {e2}")
            continue

        if df_defs_all is None or df_defs_all.empty:
            continue

        parent_col = detect_parent_col(df_defs_all)

        for parent_val, g in df_defs_all.groupby(parent_col):
            sym = parent_to_underlying(parent_val)

            if sym not in under_1m:
                continue

            df_defs = g.copy()
            if df_defs.empty:
                print(f"⏭️ {sym}: no options definitions")
                continue

            strikes = df_defs["strike_price"].dropna().astype(float).unique().tolist()
            strikes.sort()

            expirations = pd.to_datetime(df_defs["expiration"]).dt.strftime("%Y%m%d").dropna().unique().tolist()
            if not has_any_weekly_expiration(expirations):
                print(f"⏭️ {sym}: only monthly expirations -> skip")
                continue

            def_map = build_def_map(df_defs)

            raw_needed = build_needed_raw_symbols_from_map(
                data_1m=under_1m[sym],
                def_map=def_map,
                strikes=strikes,
                expirations=expirations,
            )
            if not raw_needed:
                print(f"⏭️ {sym}: no needed raw_symbols produced")
                continue

            plans[sym] = {
                "data_1m": under_1m[sym],
                "strikes": strikes,
                "expirations": expirations,
                "def_map": def_map,
                "raw_needed": set(raw_needed),
            }
            union_raw.update(raw_needed)
            print(f"[PLAN] {sym}: raw_needed={len(raw_needed):,}")

    if not plans or not union_raw:
        print("[INFO] nothing to do (no plans / no raw symbols).")
        return

    union_raw_list = sorted(union_raw)
    print(f"[INFO] shard union raw_symbols={len(union_raw_list):,} -> ONE batch per schema")

    # ---------- PHASE 2: ONE batch per schema ----------
    end_batch = end - timedelta(hours=48)

    mkt_df_all = batch_get_df_chunked(
        dataset="OPRA.PILLAR",
        schema="cbbo-1s",
        symbols=union_raw_list,
        start=start,
        end=end_batch,
        stype_in="raw_symbol",
        split_duration="day",
        poll_s=POLL_S,
    )

    print("\n[DEBUG CBBO COLUMNS]")
    print(sorted(mkt_df_all.columns))
    print("[DEBUG SAMPLE ROWS]")
    print(mkt_df_all.head(5))
    print("[DEBUG NONZERO BIDS]")
    print(mkt_df_all[mkt_df_all["bid_px_00"] > 0].head(5))
    print()
    print("[DEBUG NONZERO BIDS]")
    if "bid_px_00" in mkt_df_all.columns:
        print(mkt_df_all[mkt_df_all["bid_px_00"] > 0].head(5))
    elif "bid_px" in mkt_df_all.columns:
        print(mkt_df_all[mkt_df_all["bid_px"] > 0].head(5))
    else:
        print("[DEBUG] no bid_px column found")
    print()
    trd_df_all = batch_get_df_chunked(
        dataset="OPRA.PILLAR",
        schema="trades",
        symbols=union_raw_list,
        start=start,
        end=end_batch,
        stype_in="raw_symbol",
        split_duration="day",
        poll_s=POLL_S,
    )

    oi_df_all = batch_get_df_chunked(
        dataset="OPRA.PILLAR",
        schema="statistics",
        symbols=union_raw_list,
        start=start - pd.Timedelta(days=1),
        end=end_batch,
        stype_in="raw_symbol",
        split_duration="day",
        poll_s=POLL_S,
    )


    if oi_df_all is None or oi_df_all.empty:
        pass
    else:
        if "stat_type" in oi_df_all.columns:
            oi_df_all = oi_df_all[oi_df_all["stat_type"] == db.StatType.OPEN_INTEREST].copy()

        if "quantity" in oi_df_all.columns and "open_interest" not in oi_df_all.columns:
            oi_df_all = oi_df_all.rename(columns={"quantity": "open_interest"})

        keep_cols = [c for c in ["symbol", "ts_event", "timestamp", "open_interest"] if c in oi_df_all.columns]
        oi_df_all = oi_df_all[keep_cols].copy()
    # normalize timestamp cols once
    mkt_tcol = "ts_event" if "ts_event" in mkt_df_all.columns else ("timestamp" if "timestamp" in mkt_df_all.columns else None)
    trd_tcol = "ts_event" if "ts_event" in trd_df_all.columns else ("timestamp" if "timestamp" in trd_df_all.columns else None)
    oi_tcol  = "ts_event" if "ts_event" in oi_df_all.columns else ("timestamp" if "timestamp" in oi_df_all.columns else None)

    if mkt_tcol: _ensure_utc_col(mkt_df_all, mkt_tcol)
    if trd_tcol: _ensure_utc_col(trd_df_all, trd_tcol)
    if oi_tcol:  _ensure_utc_col(oi_df_all, oi_tcol)

    # ---------- PHASE 3: per symbol compute using global dfs ----------
    for symbol, plan in plans.items():
        data_1m = plan["data_1m"]
        strikes = plan["strikes"]
        expirations = plan["expirations"]
        def_map = plan["def_map"]
        raw_needed = plan["raw_needed"]
        daily_leg_map = build_daily_leg_map(data_1m, strikes, expirations)

        snapshot_results = []
        volume_results = []

        for window_start, window_df in data_1m.groupby(pd.Grouper(freq="1h")):
            if window_df.empty:
                continue
            window_end = window_start + pd.Timedelta(hours=1)

            per_ts = {}
            for ts, row in window_df.iterrows():
                underlying_price = float(row["Close"])
                daily_legs = daily_leg_map.get(ts.date())
                if daily_legs is None:
                    continue

                exp_date, days_till_expiry, strike_sides = daily_legs
                per_ts[ts] = (underlying_price, days_till_expiry, exp_date, strike_sides)

            if not per_ts:
                continue

            # filter GLOBAL dfs down to this symbol's raw_needed AND this hour
            # CHANGE: include lookback so "latest <= ts" has a chance to exist
            if mkt_tcol and not mkt_df_all.empty:
                win_start_mkt = window_start - QUOTE_LOOKBACK
                win_end_mkt   = window_end
                mkt_df_win = mkt_df_all[
                    (mkt_df_all[mkt_tcol] >= win_start_mkt) &
                    (mkt_df_all[mkt_tcol] <  win_end_mkt) &
                    (mkt_df_all["symbol"].isin(raw_needed))
                ].copy()
            else:
                mkt_df_win = pd.DataFrame()

            if trd_tcol and not trd_df_all.empty:
                win_start_trd = window_start - pd.Timedelta(minutes=10)
                trd_df_win = trd_df_all[
                    (trd_df_all[trd_tcol] >= win_start_trd) &
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

            for ts, (underlying_price, days_till_expiry, exp_date, strike_sides) in per_ts.items():
                if days_till_expiry <= 1:
                    time_decay_bucket = "EXTREME"
                elif days_till_expiry <= 3:
                    time_decay_bucket = "HIGH"
                elif days_till_expiry <= 7:
                    time_decay_bucket = "MEDIUM"
                else:
                    time_decay_bucket = "LOW"

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
                        bucket = "OTM1"
                    else:
                        bucket = "OTM2"

                    append_row(
                        snapshot_results,
                        ts, symbol, underlying_price, strike, side,
                        days_till_expiry, exp_date, bucket, mid, iv,
                        time_decay_bucket
                    )
                    append_volume_row(
                        volume_results,
                        ts, symbol, side, days_till_expiry, bucket, vol,
                        time_decay_bucket
                    )

        if not snapshot_results and not volume_results:
            print(f"⏭️ {symbol}: no rows produced")
            continue

        snapshot_df = pd.DataFrame(snapshot_results)
        volume_df = pd.DataFrame(volume_results)

        for df in [snapshot_df, volume_df]:
            if not df.empty:
                df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True, errors="coerce").dt.tz_convert("UTC").dt.tz_localize(None)

        con = duckdb.connect(DB_PATH)
        try:
            if not snapshot_df.empty:
                con.register("snapshot_view", snapshot_df)
                snapshot_cols = ",".join(snapshot_df.columns)
                con.execute(f"INSERT INTO option_snapshots_raw({snapshot_cols}) SELECT {snapshot_cols} FROM snapshot_view")
                con.unregister("snapshot_view")

            if not volume_df.empty:
                con.register("volume_view", volume_df)
                volume_cols = ",".join(volume_df.columns)
                con.execute(f"INSERT INTO rolling_volume_history({volume_cols}) SELECT {volume_cols} FROM volume_view")
                con.unregister("volume_view")
        finally:
            con.close()

        print(f"✅ {symbol}: inserted {len(snapshot_df):,} snapshot rows and {len(volume_df):,} volume rows")





def wipe_batch_downloads():
    if BATCH_DIR.name != "batch_downloads":
        raise RuntimeError(f"Refusing to wipe unexpected dir: {BATCH_DIR}")
    if BATCH_DIR.exists():
        shutil.rmtree(BATCH_DIR, ignore_errors=True)
    BATCH_DIR.mkdir(parents=True, exist_ok=True)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--days-back", type=int, default=35)
    args = parser.parse_args()

    ensure_table()

    symbols = get_sp500_symbols()
    symbols = [s.strip().upper() for s in symbols if s and isinstance(s, str)]

    print(f"[INFO] symbols={len(symbols)} days_back={args.days_back}")

    for i, sym in enumerate(symbols, start=1):
        print(f"\n[RUN] {i}/{len(symbols)} symbol={sym}")
        try:
            run_shard_two_phase([sym], days_back=args.days_back)
        except Exception as e:
            print(f"[ERROR] symbol={sym} crashed:")
            print(" ", repr(e))
            import traceback
            print(traceback.format_exc())
            print("[CONTINUE] moving to next symbol...")
        finally:
            wipe_batch_downloads()

def main1():
    parser = argparse.ArgumentParser()
    parser.add_argument("--days-back", type=int, default=35)
    args = parser.parse_args()

    ensure_table()

    # symbols = get_sp500_symbols()
    # symbols = [s.strip().upper() for s in symbols if s and isinstance(s, str)]

    symbols = ["AAPL"]   # <-- ONLY APPLE

    print(f"[INFO] symbols={len(symbols)} days_back={args.days_back}")

    for i, sym in enumerate(symbols, start=1):
        print(f"\n[RUN] {i}/{len(symbols)} symbol={sym}")
        try:
            run_shard_two_phase([sym], days_back=args.days_back)
        except Exception as e:
            print(f"[ERROR] symbol={sym} crashed:")
            print(" ", repr(e))
            import traceback
            print(traceback.format_exc())
            print("[CONTINUE] moving to next symbol...")
        finally:
            wipe_batch_downloads()



if __name__ == "__main__":
    main()
