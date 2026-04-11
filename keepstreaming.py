# live_ingest_latest.py
# One file, two modes:
#   1) streamer: 24/7 single-writer that maintains latest state in LIVE DuckDB
#   2) snapshot: read-only job that reads LIVE DuckDB and writes Parquet outputs
#
# Streamer is kept lean:
#   - NO z-scores / NO parquet / NO IV / NO OI cache / NO dbfunctions except get_sp500_symbols
#   - Maintains ONE table: live_contract_latest (metadata + quote + rolling vol + optional OI)

from __future__ import annotations

import time
import argparse
import datetime as dt
import datetime
import pytz
from collections import deque

import pandas as pd
import databento as db
import duckdb

from config import DATABENTO_API_KEY


# -------------------------
# CONFIG
# -------------------------
SUB_BATCH_SIZE = 200
SUB_SLEEP_SEC = 0.25

VOL_WINDOW_SEC = 10 * 60
FLUSH_EVERY_SEC = 1.0

# universe refresh cadence (reload raw_symbol cache file, subscribe new)
REFRESH_SEC = 10 * 60

RESTART_HOUR_UTC = 24
RESTART_MIN_UTC = 10

DUCKDB_LIVE_PATH = "live_state.duckdb"

# raw_symbol universe cache written by universe_builder (newline-delimited)
RAW_UNIVERSE_CACHE_PATH = "state/raw_universe_cache.txt"
UNIVERSE_CACHE_DB_PATH = "state/universe_cache.duckdb"
UNIVERSE_TABLE = "universe_targets"

# snapshot freshness gates (used by snapshot mode; harmless to keep here)
STRIKES_MAX_AGE_MIN = 20


import duckdb
import databento as db
import time


con = duckdb.connect("rawsymbols.db")

raw_symbol_list = [
    row[0]
    for row in con.execute("""
        SELECT DISTINCT raw_option_symbol
        FROM raw_symbols
        WHERE raw_option_symbol IS NOT NULL
        ORDER BY raw_option_symbol
    """).fetchall()
]

con.close()

live = db.Live(key=DATABENTO_API_KEY)

batch_size = 200
for i in range(0, len(raw_symbol_list), batch_size):
    batch = raw_symbol_list[i:i + batch_size]
    live.subscribe(
        dataset="OPRA.PILLAR",
        schema="cbbo-1m",
        symbols=batch,
        stype_in="raw_symbol",
    )
    live.subscribe(
        dataset="OPRA.PILLAR",
        schema="trades",
        symbols=batch,
        stype_in="raw_symbol",
    )
    time.sleep(0.25)

live.start()

for rec in live:
    print(rec.rtype)


    






# -------------------------
# DUCKDB (LIVE) TABLE (SINGLE)
# -------------------------
DDL_LIVE_CONTRACT_LATEST = """
CREATE TABLE IF NOT EXISTS live_contract_latest (
    raw_symbol       TEXT PRIMARY KEY,

    -- contract identity / routing (from universe cache)
    parent_symbol    TEXT,
    label            TEXT,         -- ATM, C1, P1, C2, P2
    instrument_class TEXT,         -- C or P
    exp_yyyymmdd     TEXT,
    expiration_date  DATE,
    strike_price     DOUBLE,
    underlying_price DOUBLE,
    ts_refresh       TIMESTAMP,

    -- latest quote (from cbbo-1m)
    ts_quote   TIMESTAMP,
    bid        DOUBLE,
    ask        DOUBLE,
    mid        DOUBLE,
    spread     DOUBLE,
    spread_pct DOUBLE,

    -- rolling volume (from trades)
    ts_vol     TIMESTAMP,
    vol10m     BIGINT,

    -- open interest (optional; if/when you stream it)
    oi_date        DATE,
    ts_oi          TIMESTAMP,
    open_interest  BIGINT
);
"""


def init_live_duckdb():
    con = duckdb.connect(DUCKDB_LIVE_PATH)
    con.execute(DDL_LIVE_CONTRACT_LATEST)
    con.close()


# -------------------------
# HELPERS
# -------------------------
def chunks(lst, n):
    for i in range(0, len(lst), n):
        yield lst[i : i + n]


def utc_now_naive() -> datetime.datetime:
    return dt.datetime.now(tz=pytz.UTC).replace(tzinfo=None)


def to_utc_naive(ts) -> datetime.datetime | None:
    if ts is None:
        return None
    try:
        t = pd.to_datetime(ts, utc=True, errors="coerce")
        if pd.isna(t):
            return None
        return t.to_pydatetime().replace(tzinfo=None)
    except Exception:
        return None


def compute_quote_fields(bid: float | None, ask: float | None):
    if bid is not None and ask is not None and bid > 0 and ask > 0:
        mid = (bid + ask) / 2.0
        spread = ask - bid
        spread_pct = (spread / mid) if mid > 0 else None
        return mid, spread, spread_pct
    return None, None, None


def is_symbol_mapping_msg(rec) -> bool:
    # SymbolMappingMsg can be identified by class name or rtype suffix.
    if type(rec).__name__ == "SymbolMappingMsg":
        return True
    rtype = getattr(rec, "rtype", None)
    return bool(rtype is not None and str(rtype).endswith("SYMBOL_MAPPING"))


def maybe_update_symbol_mapping(rec, inst_to_raw: dict[int, str]) -> bool:
    if not is_symbol_mapping_msg(rec):
        return False
    inst = getattr(rec, "instrument_id", None)
    sym = getattr(rec, "stype_in_symbol", None) or getattr(rec, "stype_out_symbol", None)
    if inst is not None and sym:
        try:
            inst_to_raw[int(inst)] = str(sym)
        except Exception:
            pass
    return True


def resolve_raw_symbol(rec, inst_to_raw: dict[int, str]) -> str | None:
    # Live market records are usually keyed by instrument_id; map that to raw_symbol.
    inst = getattr(rec, "instrument_id", None)
    if inst is not None:
        try:
            raw = inst_to_raw.get(int(inst))
            if raw:
                return raw
        except Exception:
            pass
    # Fallback for records that already expose symbol/raw_symbol.
    raw = getattr(rec, "symbol", None) or getattr(rec, "raw_symbol", None)
    return str(raw) if raw else None


def extract_bid_ask(rec) -> tuple[float | None, float | None]:
    # Databento Python record interface can expose BBO levels via levels[0].
    levels = getattr(rec, "levels", None)
    if levels is not None:
        try:
            lvl0 = levels[0]
        except Exception:
            lvl0 = None
        if lvl0 is not None:
            bid = getattr(lvl0, "pretty_bid_px", None)
            ask = getattr(lvl0, "pretty_ask_px", None)
            if bid is None and ask is None:
                bid = getattr(lvl0, "bid_px", None)
                ask = getattr(lvl0, "ask_px", None)
            if bid is not None or ask is not None:
                return bid, ask

    # Backward-compatible fallback to flattened top-of-book fields.
    bid = getattr(rec, "pretty_bid_px_00", None)
    ask = getattr(rec, "pretty_ask_px_00", None)
    if bid is None and ask is None:
        bid = getattr(rec, "bid_px_00", None)
        ask = getattr(rec, "ask_px_00", None)
    return bid, ask


def load_universe_cache_db(
    db_path: str = UNIVERSE_CACHE_DB_PATH,
    table: str = UNIVERSE_TABLE,
) -> tuple[list[str], list[tuple]]:
    try:
        con = duckdb.connect(db_path, read_only=True)
    except Exception:
        return [], []

    try:
        df = con.execute(
            f"""
            SELECT
                parent_symbol,
                label,
                instrument_class,
                exp_yyyymmdd,
                expiration_date,
                strike_price,
                raw_symbol,
                underlying_price,
                ts_refresh
            FROM {table}
            ORDER BY parent_symbol, label, instrument_class
            """
        ).fetchdf()
    except Exception:
        con.close()
        return [], []

    con.close()

    if df is None or df.empty:
        return [], []

    raws: list[str] = []
    strike_rows: list[tuple] = []
    seen_raw = set()
    seen_key = set()

    for _, r in df.iterrows():
        parent = str(r["parent_symbol"])
        label = str(r["label"])
        cp = str(r["instrument_class"])
        exp_yyyymmdd = str(r["exp_yyyymmdd"])

        try:
            strike = float(r["strike_price"])
        except Exception:
            continue

        raw = str(r["raw_symbol"]) if pd.notna(r["raw_symbol"]) else None
        if not raw:
            continue

        try:
            underlying_px = float(r["underlying_price"])
        except Exception:
            underlying_px = None

        expiration_date = r["expiration_date"] if pd.notna(r["expiration_date"]) else None
        ts_refresh = to_utc_naive(r["ts_refresh"]) or utc_now_naive()

        if raw not in seen_raw:
            seen_raw.add(raw)
            raws.append(raw)

        key = (parent, label, cp)
        if key in seen_key:
            continue
        seen_key.add(key)

        strike_rows.append(
            (
                parent,
                label,
                cp,
                exp_yyyymmdd,
                expiration_date,
                strike,
                raw,
                underlying_px,
                ts_refresh,
            )
        )

    return raws, strike_rows


def load_universe_cache(path: str):
    """
    Expected per line (space-separated):
      parent_symbol label instrument_class exp_yyyymmdd strike_price root_symbol raw_symbol underlying_price ts_refresh...

    Example:
      BAC ATM C 20260227 52.0 BAC 260227C00052000 51.89 2026-02-26 17:55:30.757915
    """
    try:
        raws: list[str] = []
        strike_rows: list[tuple] = []
        seen_raw = set()
        seen_key = set()  # (parent, label, cp)

        with open(path, "r", encoding="utf-8") as f:
            for ln in f:
                ln = ln.strip()
                if not ln:
                    continue

                parts = ln.split()
                # Need at least up to underlying_price; ts may be 2 tokens (date time)
                if len(parts) < 8:
                    continue

                parent = parts[0]
                label = parts[1]
                cp = parts[2]  # "C" or "P"
                exp_yyyymmdd = parts[3]

                # strike
                try:
                    strike = float(parts[4])
                except Exception:
                    continue

                # Supports both:
                #   new format: parent label cp exp strike raw underlying ts...
                #   legacy format: parent label cp exp strike root short_raw underlying ts...
                raw = None
                underlying_px = None
                ts_idx = None

                # New format first.
                if len(parts) >= 8:
                    try:
                        raw = parts[5]
                        underlying_px = float(parts[6])
                        ts_idx = 7
                    except Exception:
                        raw = None

                # Legacy fallback.
                if raw is None and len(parts) >= 9:
                    try:
                        raw = parts[6]
                        underlying_px = float(parts[7])
                        ts_idx = 8
                    except Exception:
                        raw = None

                if raw is None:
                    continue

                # ts_refresh: if present, parse; else set now
                ts_refresh = utc_now_naive()
                if ts_idx is not None:
                    if len(parts) >= ts_idx + 2:
                        ts_refresh = to_utc_naive(parts[ts_idx] + " " + parts[ts_idx + 1]) or ts_refresh
                    elif len(parts) >= ts_idx + 1:
                        ts_refresh = to_utc_naive(parts[ts_idx]) or ts_refresh

                # expiration_date from exp_yyyymmdd
                expiration_date = None
                try:
                    expiration_date = dt.datetime.strptime(exp_yyyymmdd, "%Y%m%d").date()
                except Exception:
                    expiration_date = None

                if raw and raw not in seen_raw:
                    seen_raw.add(raw)
                    raws.append(raw)

                key = (parent, label, cp)
                if key in seen_key:
                    continue
                seen_key.add(key)

                # row aligns with upsert_meta VALUES order below
                strike_rows.append(
                    (
                        parent,
                        label,
                        cp,
                        exp_yyyymmdd,
                        expiration_date,
                        strike,
                        raw,
                        underlying_px,
                        ts_refresh,
                    )
                )

        return raws, strike_rows

    except FileNotFoundError:
        return [], []
    except Exception:
        return [], []


def load_universe() -> tuple[list[str], list[tuple], str]:
    raws, strike_rows = load_universe_cache_db()
    if raws or strike_rows:
        return raws, strike_rows, "duckdb"

    raws, strike_rows = load_universe_cache(RAW_UNIVERSE_CACHE_PATH)
    return raws, strike_rows, "text"


# -------------------------
# STREAMER: SUBSCRIBE + STREAM -> UPSERT LIVE TABLE
# -------------------------
def subscribe_raws(live: db.Live, raws: list[str]):
    for batch in chunks(raws, SUB_BATCH_SIZE):
        live.subscribe(dataset="OPRA.PILLAR", schema="cbbo-1m", symbols=batch, stype_in="raw_symbol")
        live.subscribe(dataset="OPRA.PILLAR", schema="trades", symbols=batch, stype_in="raw_symbol")
        time.sleep(SUB_SLEEP_SEC)


def stream_to_duckdb_latest(initial_raw_symbols: list[str], initial_strike_rows: list[tuple]):
    quote_latest: dict[str, dict] = {}
    vol_deques: dict[str, deque] = {}
    vol_latest: dict[str, dict] = {}
    inst_to_raw: dict[int, str] = {}

    con = duckdb.connect(DUCKDB_LIVE_PATH)

    upsert_meta = """
    INSERT INTO live_contract_latest (
      parent_symbol, label, instrument_class,
      exp_yyyymmdd, expiration_date, strike_price,
      raw_symbol, underlying_price, ts_refresh
    )
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT(raw_symbol) DO UPDATE SET
      parent_symbol=excluded.parent_symbol,
      label=excluded.label,
      instrument_class=excluded.instrument_class,
      exp_yyyymmdd=excluded.exp_yyyymmdd,
      expiration_date=excluded.expiration_date,
      strike_price=excluded.strike_price,
      underlying_price=excluded.underlying_price,
      ts_refresh=excluded.ts_refresh;
    """

    upsert_quote = """
    INSERT INTO live_contract_latest (raw_symbol, ts_quote, bid, ask, mid, spread, spread_pct)
    VALUES (?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT(raw_symbol) DO UPDATE SET
      ts_quote=excluded.ts_quote,
      bid=excluded.bid,
      ask=excluded.ask,
      mid=excluded.mid,
      spread=excluded.spread,
      spread_pct=excluded.spread_pct;
    """

    upsert_vol = """
    INSERT INTO live_contract_latest (raw_symbol, ts_vol, vol10m)
    VALUES (?, ?, ?)
    ON CONFLICT(raw_symbol) DO UPDATE SET
      ts_vol=excluded.ts_vol,
      vol10m=excluded.vol10m;
    """

    clear_old_labels = """
    UPDATE live_contract_latest
    SET
      parent_symbol=NULL,
      label=NULL,
      instrument_class=NULL,
      exp_yyyymmdd=NULL,
      expiration_date=NULL,
      strike_price=NULL,
      underlying_price=NULL,
      ts_refresh=NULL
    WHERE parent_symbol = ?
      AND label IN ('ATM','C1','P1','C2','P2')
      AND instrument_class IN ('C','P');
    """

    if initial_strike_rows:
        con.executemany(upsert_meta, initial_strike_rows)

    live = db.Live(key=DATABENTO_API_KEY)

    # Use callback queue (Databento Live has no next_record)
    record_q = deque(maxlen=200_000)

    def _cb(rec):
        record_q.append(rec)

    def _cb_exc(exc: Exception):
        print(f"[DATABENTO_CALLBACK_EXCEPTION] {exc}")

    live.add_callback(record_callback=_cb, exception_callback=_cb_exc)

    subscribed: set[str] = set()
    last_refresh = 0.0
    last_flush = time.time()

    print("Subscribing initial raw_symbols:", len(initial_raw_symbols))
    if initial_raw_symbols:
        subscribe_raws(live, initial_raw_symbols)
        subscribed.update(initial_raw_symbols)
    else:
        print(f"WARNING: initial_raw_symbols empty (cache={RAW_UNIVERSE_CACHE_PATH}).")

    live.start()
    print("Live streaming started. Ctrl+C to stop.")

    try:
        while True:
            now_utc = dt.datetime.now(dt.timezone.utc)
            if (
                now_utc.hour > RESTART_HOUR_UTC
                or (now_utc.hour == RESTART_HOUR_UTC and now_utc.minute >= RESTART_MIN_UTC)
            ):
                print("Nightly UTC restart time reached. Exiting loop...")
                break

            if (time.time() - last_refresh) >= REFRESH_SEC:
                last_refresh = time.time()

                desired_raws, strike_rows, src = load_universe()
                if strike_rows:
                    parents = sorted({row[0] for row in strike_rows})
                    con.executemany(clear_old_labels, [(p,) for p in parents])
                    con.executemany(upsert_meta, strike_rows)

                if not desired_raws:
                    print(
                        f"Universe cache empty/missing (db={UNIVERSE_CACHE_DB_PATH}, text={RAW_UNIVERSE_CACHE_PATH})"
                    )
                else:
                    to_add = [r for r in desired_raws if r not in subscribed]
                    if to_add:
                        print(f"Subscribing NEW raws from {src} cache: {len(to_add)}")
                        subscribe_raws(live, to_add)
                        subscribed.update(to_add)
                    else:
                        print("No new raws to add from cache.")

            # Pop from callback queue
            rec = record_q.popleft() if record_q else None
            now_sec = time.time()

            if rec is not None:
                if maybe_update_symbol_mapping(rec, inst_to_raw):
                    continue

                raw = resolve_raw_symbol(rec, inst_to_raw)
                if raw:
                    ts_event = to_utc_naive(getattr(rec, "ts_event", None))

                    bid, ask = extract_bid_ask(rec)

                    # Quote record (cbbo-1m)
                    if bid is not None or ask is not None:
                        bid_f = float(bid) if bid is not None and bid > 0 else None
                        ask_f = float(ask) if ask is not None and ask > 0 else None
                        mid, spread, spread_pct = compute_quote_fields(bid_f, ask_f)

                        quote_latest[raw] = {
                            "ts_event": ts_event,
                            "bid": bid_f,
                            "ask": ask_f,
                            "mid": mid,
                            "spread": spread,
                            "spread_pct": spread_pct,
                        }
                    else:
                        # Trade record (trades) -> rolling vol
                        size = getattr(rec, "size", None)
                        if size is not None:
                            try:
                                sz = int(size)
                            except Exception:
                                sz = 0
                            dq = vol_deques.get(raw)
                            if dq is None:
                                dq = deque()
                                vol_deques[raw] = dq
                            dq.append((now_sec, sz))

            cutoff = now_sec - VOL_WINDOW_SEC
            for raw, dq in list(vol_deques.items()):
                while dq and dq[0][0] < cutoff:
                    dq.popleft()
                vol10m = int(sum(sz for _, sz in dq)) if dq else 0
                vol_latest[raw] = {"ts_calc": utc_now_naive(), "vol10m": vol10m}

            if (time.time() - last_flush) >= FLUSH_EVERY_SEC:
                last_flush = time.time()

                if quote_latest:
                    rows = [
                        (
                            raw,
                            q["ts_event"] or utc_now_naive(),
                            q["bid"],
                            q["ask"],
                            q["mid"],
                            q["spread"],
                            q["spread_pct"],
                        )
                        for raw, q in quote_latest.items()
                    ]
                    con.executemany(upsert_quote, rows)

                if vol_latest:
                    rows = [
                        (raw, v["ts_calc"] or utc_now_naive(), int(v["vol10m"]))
                        for raw, v in vol_latest.items()
                    ]
                    con.executemany(upsert_vol, rows)

                print(f"flush quotes={len(quote_latest)} vol={len(vol_latest)} subscribed={len(subscribed)}")

    except KeyboardInterrupt:
        print("Stopping (KeyboardInterrupt)...")
    finally:
        try:
            live.stop()
        except Exception:
            pass
        try:
            con.close()
        except Exception:
            pass


# -------------------------
# SNAPSHOT MODE (placeholder)
# -------------------------


# -------------------------
# MAIN
# -------------------------
def main():
    init_live_duckdb()

    raw_symbols, strike_rows, src = load_universe()
    print(f"Initial raw_symbols from {src} cache:", len(raw_symbols))

    stream_to_duckdb_latest(raw_symbols, strike_rows)


if __name__ == "__main__":
    main()
