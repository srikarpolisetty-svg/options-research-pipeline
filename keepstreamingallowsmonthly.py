# live_ingest_latest.py
# One file, two modes:
#   1) streamer: 24/7 single-writer that maintains latest state in LIVE DuckDB
#   2) snapshot: read-only job that reads LIVE DuckDB and writes Parquet outputs
#
# Streamer is kept lean:
#   - NO z-scores / NO parquet / NO IV / NO OI cache / NO dbfunctions except get_sp500_symbols
#   - Only maintains: live_quotes_latest, live_vol10m_latest, live_strikes6_latest, live_oi_latest,
#                     live_sixpack_ready view

from __future__ import annotations

import time
import argparse
import datetime as dt
import datetime
import pytz
from collections import deque

import pandas as pd
import yfinance as yf
import databento as db
import duckdb

from databasefunctions import get_sp500_symbols
from config import DATABENTO_API_KEY
from policy.expiration import LOOKAHEAD_DAYS_DEFAULT, find_first_eligible_friday
from policy.strikes import build_strike_map


# -------------------------
# CONFIG
# -------------------------
BATCH_SIZE_YF = 40
SLEEP_SEC_YF = 5
INTERVAL_YF = "15m"

RESTART_HOUR_UTC = 21
RESTART_MIN_UTC = 10

SUB_BATCH_SIZE = 200
SUB_SLEEP_SEC = 0.25

VOL_WINDOW_SEC = 10 * 60
FLUSH_EVERY_SEC = 1.0

# latest 6-strike set refresh cadence
REFRESH_SEC = 10 * 60

# daily OI fetch (only once per day per raw_symbol)
OI_FETCH_BATCH = 200
OI_FETCH_SLEEP_SEC = 0.25

DUCKDB_LIVE_PATH = "live_state.duckdb"

# snapshot freshness gates (used by snapshot mode; harmless to keep here)
STRIKES_MAX_AGE_MIN = 20


# -------------------------
# DUCKDB (LIVE) TABLES
# -------------------------
DDL_LIVE_QUOTES = """
CREATE TABLE IF NOT EXISTS live_quotes_latest (
    raw_symbol TEXT PRIMARY KEY,
    ts_event   TIMESTAMP,
    bid        DOUBLE,
    ask        DOUBLE,
    mid        DOUBLE,
    spread     DOUBLE,
    spread_pct DOUBLE
);
"""

DDL_LIVE_VOL = """
CREATE TABLE IF NOT EXISTS live_vol10m_latest (
    raw_symbol TEXT PRIMARY KEY,
    ts_calc    TIMESTAMP,
    vol10m     BIGINT
);
"""

DDL_LIVE_STRIKES6 = """
CREATE TABLE IF NOT EXISTS live_strikes6_latest (
    parent_symbol    TEXT,
    label            TEXT,       -- ATM, C1, P1, C2, P2
    instrument_class TEXT,       -- C or P
    exp_yyyymmdd     TEXT,
    expiration_date  DATE,
    strike_price     DOUBLE,
    raw_symbol       TEXT,
    underlying_price DOUBLE,
    ts_refresh       TIMESTAMP,
    PRIMARY KEY(parent_symbol, label, instrument_class)
);
"""

DDL_LIVE_OI = """
CREATE TABLE IF NOT EXISTS live_oi_latest (
    raw_symbol     TEXT PRIMARY KEY,
    oi_date        DATE,        -- as-of date for OI (typically yesterday UTC)
    ts_oi          TIMESTAMP,
    open_interest  BIGINT
);
"""

DDL_VIEW_SIXPACK_READY = """
CREATE VIEW IF NOT EXISTS live_sixpack_ready AS
SELECT
  s.parent_symbol AS symbol,
  s.label,
  s.instrument_class,
  s.exp_yyyymmdd,
  s.expiration_date,
  s.strike_price,
  s.raw_symbol,
  s.underlying_price,
  s.ts_refresh,

  q.ts_event,
  q.bid, q.ask, q.mid, q.spread, q.spread_pct,

  v.ts_calc,
  v.vol10m,

  o.oi_date,
  o.ts_oi,
  o.open_interest
FROM live_strikes6_latest s
LEFT JOIN live_quotes_latest q ON q.raw_symbol = s.raw_symbol
LEFT JOIN live_vol10m_latest v ON v.raw_symbol = s.raw_symbol
LEFT JOIN live_oi_latest o ON o.raw_symbol = s.raw_symbol;
"""


def init_live_duckdb():
    con = duckdb.connect(DUCKDB_LIVE_PATH)
    con.execute(DDL_LIVE_QUOTES)
    con.execute(DDL_LIVE_VOL)
    con.execute(DDL_LIVE_STRIKES6)
    con.execute(DDL_LIVE_OI)
    con.execute(DDL_VIEW_SIXPACK_READY)
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


def get_friday_within_4_days(expirations: list[str], today_utc: datetime.date) -> str | None:
    return find_first_eligible_friday(
        expirations,
        today_utc,
        lookahead_days=LOOKAHEAD_DAYS_DEFAULT,
        exclude_third_friday=False,
    )


def pick_strikes(underlying_price: float, strikes: list[float]) -> dict:
    return build_strike_map(float(underlying_price), strikes)


def pick_raw_symbol(sub: pd.DataFrame, strike: float, cp: str) -> str | None:
    tmp = sub[
        (sub["strike_price"].astype(float) == float(strike))
        & (sub["instrument_class"] == cp)
    ]["raw_symbol"]
    if tmp.empty:
        return None
    return tmp.iloc[0]


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


# -------------------------
# OI HELPERS (daily, once per raw_symbol per day)
# -------------------------
def oi_asof_date_utc() -> datetime.date:
    # OI is end-of-day, so "as of yesterday" in UTC
    return dt.datetime.now(tz=pytz.UTC).date() - dt.timedelta(days=1)


def _df_symbol_col(df: pd.DataFrame) -> str | None:
    for c in ("raw_symbol", "symbol"):
        if c in df.columns:
            return c
    return None


def fetch_open_interest_batch(
    hist: db.Historical,
    raw_symbols: list[str],
    asof_date: datetime.date,
) -> dict[str, int | None]:
    """
    raw_symbol -> open_interest (or None).
    Includes missing raws as None (so we can upsert NULL and not retry until tomorrow).
    """
    out: dict[str, int | None] = {r: None for r in raw_symbols}

    df = hist.timeseries.get_range(
        dataset="OPRA.PILLAR",
        schema="statistics",
        symbols=raw_symbols,
        stype_in="raw_symbol",
        start=asof_date,
        end=asof_date,
    ).to_df()

    if df is None or df.empty:
        return out

    if "stat_type" in df.columns:
        df = df[df["stat_type"] == db.StatType.OPEN_INTEREST]
    if df.empty:
        return out

    sym_col = _df_symbol_col(df)
    if sym_col is None or "quantity" not in df.columns:
        return out

    if "ts_event" in df.columns:
        df = df.sort_values("ts_event")

    last = df.groupby(sym_col, as_index=False).tail(1)

    for _, row in last.iterrows():
        raw = row.get(sym_col)
        qty = row.get("quantity", None)
        try:
            if raw is not None and pd.notna(qty):
                out[str(raw)] = int(qty)
        except Exception:
            pass

    return out


def update_daily_oi_if_needed(
    con: duckdb.DuckDBPyConnection,
    hist: db.Historical,
    raw_symbols: list[str],
) -> int:
    """
    Ensures OI exists for each raw_symbol for today's as-of date.
    Only fetches for raws missing that oi_date.
    Writes NULL OI rows when API returns nothing so we don't retry until tomorrow.
    """
    if not raw_symbols:
        return 0

    asof = oi_asof_date_utc()

    existing_rows = con.execute(
        "SELECT raw_symbol FROM live_oi_latest WHERE oi_date = ?",
        [asof],
    ).fetchall()
    existing = {r[0] for r in existing_rows} if existing_rows else set()

    missing = [r for r in raw_symbols if r not in existing]
    if not missing:
        return 0

    upsert_oi = """
    INSERT INTO live_oi_latest (raw_symbol, oi_date, ts_oi, open_interest)
    VALUES (?, ?, ?, ?)
    ON CONFLICT(raw_symbol) DO UPDATE SET
      oi_date=excluded.oi_date,
      ts_oi=excluded.ts_oi,
      open_interest=excluded.open_interest;
    """

    total_upserted = 0
    for batch in chunks(missing, OI_FETCH_BATCH):
        oi_map = fetch_open_interest_batch(hist, batch, asof_date=asof)
        ts_oi = utc_now_naive()
        rows = [(raw, asof, ts_oi, oi_map.get(raw)) for raw in batch]
        con.executemany(upsert_oi, rows)
        total_upserted += len(rows)
        time.sleep(OI_FETCH_SLEEP_SEC)

    return total_upserted


# -------------------------
# STREAMER: 1) BATCH UNDERLYING PRICES (yfinance)
# -------------------------
def batch_underlying_last_close(
    symbols: list[str],
    start: dt.datetime,
    end: dt.datetime,
) -> dict[str, float]:
    out: dict[str, float] = {}

    for batch in chunks(symbols, BATCH_SIZE_YF):
        try:
            df = yf.download(
                tickers=batch,
                start=start,
                end=end,
                interval=INTERVAL_YF,
                group_by="ticker",
                progress=False,
                auto_adjust=False,
            )
        except Exception:
            time.sleep(15)
            continue

        if df is None or df.empty:
            time.sleep(SLEEP_SEC_YF)
            continue

        close = None
        try:
            close = df.xs("Close", axis=1, level=1)
        except Exception:
            try:
                close = df.xs("Close", axis=1, level=0)
            except Exception:
                time.sleep(SLEEP_SEC_YF)
                continue

        for sym in batch:
            if close is None or sym not in close.columns:
                continue
            s = close[sym].dropna()
            if len(s) == 0:
                continue
            out[sym] = float(s.iloc[-1])

        time.sleep(SLEEP_SEC_YF)

    return out










# -------------------------
# STREAMER: 2) BUILD RAW_SYMBOL UNIVERSE + STRIKES6 ROWS (definition)
# -------------------------
# -------------------------
def build_raw_symbol_universe(
    symbols: list[str],
    underlying_px: dict[str, float],
) -> tuple[list[str], list[tuple], dict[str, dict]]:
    hist = db.Historical(DATABENTO_API_KEY)
    today_utc = dt.datetime.now(tz=pytz.UTC).date()
    ts_refresh = utc_now_naive()

    all_raw_symbols: list[str] = []
    strike_rows: list[tuple] = []
    meta: dict[str, dict] = {}

    for sym in symbols:
        px = underlying_px.get(sym)
        if px is None or px <= 0:
            continue

        # Databento parent symbology fix (remove '-' and '.' e.g. BF-B -> BFB)
        parent_sym = sym.replace("-", "").replace(".", "")

        chain_df = hist.timeseries.get_range(
            dataset="OPRA.PILLAR",
            schema="definition",
            symbols=f"{parent_sym}.OPT",
            stype_in="parent",
            start=today_utc - dt.timedelta(days=1),
            end=today_utc,
        ).to_df()

        if chain_df is None or chain_df.empty:
            continue

        chain_df["exp_yyyymmdd"] = chain_df["expiration"].dt.strftime("%Y%m%d")
        expirations = sorted(chain_df["exp_yyyymmdd"].unique())
        exp = get_friday_within_4_days(expirations, today_utc)
        if exp is None:
            continue

        sub = chain_df[chain_df["exp_yyyymmdd"] == exp]
        if sub.empty:
            continue

        strikes = sorted(sub["strike_price"].astype(float).unique())
        if not strikes:
            continue

        strike_map = pick_strikes(px, strikes)

        atm = strike_map["ATM"]
        c1 = strike_map["C1"]
        p1 = strike_map["P1"]
        c2 = strike_map["C2"]
        p2 = strike_map["P2"]

        raw_atm_c = pick_raw_symbol(sub, atm, "C")
        raw_atm_p = pick_raw_symbol(sub, atm, "P")
        raw_c1 = pick_raw_symbol(sub, c1, "C")
        raw_p1 = pick_raw_symbol(sub, p1, "P")
        raw_c2 = pick_raw_symbol(sub, c2, "C")
        raw_p2 = pick_raw_symbol(sub, p2, "P")

        raws = [raw_atm_c, raw_atm_p, raw_c1, raw_p1, raw_c2, raw_p2]
        if any(x is None for x in raws):
            continue

        exp_date = datetime.datetime.strptime(exp, "%Y%m%d").date()

        strike_rows.extend(
            [
                (sym, "ATM", "C", exp, exp_date, float(atm), raw_atm_c, float(px), ts_refresh),
                (sym, "ATM", "P", exp, exp_date, float(atm), raw_atm_p, float(px), ts_refresh),
                (sym, "C1", "C", exp, exp_date, float(c1), raw_c1, float(px), ts_refresh),
                (sym, "P1", "P", exp, exp_date, float(p1), raw_p1, float(px), ts_refresh),
                (sym, "C2", "C", exp, exp_date, float(c2), raw_c2, float(px), ts_refresh),
                (sym, "P2", "P", exp, exp_date, float(p2), raw_p2, float(px), ts_refresh),
            ]
        )

        all_raw_symbols.extend(raws)
        meta[sym] = {
            "exp": exp,
            "underlying_price": float(px),
            "strike_map": strike_map,
            "raws": raws,
        }

    seen = set()
    uniq = []
    for r in all_raw_symbols:
        if r not in seen:
            seen.add(r)
            uniq.append(r)

    return uniq, strike_rows, meta







# -------------------------
# STREAMER: SUBSCRIBE + STREAM -> UPSERT LIVE TABLES
# -------------------------
def subscribe_raws(live: db.Live, raws: list[str]):
    for batch in chunks(raws, SUB_BATCH_SIZE):
        live.subscribe(dataset="OPRA.PILLAR", schema="mbp-1", symbols=batch, stype_in="raw_symbol")
        live.subscribe(dataset="OPRA.PILLAR", schema="trades", symbols=batch, stype_in="raw_symbol")
        time.sleep(SUB_SLEEP_SEC)


def stream_to_duckdb_latest(initial_raw_symbols: list[str]):
    quote_latest: dict[str, dict] = {}
    vol_deques: dict[str, deque] = {}
    vol_latest: dict[str, dict] = {}
    inst_to_raw: dict[int, str] = {}

    con = duckdb.connect(DUCKDB_LIVE_PATH)

    # one Historical client reused (for daily OI only)
    hist = db.Historical(DATABENTO_API_KEY)

    upsert_quote = """
    INSERT INTO live_quotes_latest (raw_symbol, ts_event, bid, ask, mid, spread, spread_pct)
    VALUES (?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT(raw_symbol) DO UPDATE SET
      ts_event=excluded.ts_event,
      bid=excluded.bid,
      ask=excluded.ask,
      mid=excluded.mid,
      spread=excluded.spread,
      spread_pct=excluded.spread_pct;
    """

    upsert_vol = """
    INSERT INTO live_vol10m_latest (raw_symbol, ts_calc, vol10m)
    VALUES (?, ?, ?)
    ON CONFLICT(raw_symbol) DO UPDATE SET
      ts_calc=excluded.ts_calc,
      vol10m=excluded.vol10m;
    """

    upsert_strikes6 = """
    INSERT INTO live_strikes6_latest (
      parent_symbol, label, instrument_class,
      exp_yyyymmdd, expiration_date, strike_price,
      raw_symbol, underlying_price, ts_refresh
    )
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT(parent_symbol, label, instrument_class) DO UPDATE SET
      exp_yyyymmdd=excluded.exp_yyyymmdd,
      expiration_date=excluded.expiration_date,
      strike_price=excluded.strike_price,
      raw_symbol=excluded.raw_symbol,
      underlying_price=excluded.underlying_price,
      ts_refresh=excluded.ts_refresh;
    """

    live = db.Live(key=DATABENTO_API_KEY)
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
    subscribe_raws(live, initial_raw_symbols)
    subscribed.update(initial_raw_symbols)

    # initial daily OI upsert for the starting universe (only missing)
    n0 = update_daily_oi_if_needed(con, hist, list(subscribed))
    if n0:
        print(f"Initial OI upserts: {n0} (as-of {oi_asof_date_utc()})")

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
                print("Refreshing strikes6 universe...")

                now = dt.datetime.now(tz=pytz.UTC)
                safe_end = now - dt.timedelta(minutes=10)
                safe_start = safe_end - dt.timedelta(minutes=180)

                symbols = sorted(get_sp500_symbols())
                underlying_px = batch_underlying_last_close(symbols, start=safe_start, end=safe_end)
                desired_raws, strike_rows, _meta = build_raw_symbol_universe(symbols, underlying_px)

                if strike_rows:
                    con.executemany(upsert_strikes6, strike_rows)
                    print(f"Upserted strikes6 rows: {len(strike_rows)}")

                # daily OI: only fetch for raws missing today's as-of date
                oi_upserts = update_daily_oi_if_needed(con, hist, desired_raws)
                if oi_upserts:
                    print(f"Upserted OI rows: {oi_upserts} (as-of {oi_asof_date_utc()})")

                to_add = [r for r in desired_raws if r not in subscribed]
                if to_add:
                    print(f"Subscribing NEW raws: {len(to_add)}")
                    subscribe_raws(live, to_add)
                    subscribed.update(to_add)

                    # for newly added raws, ensure OI is present for today (only missing)
                    oi_new = update_daily_oi_if_needed(con, hist, to_add)
                    if oi_new:
                        print(f"Upserted NEW OI rows: {oi_new} (as-of {oi_asof_date_utc()})")
                else:
                    print("No new raws to add.")

            rec = record_q.popleft() if record_q else None
            now_sec = time.time()

            if rec is not None:
                if maybe_update_symbol_mapping(rec, inst_to_raw):
                    continue

                raw = resolve_raw_symbol(rec, inst_to_raw)
                if raw:
                    ts_event = to_utc_naive(getattr(rec, "ts_event", None))

                    # QUOTES (mbp-1)
                    bid, ask = extract_bid_ask(rec)
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

                    # TRADES (volume) — use trade size fields (size or quantity)
                    size = getattr(rec, "size", None)
                    if size is None:
                        size = getattr(rec, "quantity", None)
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

                print(
                    f"flush quotes={len(quote_latest)} vol={len(vol_latest)} subscribed={len(subscribed)}"
                )

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

    now = dt.datetime.now(tz=pytz.UTC)
    safe_end = now - dt.timedelta(minutes=10)
    safe_start = safe_end - dt.timedelta(minutes=180)

    symbols = sorted(get_sp500_symbols())
    print("Total symbols:", len(symbols))

    print("Batch fetching underlying prices (yfinance)...")
    underlying_px = batch_underlying_last_close(symbols, start=safe_start, end=safe_end)
    print("Underlying prices found:", len(underlying_px))

    print("Building raw_symbol universe (Databento definition)...")
    raw_symbols, strike_rows, meta = build_raw_symbol_universe(symbols, underlying_px)
    print("Good underlyings:", len(meta))
    print("Total raw_symbols:", len(raw_symbols))

    if strike_rows:
        con0 = duckdb.connect(DUCKDB_LIVE_PATH)
        upsert_strikes6 = """
        INSERT INTO live_strikes6_latest (
          parent_symbol, label, instrument_class,
          exp_yyyymmdd, expiration_date, strike_price,
          raw_symbol, underlying_price, ts_refresh
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(parent_symbol, label, instrument_class) DO UPDATE SET
          exp_yyyymmdd=excluded.exp_yyyymmdd,
          expiration_date=excluded.expiration_date,
          strike_price=excluded.strike_price,
          raw_symbol=excluded.raw_symbol,
          underlying_price=excluded.underlying_price,
          ts_refresh=excluded.ts_refresh;
        """
        con0.executemany(upsert_strikes6, strike_rows)
        con0.close()
        print(f"Initial upsert strikes6 rows: {len(strike_rows)}")

    stream_to_duckdb_latest(raw_symbols)


if __name__ == "__main__":
    main()

