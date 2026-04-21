import os
import math
import datetime as dt
import datetime
import pytz
import pandas as pd
import databento as db
import duckdb

from databasefunctions import compute_z_scores_for_bucket, stabilize_schema
from config import DATABENTO_API_KEY

DUCKDB_PATH = "live_state.duckdb"
STRIKES_MAX_AGE_MIN = 10

# -------------------------
# DB READ HELPERS
# -------------------------
def get_sixpack_from_db(con, symbol: str) -> pd.DataFrame:
    """
    Returns 6 rows for a symbol from live_strikes6_latest.
    Expected labels: ATM, C1, P1, C2, P2 with instrument_class C/P as appropriate.
    """
    return con.execute(
        """
        SELECT
            parent_symbol AS symbol,
            exp_yyyymmdd,
            expiration_date,
            label,
            instrument_class,
            strike_price,
            raw_symbol,
            underlying_price,
            ts_refresh
        FROM live_strikes6_latest
        WHERE parent_symbol = ?
        ORDER BY label, instrument_class
        """,
        [symbol],
    ).fetchdf()






def get_quote_from_db(con, raw_symbol: str) -> dict:
    row = con.execute(
        """
        SELECT bid, ask, mid, spread, spread_pct
        FROM live_quotes_latest
        WHERE raw_symbol = ?
        """,
        [raw_symbol],
    ).fetchone()

    if not row:
        return {"bid": None, "ask": None, "mid": None, "spread": None, "spread_pct": None}

    bid, ask, mid, spread, spread_pct = row
    # compute derived if missing
    if mid is None and bid is not None and ask is not None and bid > 0 and ask > 0:
        mid = 0.5 * (float(bid) + float(ask))
    if spread is None and bid is not None and ask is not None and bid > 0 and ask > 0:
        spread = float(ask) - float(bid)
    if spread_pct is None and mid is not None and mid > 0 and spread is not None:
        spread_pct = float(spread) / float(mid)

    return {"bid": bid, "ask": ask, "mid": mid, "spread": spread, "spread_pct": spread_pct}


def get_vol10m_from_db(con, raw_symbol: str) -> int:
    row = con.execute(
        """
        SELECT vol10m
        FROM live_vol10m_latest
        WHERE raw_symbol = ?
        """,
        [raw_symbol],
    ).fetchone()
    return int(row[0]) if row and row[0] is not None else 0


def get_oi_from_db(con, raw_symbol: str) -> int | None:
    """
    Daily OI populated by streamer into live_oi_latest.
    """
    row = con.execute(
        """
        SELECT open_interest
        FROM live_oi_latest
        WHERE raw_symbol = ?
        """,
        [raw_symbol],
    ).fetchone()
    if not row or row[0] is None:
        return None
    try:
        return int(row[0])
    except Exception:
        return None


# -------------------------
# MARKET MATH
# -------------------------
def bs_iv_bisect(mid: float | None, S: float, K: float, days_to_expiry: int, call_put: str) -> float | None:
    if mid is None or mid <= 0:
        return None
    T = float(days_to_expiry) / 365.0
    if T <= 0:
        return None

    r = 0.01
    lo, hi = 1e-6, 5.0

    def N(x: float) -> float:
        return 0.5 * (1.0 + math.erf(x / math.sqrt(2.0)))

    def bs_price(sigma: float) -> float:
        d1 = (math.log(S / K) + (r + 0.5 * sigma * sigma) * T) / (sigma * math.sqrt(T))
        d2 = d1 - sigma * math.sqrt(T)
        if call_put == "C":
            return S * N(d1) - K * math.exp(-r * T) * N(d2)
        else:
            return K * math.exp(-r * T) * N(-d2) - S * N(-d1)

    for _ in range(60):
        m = 0.5 * (lo + hi)
        price = bs_price(m)
        if price > mid:
            hi = m
        else:
            lo = m

    return 0.5 * (lo + hi)


def time_decay_bucket(days: int) -> str:
    if days <= 1:
        return "EXTREME"
    if days <= 3:
        return "HIGH"
    if days <= 7:
        return "MEDIUM"
    return "LOW"


# -------------------------
# SNAPSHOT (DB -> PARQUET)
# -------------------------
def run_db_option_snapshot_to_parquet(
    run_id: str,
    symbol: str,
    shard_id: int,
):
    # read-only db connection (safer; streamer is writer)
    con = duckdb.connect(DUCKDB_PATH, read_only=True)

    six = get_sixpack_from_db(con, symbol)
    if six is None or six.empty:
        con.close()
        return "no strikes6 in db"

    # -------------------------
    # FRESHNESS GATE
    # -------------------------
    ts_series = six["ts_refresh"].dropna()
    if ts_series.empty:
        con.close()
        return "no ts_refresh in strikes6"

    ts_refresh = pd.to_datetime(ts_series.iloc[0], utc=True, errors="coerce")
    if pd.isna(ts_refresh):
        con.close()
        return "bad ts_refresh in strikes6"

    now = dt.datetime.now(tz=pytz.UTC)
    age_min = (now - ts_refresh).total_seconds() / 60.0
    if age_min > STRIKES_MAX_AGE_MIN:
        con.close()
        return f"stale strikes6 ({age_min:.1f} min old)"

    # -------------------------
    # UNDERLYING / EXPIRY
    # -------------------------
    underlying_price_series = six["underlying_price"].dropna()
    if underlying_price_series.empty:
        con.close()
        return "no underlying_price in strikes6"
    underlying_price = float(underlying_price_series.iloc[0])

    exp_yyyymmdd = str(six["exp_yyyymmdd"].iloc[0])
    exp_date = datetime.datetime.strptime(exp_yyyymmdd, "%Y%m%d").date()

    today_utc = now.date()
    days_till_expiry = int((exp_date - today_utc).days)
    tdb = time_decay_bucket(days_till_expiry)

    # -------------------------
    # MAP SIX LEGS
    # -------------------------
    def pick_row(label: str, cp: str) -> pd.Series | None:
        sub = six[(six["label"] == label) & (six["instrument_class"] == cp)]
        if sub.empty:
            return None
        return sub.iloc[0]

    legs = [
        ("ATM", "C", "ATM"),
        ("ATM", "P", "ATM"),
        ("C1", "C", "OTM_1"),
        ("P1", "P", "OTM_1"),
        ("C2", "C", "OTM_2"),
        ("P2", "P", "OTM_2"),
    ]

    leg_rows = []
    for label, cp, bucket in legs:
        r = pick_row(label, cp)
        if r is None:
            con.close()
            return f"missing strikes6 row: {label} {cp}"
        leg_rows.append((label, cp, bucket, r))

    # -------------------------
    # PULL QUOTES / VOL / OI
    # -------------------------
    enriched = []
    for label, cp, bucket, r in leg_rows:
        raw = str(r["raw_symbol"])
        strike = float(r["strike_price"])

        q = get_quote_from_db(con, raw)
        vol10m = get_vol10m_from_db(con, raw)
        oi = get_oi_from_db(con, raw)

        enriched.append(
            {
                "label": label,
                "call_put": cp,
                "moneyness_bucket": bucket,
                "raw_symbol": raw,
                "strike": strike,
                "bid": q["bid"],
                "ask": q["ask"],
                "mid": q["mid"],
                "spread": q["spread"],
                "spread_pct": q["spread_pct"],
                "volume": int(vol10m),
                "open_interest": oi,
            }
        )

    con.close()

    # require ATM mids
    atm_call_mid = next(x["mid"] for x in enriched if x["label"] == "ATM" and x["call_put"] == "C")
    atm_put_mid = next(x["mid"] for x in enriched if x["label"] == "ATM" and x["call_put"] == "P")
    if atm_call_mid is None or atm_put_mid is None:
        return "no live quotes yet"

    # -------------------------
    # IV CALC
    # -------------------------
    for x in enriched:
        x["iv"] = bs_iv_bisect(x["mid"], underlying_price, x["strike"], days_till_expiry, x["call_put"])

    timestamp = now.strftime("%Y-%m-%d %H:%M:%S")
    snapshot_id = f"{symbol}_{timestamp}"

    # (rest of your parquet / z-score / exec code continues unchanged)

    # -------------------------
    # RAW parquet (same schema)
    # -------------------------
    cols1 = [
        "snapshot_id", "timestamp", "symbol", "underlying_price", "strike", "call_put",
        "days_to_expiry", "expiration_date", "moneyness_bucket", "bid", "ask", "mid",
        "volume", "open_interest", "iv", "spread", "spread_pct", "time_decay_bucket",
    ]

    rows1 = []
    for x in enriched:
        rows1.append(
            [
                snapshot_id,
                timestamp,
                symbol,
                float(underlying_price),
                float(x["strike"]),
                x["call_put"],
                int(days_till_expiry),
                exp_date,
                x["moneyness_bucket"],
                x["bid"],
                x["ask"],
                x["mid"],
                int(x["volume"]),
                x["open_interest"],
                x["iv"],
                x["spread"],
                x["spread_pct"],
                tdb,
            ]
        )

    df1 = pd.DataFrame(rows1, columns=cols1)
    df1 = stabilize_schema(df1)

    out_dir = f"runs/{run_id}/option_snapshots_raw"
    os.makedirs(out_dir, exist_ok=True)
    out_path = f"{out_dir}/shard_{shard_id}_{symbol}.parquet"
    df1.to_parquet(out_path, index=False)

    # -------------------------
    # Z-SCORES (same calls, just sourced from DB now)
    # -------------------------
    def cur(bucket: str, cp: str) -> dict:
        for x in enriched:
            if x["moneyness_bucket"] == bucket and x["call_put"] == cp:
                return x
        raise RuntimeError(f"missing leg {bucket} {cp}")

    def z_for(bucket: str, cp: str):
        x = cur(bucket, cp)
        return compute_z_scores_for_bucket(
            symbol=symbol,
            bucket=bucket,
            call_put=cp,
            time_decay_bucket=tdb,
            current_mid=x["mid"],
            current_volume=x["volume"],
            current_iv=x["iv"],
        )

    (
        atm_call_mid_z_3d, atm_call_vol_z_3d, atm_call_iv_z_3d,
        atm_call_mid_z_5w, atm_call_vol_z_5w, atm_call_iv_z_5w,
    ) = z_for("ATM", "C")

    (
        atm_put_mid_z_3d, atm_put_vol_z_3d, atm_put_iv_z_3d,
        atm_put_mid_z_5w, atm_put_vol_z_5w, atm_put_iv_z_5w,
    ) = z_for("ATM", "P")

    (
        otm_call_1_mid_z_3d, otm_call_1_vol_z_3d, otm_call_1_iv_z_3d,
        otm_call_1_mid_z_5w, otm_call_1_vol_z_5w, otm_call_1_iv_z_5w,
    ) = z_for("OTM_1", "C")

    (
        otm_put_1_mid_z_3d, otm_put_1_vol_z_3d, otm_put_1_iv_z_3d,
        otm_put_1_mid_z_5w, otm_put_1_vol_z_5w, otm_put_1_iv_z_5w,
    ) = z_for("OTM_1", "P")

    (
        otm_call_2_mid_z_3d, otm_call_2_vol_z_3d, otm_call_2_iv_z_3d,
        otm_call_2_mid_z_5w, otm_call_2_vol_z_5w, otm_call_2_iv_z_5w,
    ) = z_for("OTM_2", "C")

    (
        otm_put_2_mid_z_3d, otm_put_2_vol_z_3d, otm_put_2_iv_z_3d,
        otm_put_2_mid_z_5w, otm_put_2_vol_z_5w, otm_put_2_iv_z_5w,
    ) = z_for("OTM_2", "P")

    # -------------------------
    # ENRICHED parquet
    # -------------------------
    cols2 = [
        "snapshot_id", "timestamp", "symbol", "underlying_price",
        "strike", "call_put", "days_to_expiry", "expiration_date", "moneyness_bucket",
        "bid", "ask", "mid", "volume", "open_interest", "iv", "spread", "spread_pct",
        "time_decay_bucket",
        "mid_z_3d", "volume_z_3d", "iv_z_3d",
        "mid_z_5w", "volume_z_5w", "iv_z_5w",
        "opt_ret_10m", "opt_ret_1h", "opt_ret_eod", "opt_ret_next_open", "opt_ret_1d", "opt_ret_exp",
    ]

    def pack_row(bucket: str, cp: str, mid_z_3d, vol_z_3d, iv_z_3d, mid_z_5w, vol_z_5w, iv_z_5w):
        x = cur(bucket, cp)
        return [
            snapshot_id, timestamp, symbol, float(underlying_price),
            float(x["strike"]), cp, int(days_till_expiry), exp_date, bucket,
            x["bid"], x["ask"], x["mid"], int(x["volume"]), x["open_interest"], x["iv"],
            x["spread"], x["spread_pct"], tdb,
            mid_z_3d, vol_z_3d, iv_z_3d,
            mid_z_5w, vol_z_5w, iv_z_5w,
            None, None, None, None, None, None,
        ]

    rows2 = [
        pack_row("ATM", "C", atm_call_mid_z_3d, atm_call_vol_z_3d, atm_call_iv_z_3d, atm_call_mid_z_5w, atm_call_vol_z_5w, atm_call_iv_z_5w),
        pack_row("ATM", "P", atm_put_mid_z_3d, atm_put_vol_z_3d, atm_put_iv_z_3d, atm_put_mid_z_5w, atm_put_vol_z_5w, atm_put_iv_z_5w),
        pack_row("OTM_1", "C", otm_call_1_mid_z_3d, otm_call_1_vol_z_3d, otm_call_1_iv_z_3d, otm_call_1_mid_z_5w, otm_call_1_vol_z_5w, otm_call_1_iv_z_5w),
        pack_row("OTM_1", "P", otm_put_1_mid_z_3d, otm_put_1_vol_z_3d, otm_put_1_iv_z_3d, otm_put_1_mid_z_5w, otm_put_1_vol_z_5w, otm_put_1_iv_z_5w),
        pack_row("OTM_2", "C", otm_call_2_mid_z_3d, otm_call_2_vol_z_3d, otm_call_2_iv_z_3d, otm_call_2_mid_z_5w, otm_call_2_vol_z_5w, otm_call_2_iv_z_5w),
        pack_row("OTM_2", "P", otm_put_2_mid_z_3d, otm_put_2_vol_z_3d, otm_put_2_iv_z_3d, otm_put_2_mid_z_5w, otm_put_2_vol_z_5w, otm_put_2_iv_z_5w),
    ]

    df2 = pd.DataFrame(rows2, columns=cols2)
    df2 = stabilize_schema(df2)

    out_dir = f"runs/{run_id}/option_snapshots_enriched"
    os.makedirs(out_dir, exist_ok=True)
    out_path = f"{out_dir}/shard_{shard_id}_{symbol}.parquet"
    df2.to_parquet(out_path, index=False)

    # -------------------------
    # EXECUTION SIGNALS parquet
    # -------------------------
    cols3 = [
        "snapshot_id", "timestamp", "symbol", "underlying_price",
        "strike", "call_put", "days_to_expiry", "expiration_date", "moneyness_bucket",
        "bid", "ask", "mid", "volume", "open_interest", "iv", "spread", "spread_pct",
        "time_decay_bucket",
        "mid_z_3d", "volume_z_3d", "iv_z_3d",
        "mid_z_5w", "volume_z_5w", "iv_z_5w",
        "opt_ret_10m", "opt_ret_1h", "opt_ret_eod", "opt_ret_next_open", "opt_ret_1d", "opt_ret_exp",
        "atm_call_signal", "atm_put_signal", "otm1_call_signal", "otm1_put_signal", "otm2_call_signal", "otm2_put_signal",
    ]

    def pack_exec(bucket: str, cp: str, mid_z_3d, vol_z_3d, iv_z_3d, mid_z_5w, vol_z_5w, iv_z_5w):
        x = cur(bucket, cp)
        return [
            snapshot_id, timestamp, symbol, float(underlying_price),
            float(x["strike"]), cp, int(days_till_expiry), exp_date, bucket,
            x["bid"], x["ask"], x["mid"], int(x["volume"]), x["open_interest"], x["iv"],
            x["spread"], x["spread_pct"], tdb,
            mid_z_3d, vol_z_3d, iv_z_3d,
            mid_z_5w, vol_z_5w, iv_z_5w,
            None, None, None, None, None, None,
            None, None, None, None, None, None,
        ]

    rows3 = [
        pack_exec("ATM", "C", atm_call_mid_z_3d, atm_call_vol_z_3d, atm_call_iv_z_3d, atm_call_mid_z_5w, atm_call_vol_z_5w, atm_call_iv_z_5w),
        pack_exec("ATM", "P", atm_put_mid_z_3d, atm_put_vol_z_3d, atm_put_iv_z_3d, atm_put_mid_z_5w, atm_put_vol_z_5w, atm_put_iv_z_5w),
        pack_exec("OTM_1", "C", otm_call_1_mid_z_3d, otm_call_1_vol_z_3d, otm_call_1_iv_z_3d, otm_call_1_mid_z_5w, otm_call_1_vol_z_5w, otm_call_1_iv_z_5w),
        pack_exec("OTM_1", "P", otm_put_1_mid_z_3d, otm_put_1_vol_z_3d, otm_put_1_iv_z_3d, otm_put_1_mid_z_5w, otm_put_1_vol_z_5w, otm_put_1_iv_z_5w),
        pack_exec("OTM_2", "C", otm_call_2_mid_z_3d, otm_call_2_vol_z_3d, otm_call_2_iv_z_3d, otm_call_2_mid_z_5w, otm_call_2_vol_z_5w, otm_call_2_iv_z_5w),
        pack_exec("OTM_2", "P", otm_put_2_mid_z_3d, otm_put_2_vol_z_3d, otm_put_2_iv_z_3d, otm_put_2_mid_z_5w, otm_put_2_vol_z_5w, otm_put_2_iv_z_5w),
    ]

    df3 = pd.DataFrame(rows3, columns=cols3)
    df3 = stabilize_schema(df3)

    out_dir = f"runs/{run_id}/option_snapshots_execution_signals"
    os.makedirs(out_dir, exist_ok=True)
    out_path = f"{out_dir}/shard_{shard_id}_{symbol}.parquet"
    df3.to_parquet(out_path, index=False)

    return "ok"