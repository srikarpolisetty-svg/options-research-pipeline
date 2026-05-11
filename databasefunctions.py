import duckdb
import pandas as pd
import os

DB_PATH = os.getenv("OPTIONS_DB_PATH", "options_data.db")

def compute_z_scores_for_bucket(
    symbol: str,
    bucket: str,
    call_put: str,
    time_decay_bucket: str,
    current_mid,
    current_volume,
    current_iv,
    table: str = "option_snapshots_raw",
):
    """
    Compute BOTH 3-day and 35-day z-scores for mid, volume, iv
    from a single raw options table.

    Returns:
        (mid_z_3d, vol_z_3d, iv_z_3d, mid_z_35d, vol_z_35d, iv_z_35d)
        where any element can be None if we can't compute it safely.
    """

    with duckdb.connect(DB_PATH, read_only=True) as con:
        df = con.execute(
            f"""
            SELECT
                mid,
                volume,
                iv,
                timestamp
            FROM {table}
            WHERE symbol = ?
              AND moneyness_bucket = ?
              AND call_put = ?
              AND time_decay_bucket = ?
              AND timestamp >= CURRENT_TIMESTAMP - INTERVAL 35 DAY
            """,
            [symbol, bucket, call_put, time_decay_bucket],
        ).df()

    if df.empty:
        return (None, None, None, None, None, None)

    # Make sure timestamp is datetime
    df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")

    # Helper: safe z
    def z(curr, series: pd.Series):
        if curr is None:
            return None

        s = series.dropna()
        if s.empty:
            return None

        mean = s.mean()
        std  = s.std()

        if std is None or pd.isna(std) or std <= 0:
            return None

        try:
            curr_val = float(curr)
        except Exception:
            return None

        return (curr_val - mean) / std

    # --------------------
    # 3-day window (relative to most recent timestamp we have for this symbol+bucket)
    # --------------------
    tmax = df["timestamp"].max()
    if pd.isna(tmax):
        df_3d = df.iloc[0:0]
    else:
        df_3d = df[df["timestamp"] >= (tmax - pd.Timedelta(days=3))]

    mid_z_3d = z(current_mid,    df_3d["mid"])
    vol_z_3d = z(current_volume, df_3d["volume"])
    iv_z_3d  = z(current_iv,     df_3d["iv"])

    # --------------------
    # 35-day window
    # --------------------
    mid_z_35d = z(current_mid,    df["mid"])
    vol_z_35d = z(current_volume, df["volume"])
    iv_z_35d  = z(current_iv,     df["iv"])

    return (mid_z_3d, vol_z_3d, iv_z_3d, mid_z_35d, vol_z_35d, iv_z_35d)












def get_option_quote(chain, call_put: str, strike: float):
    """
    chain: yfinance option_chain(exp)
    call_put: "C" for calls, "P" for puts
    strike: target strike price (float)

    Returns a dict with:
      last_price, bid, ask, mid,
      volume, iv, oi,
      spread, spread_pct
    """
    df = chain.calls if call_put == "C" else chain.puts

    row = df[df["strike"] == strike].iloc[0]

    bid   = row["bid"]
    ask   = row["ask"]
    mid   = (bid + ask) / 2
    spread = ask - bid
    spread_pct = (spread / mid) * 100 if mid != 0 else 0.0

    return {
        "last_price": row["lastPrice"],
        "bid": bid,
        "ask": ask,
        "mid": mid,
        "volume": row["volume"],
        "iv": row["impliedVolatility"],
        "oi": row["openInterest"],
        "spread": spread,
        "spread_pct": spread_pct,
    }











def get_closest_strike(chain, call_put, target):
    df = chain.calls if call_put == "C" else chain.puts
    strikes = df["strike"].values

    # find strike with minimum absolute distance from target
    return min(strikes, key=lambda s: abs(s - target))















import os
import time
import tempfile
from typing import List, Optional

import requests
import pandas as pd




SP500_URL = "https://raw.githubusercontent.com/datasets/s-and-p-500-companies/main/data/constituents.csv"
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CACHE_PATH = os.path.join(BASE_DIR, "state", "sp500_constituents.csv")

def _atomic_write_csv(df: pd.DataFrame, path: str) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    d = os.path.dirname(path) or "."
    with tempfile.NamedTemporaryFile("w", delete=False, dir=d, suffix=".tmp") as f:
        tmp_path = f.name
        df.to_csv(tmp_path, index=False)
    os.replace(tmp_path, path)  # atomic on POSIX

def _normalize_symbol(sym: str) -> str:
    # yfinance compatibility: BRK.B -> BRK-B, BF.B -> BF-B
    return sym.replace(".", "-").strip().upper()

def get_sp500_symbols(retries: int = 3, backoff_sec: float = 2.0, timeout_sec: float = 10.0) -> List[str]:
    last_err: Optional[Exception] = None

    for i in range(retries):
        try:
            r = requests.get(SP500_URL, timeout=timeout_sec)
            r.raise_for_status()

            # Parse CSV from text content
            from io import StringIO
            df = pd.read_csv(StringIO(r.text))

            if "Symbol" not in df.columns:
                raise ValueError(f"Expected 'Symbol' column, got columns={list(df.columns)}")

            syms = df["Symbol"].dropna().astype(str).map(_normalize_symbol).tolist()

            # sanity check: avoid caching nonsense
            if len(syms) < 400:
                raise ValueError(f"Too few symbols ({len(syms)}). Possible bad response.")

            _atomic_write_csv(df, CACHE_PATH)
            return syms

        except Exception as e:
            last_err = e
            time.sleep(backoff_sec * (2 ** i))

    # Fallback cache
    if os.path.exists(CACHE_PATH):
        df = pd.read_csv(CACHE_PATH)
        if "Symbol" not in df.columns:
            raise RuntimeError(f"Cache exists but missing 'Symbol' column: {CACHE_PATH}")
        return df["Symbol"].dropna().astype(str).map(_normalize_symbol).tolist()

    raise RuntimeError(f"Failed to fetch S&P 500 symbols and no cache found at {CACHE_PATH}.") from last_err









import glob
import duckdb


def master_ingest(run_id: str, db_path: str = "options_data.db"):
    """
    Master ingest for a single options run_id.
    Tables already exist.
    Single-DB version (no *_5w tables).

    Hardens SELECT * by:
      - skipping when a glob matches zero files
      - asserting parquet column order == table column order (so SELECT * is safe)
      - still using union_by_name=True for NULL/DOUBLE compatibility
    """
    import glob
    import duckdb

    raw_dir = f"runs/{run_id}/option_snapshots_raw"
    enriched_dir = f"runs/{run_id}/option_snapshots_enriched"
    signals_dir = f"runs/{run_id}/option_snapshots_execution_signals"

    raw_glob = f"{raw_dir}/shard_*.parquet"
    enriched_glob = f"{enriched_dir}/shard_*.parquet"
    signals_glob = f"{signals_dir}/shard_*.parquet"

    # ---- guard: if nothing exists anywhere, exit cleanly ----
    if not (glob.glob(raw_glob) or glob.glob(enriched_glob) or glob.glob(signals_glob)):
        print(f"[master_ingest] no parquet files found for run_id={run_id}", flush=True)
        return

    con = duckdb.connect(db_path)

    def _assert_schema_order(table: str, parquet_glob: str) -> bool:
        """
        Enforce: parquet columns (names + order) exactly match table columns.
        Keeps SELECT * safe. Returns False if no files.
        """
        if not glob.glob(parquet_glob):
            print(f"[master_ingest] skip {table}: no files", flush=True)
            return False

        pq_cols = con.execute(
            "DESCRIBE SELECT * FROM read_parquet(?, union_by_name=True)",
            [parquet_glob],
        ).df()["column_name"].tolist()

        tbl_cols = con.execute(
            f"PRAGMA table_info('{table}')"
        ).df()["name"].tolist()

        if pq_cols != tbl_cols:
            raise RuntimeError(
                f"[master_ingest] schema/order mismatch for {table}\n"
                f"  parquet={pq_cols}\n"
                f"  table ={tbl_cols}"
            )
        return True

    def ingest_if_exists(table: str, parquet_glob: str):
        if not _assert_schema_order(table, parquet_glob):
            return

        con.execute(
            f"""
            INSERT INTO {table}
            SELECT * FROM read_parquet(?, union_by_name=True)
            """,
            [parquet_glob],
        )

    try:
        con.execute("BEGIN;")

        ingest_if_exists("option_snapshots_execution_signals", signals_glob)
        ingest_if_exists("option_snapshots_enriched", enriched_glob)
        ingest_if_exists("option_snapshots_raw", raw_glob)

        con.execute("COMMIT;")
        print(f"[master_ingest] committed run_id={run_id}", flush=True)

    except Exception:
        con.execute("ROLLBACK;")
        raise

    finally:
        con.close()




import pandas as pd

NUMERIC_COLS = [
    "bid", "ask", "mid", "iv", "spread", "spread_pct",
    "strike",
    # add any other numeric columns you have
]

INT_COLS = [
    "volume", "open_interest", "bidSize", "askSize", "lastSize",
    # add any other integer-ish columns
]

def stabilize_schema(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    for c in NUMERIC_COLS:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce").astype("float64")

    for c in INT_COLS:
        if c in df.columns:
            # keep as nullable int if you want None support
            df[c] = pd.to_numeric(df[c], errors="coerce").astype("Int64")

    return df
