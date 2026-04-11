import duckdb
import datetime as dt
import pandas as pd
import yfinance as yf
from policy.expiration import find_first_eligible_friday
from policy.strikes import closest_strike


def get_raw_symbol(df, sym, exp_yyyymmdd, cp, target_strike):
    for _, row in df.iterrows():
        if row["symbol"] != sym:
            continue

        exp = pd.to_datetime(row["expiration"], errors="coerce")
        if pd.isna(exp) or exp.strftime("%Y%m%d") != exp_yyyymmdd:
            continue

        if row["instrument_class"] != cp:
            continue

        strike = pd.to_numeric(row["strike_price"], errors="coerce")
        if pd.isna(strike) or float(strike) != float(target_strike):
            continue

        return str(row["raw_symbol"])

    return None


con = duckdb.connect("definitioncache.duckdb")
raw_con = duckdb.connect("rawsymbols.db")
raw_con.execute("""
CREATE TABLE IF NOT EXISTS raw_symbols (
    parent_symbol TEXT,
    raw_option_symbol TEXT,
    side TEXT,      -- C or P
    grouping TEXT,  -- ATM or OTM1
    decay_bucket TEXT
    days_to_expiry TEXT
)
""")
raw_con.execute("ALTER TABLE raw_symbols ADD COLUMN IF NOT EXISTS decay_bucket TEXT")

symbols = [r[0] for r in con.execute("""
    SELECT DISTINCT symbol
    FROM definition_cache
""").fetchall()]

for sym in symbols:
    df = con.execute("""
        SELECT *
        FROM definition_cache AS d
        WHERE d.symbol = ?
    """, [sym]).fetchdf()
    print(sym, len(df))
    if df.empty:
        continue

    strike_col = "strike_price" if "strike_price" in df.columns else "strike_price "
    strike_prices = df[strike_col]
    expirations = df["expiration"]

    expirations_yyyymmdd = (
        pd.to_datetime(expirations, errors="coerce")
        .dropna()
        .dt.strftime("%Y%m%d")
        .unique()
        .tolist()
    )
    valid_friday_exp_yyyymmdd = find_first_eligible_friday(
        expirations_yyyymmdd,
        dt.datetime.utcnow().date(),
        lookahead_days=4,
        exclude_third_friday=True,
    )

    if valid_friday_exp_yyyymmdd is None:
        print(f"{sym}: no valid Friday expiration in next 4 days")
        continue

    print(f"{sym}: valid Friday expiration = {valid_friday_exp_yyyymmdd}")

    hist = yf.Ticker(sym).history(period="1d", interval="1m")
    close_series = hist["Close"].dropna() if "Close" in hist.columns else pd.Series(dtype=float)
    if close_series.empty:
        print(f"{sym}: no underlying price")
        continue
    underlying = float(close_series.iloc[-1])

    STRIKE_TARGET_MULTIPLIERS = {
        "ATM": 1.000 * underlying,
        "C1": 1.015 * underlying,
        "P1": 0.985 * underlying,
        "C2": 1.035 * underlying,
        "P2": 0.965 * underlying,
    }

    def time_decay_bucket(days: int) -> str:
        if days <= 1:
            return "EXTREME"
        if days <= 3:
            return "HIGH"
        if days <= 7:
            return "MEDIUM"
        return "LOW"

    expiration_date = dt.datetime.strptime(valid_friday_exp_yyyymmdd, "%Y%m%d").date()
    days_to_expiry = (expiration_date - dt.datetime.utcnow().date()).days
    decay_bucket = time_decay_bucket(days_to_expiry)

    available_strikes = pd.to_numeric(strike_prices, errors="coerce").dropna().astype(float).tolist()
    if not available_strikes:
        print(f"{sym}: no usable strike prices")
        continue

    closest_by_label = {
        label: closest_strike(target_price, available_strikes)
        for label, target_price in STRIKE_TARGET_MULTIPLIERS.items()
    }

    ATM = closest_by_label["ATM"]   # same strike for call + put
    C1 = closest_by_label["C1"]
    P1 = closest_by_label["P1"]
    C2 = closest_by_label["C2"]
    P2 = closest_by_label["P2"]

    raw_atm_c = get_raw_symbol(df, sym, valid_friday_exp_yyyymmdd, "C", ATM)
    raw_atm_p = get_raw_symbol(df, sym, valid_friday_exp_yyyymmdd, "P", ATM)
    raw_c1 = get_raw_symbol(df, sym, valid_friday_exp_yyyymmdd, "C", C1)
    raw_p1 = get_raw_symbol(df, sym, valid_friday_exp_yyyymmdd, "P", P1)
    raw_c2 = get_raw_symbol(df, sym, valid_friday_exp_yyyymmdd, "C", C2)
    raw_p2 = get_raw_symbol(df, sym, valid_friday_exp_yyyymmdd, "P", P2)

    print(
        f"{sym} raws | "
        f"ATM_C={raw_atm_c} ATM_P={raw_atm_p} "
        f"C1={raw_c1} P1={raw_p1} C2={raw_c2} P2={raw_p2}"
    )

    raw_con.execute("DELETE FROM raw_symbols WHERE parent_symbol = ?", [sym])

    rows_to_insert = [
        row for row in [
            (sym, raw_atm_c, "C", "ATM", decay_bucket,days_to_expiry),
            (sym, raw_atm_p, "P", "ATM", decay_bucket,days_to_expiry),
            (sym, raw_c1, "C", "OTM1", decay_bucket,days_to_expiry),
            (sym, raw_p1, "P", "OTM1", decay_bucket,days_to_expiry),
            (sym, raw_c2, "C", "OTM2", decay_bucket,days_to_expiry),
            (sym, raw_p2, "P", "OTM2", decay_bucket,days_to_expiry),
        ]
        if row[1] is not None
    ]

    if rows_to_insert:
        raw_con.executemany(
            "INSERT INTO raw_symbols (parent_symbol, raw_option_symbol, side, grouping, decay_bucket,days_to_expiry) VALUES (?, ?, ?, ?, ?)",
            rows_to_insert,
        )


con.close()
raw_con.close()
