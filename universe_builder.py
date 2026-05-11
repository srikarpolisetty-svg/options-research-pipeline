from collections import defaultdict
import duckdb
import datetime as dt
import pandas as pd
import yfinance as yf
from policy.expiration import find_first_eligible_friday_with_reason
from policy.option_symbols import (
    UNSUPPORTED_OPTION_CHAIN_SYMBOLS,
    filter_supported_option_chain_symbols,
)
from policy.strikes import closest_strike

PROGRESS_EVERY = 50


def debug(message: str) -> None:
    print(f"[UNIVERSE] {message}", flush=True)


def expiration_to_utc_date(value):
    ts = pd.to_datetime(value, errors="coerce", utc=True)
    if pd.isna(ts):
        return None
    return ts.date()


def expiration_series_to_yyyymmdd(series: pd.Series) -> pd.Series:
    parsed = pd.to_datetime(series, errors="coerce", utc=True)
    return parsed.dt.strftime("%Y%m%d")


def get_raw_symbol(df, sym, exp_yyyymmdd, cp, target_strike):
    for _, row in df.iterrows():
        if row["symbol"] != sym:
            continue

        exp_date = expiration_to_utc_date(row["expiration"])
        if exp_date is None or exp_date.strftime("%Y%m%d") != exp_yyyymmdd:
            continue

        if row["instrument_class"] != cp:
            continue

        strike = pd.to_numeric(row["strike_price"], errors="coerce")
        if pd.isna(strike) or float(strike) != float(target_strike):
            continue

        return str(row["raw_symbol"])

    return None


def format_reason_counts(reason_counts: dict[str, int]) -> str:
    if not reason_counts:
        return "none"
    return ", ".join(f"{reason}={count}" for reason, count in sorted(reason_counts.items()))


con = duckdb.connect("definitioncache.duckdb", read_only=True)
raw_con = duckdb.connect("rawsymbols.db")
debug("db open definitioncache=read_only rawsymbols=write")
raw_con.execute("""
CREATE TABLE IF NOT EXISTS raw_symbols (
    parent_symbol TEXT,
    raw_option_symbol TEXT,
    side TEXT,      -- C or P
    grouping TEXT,  -- ATM or OTM1
    decay_bucket TEXT,
    days_to_expiry INTEGER
)
""")
raw_con.execute("ALTER TABLE raw_symbols ADD COLUMN IF NOT EXISTS decay_bucket TEXT")
raw_con.execute("ALTER TABLE raw_symbols ADD COLUMN IF NOT EXISTS days_to_expiry INTEGER")
if UNSUPPORTED_OPTION_CHAIN_SYMBOLS:
    placeholders = ", ".join(["?"] * len(UNSUPPORTED_OPTION_CHAIN_SYMBOLS))
    raw_con.execute(
        f"DELETE FROM raw_symbols WHERE parent_symbol IN ({placeholders})",
        sorted(UNSUPPORTED_OPTION_CHAIN_SYMBOLS),
    )

symbols = [r[0] for r in con.execute("""
    SELECT DISTINCT symbol
    FROM definition_cache
""").fetchall()]
symbols = filter_supported_option_chain_symbols([str(symbol) for symbol in symbols])
today_utc = dt.datetime.now(dt.timezone.utc).date()
debug(f"start symbols={len(symbols)} date={today_utc}")
summary_counts: dict[str, int] = defaultdict(int)
expiry_reason_counts: dict[str, int] = defaultdict(int)
expiry_reason_samples: list[str] = []
inserted_rows = 0

for sym in symbols:
    summary_counts["symbols_seen"] += 1
    if summary_counts["symbols_seen"] == 1 or summary_counts["symbols_seen"] % PROGRESS_EVERY == 0:
        debug(
            f"progress {summary_counts['symbols_seen']}/{len(symbols)} "
            f"inserted={summary_counts['inserted_symbols']} "
            f"no_exp={summary_counts['no_valid_weekly_expiration']}"
        )
    df = con.execute("""
        SELECT *
        FROM definition_cache AS d
        WHERE d.symbol = ?
    """, [sym]).fetchdf()
    if df.empty:
        summary_counts["empty_definition_rows"] += 1
        continue

    strike_col = "strike_price" if "strike_price" in df.columns else "strike_price "
    expirations = df["expiration"]

    expirations_yyyymmdd = (
        expiration_series_to_yyyymmdd(expirations)
        .dropna()
        .unique()
        .tolist()
    )
    valid_weekly_exp_yyyymmdd, expiry_reason = find_first_eligible_friday_with_reason(
        expirations_yyyymmdd,
        today_utc,
        lookahead_days=4,
        exclude_third_friday=True,
    )

    if valid_weekly_exp_yyyymmdd is None:
        summary_counts["no_valid_weekly_expiration"] += 1
        reason = expiry_reason or "unknown"
        expiry_reason_counts[reason] += 1
        if len(expiry_reason_samples) < 10:
            expiry_reason_samples.append(f"{sym}:{reason}")
        continue

    exp_df = df[
        expiration_series_to_yyyymmdd(df["expiration"]) == valid_weekly_exp_yyyymmdd
    ].copy()
    if exp_df.empty:
        summary_counts["no_rows_for_selected_expiration"] += 1
        continue

    hist = yf.Ticker(sym).history(period="1d", interval="1m")
    close_series = hist["Close"].dropna() if "Close" in hist.columns else pd.Series(dtype=float)
    if close_series.empty:
        summary_counts["no_underlying_price"] += 1
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

    expiration_date = dt.datetime.strptime(valid_weekly_exp_yyyymmdd, "%Y%m%d").date()
    days_to_expiry = (expiration_date - today_utc).days
    decay_bucket = time_decay_bucket(days_to_expiry)

    available_strikes = pd.to_numeric(exp_df[strike_col], errors="coerce").dropna().astype(float).tolist()
    if not available_strikes:
        summary_counts["no_usable_strike_prices"] += 1
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

    raw_atm_c = get_raw_symbol(exp_df, sym, valid_weekly_exp_yyyymmdd, "C", ATM)
    raw_atm_p = get_raw_symbol(exp_df, sym, valid_weekly_exp_yyyymmdd, "P", ATM)
    raw_c1 = get_raw_symbol(exp_df, sym, valid_weekly_exp_yyyymmdd, "C", C1)
    raw_p1 = get_raw_symbol(exp_df, sym, valid_weekly_exp_yyyymmdd, "P", P1)
    raw_c2 = get_raw_symbol(exp_df, sym, valid_weekly_exp_yyyymmdd, "C", C2)
    raw_p2 = get_raw_symbol(exp_df, sym, valid_weekly_exp_yyyymmdd, "P", P2)

    six_pack = [raw_atm_c, raw_atm_p, raw_c1, raw_p1, raw_c2, raw_p2]
    if any(raw_symbol is None for raw_symbol in six_pack):
        summary_counts["incomplete_six_leg_set"] += 1
        continue

    raw_con.execute("DELETE FROM raw_symbols WHERE parent_symbol = ?", [sym])

    rows_to_insert = [
        (sym, raw_atm_c, "C", "ATM", decay_bucket, days_to_expiry),
        (sym, raw_atm_p, "P", "ATM", decay_bucket, days_to_expiry),
        (sym, raw_c1, "C", "OTM1", decay_bucket, days_to_expiry),
        (sym, raw_p1, "P", "OTM1", decay_bucket, days_to_expiry),
        (sym, raw_c2, "C", "OTM2", decay_bucket, days_to_expiry),
        (sym, raw_p2, "P", "OTM2", decay_bucket, days_to_expiry),
    ]

    if rows_to_insert:
        raw_con.executemany(
            "INSERT INTO raw_symbols (parent_symbol, raw_option_symbol, side, grouping, decay_bucket, days_to_expiry) VALUES (?, ?, ?, ?, ?, ?)",
            rows_to_insert,
        )
        summary_counts["inserted_symbols"] += 1
        inserted_rows += len(rows_to_insert)


con.close()
raw_con.close()
print(
    f"[UNIVERSE] symbols={summary_counts['symbols_seen']} "
    f"inserted_symbols={summary_counts['inserted_symbols']} inserted_rows={inserted_rows}"
)
print(
    "[UNIVERSE] skipped: "
    f"no_valid_weekly_expiration={summary_counts['no_valid_weekly_expiration']} "
    f"no_rows_for_selected_expiration={summary_counts['no_rows_for_selected_expiration']} "
    f"no_underlying_price={summary_counts['no_underlying_price']} "
    f"no_usable_strike_prices={summary_counts['no_usable_strike_prices']} "
    f"incomplete_six_leg_set={summary_counts['incomplete_six_leg_set']} "
    f"empty_definition_rows={summary_counts['empty_definition_rows']}"
)
print(
    f"[UNIVERSE] no_valid_weekly_expiration reasons: {format_reason_counts(dict(expiry_reason_counts))}"
)
if expiry_reason_samples:
    print(f"[UNIVERSE] expiry samples: {', '.join(expiry_reason_samples)}")
