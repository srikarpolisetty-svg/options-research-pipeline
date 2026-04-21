import databento as db
import yfinance as yf
import pandas as pd
import numpy as np
from databento.common.error import BentoClientError
from datetime import date, datetime, time as dt_time, timedelta, timezone
from config import DATABENTO_API_KEY
import math
import duckdb
import argparse
import zlib
import time
import pathlib
import shutil
from functools import lru_cache
from collections import defaultdict, deque
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from zoneinfo import ZoneInfo


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
POLL_S = 10.0
POST_DEF_MAX_WORKERS = 15
BATCH_SUBMIT_RATE_LIMIT_PER_MIN = 20
BATCH_SUBMIT_WINDOW_S = 60.0
BATCH_SUBMIT_MIN_SPACING_S = 3.2
DEF_TS_MAX_WORKERS = 50
DEF_TS_RATE_LIMIT_COUNT = 50
DEF_TS_RATE_LIMIT_WINDOW_S = 2.0
DEF_TS_PROGRESS_EVERY = 25
DEF_TS_SUBMIT_PROGRESS_EVERY = 25
DEF_TS_PREPARE_PROGRESS_EVERY = 50
MATCH_MISS_PREVIEW = 4
UNSUPPORTED_OPTION_CHAIN_SYMBOLS = {
    "NVR",
}

QUOTE_LOOKBACK = pd.Timedelta(hours=1)
TRADE_LOOKBACK = pd.Timedelta(minutes=10)
OPEN_INTEREST_LOOKBACK = pd.Timedelta(days=1)
MAX_QUOTE_AGE = QUOTE_LOOKBACK
DB_WRITE_SYMBOL_BATCH = 25
COMPLETE_STATUS = "COMPLETE"
THIRD_FRIDAY_SKIP_STATUS = "THIRD_FRIDAY_SKIP"
MONTHLY_ONLY_STATUS = "MONTHLY_ONLY"
COVERED_STATUSES = (
    COMPLETE_STATUS,
    THIRD_FRIDAY_SKIP_STATUS,
    MONTHLY_ONLY_STATUS,
)
MARKET_SESSION_COMPLETE_TIME = dt_time(16, 0)
NY_TZ = ZoneInfo("America/New_York")

QUOTE_LOOKBACK_NS = QUOTE_LOOKBACK.value
TRADE_LOOKBACK_NS = TRADE_LOOKBACK.value
OPEN_INTEREST_LOOKBACK_NS = OPEN_INTEREST_LOOKBACK.value


def filter_supported_option_chain_symbols(symbols: list[str]) -> list[str]:
    filtered_symbols: list[str] = []
    seen: set[str] = set()

    for symbol in symbols:
        if not symbol or not isinstance(symbol, str):
            continue

        normalized = symbol.strip().upper()
        if not normalized or normalized in seen:
            continue
        if normalized in UNSUPPORTED_OPTION_CHAIN_SYMBOLS:
            continue

        seen.add(normalized)
        filtered_symbols.append(normalized)

    return filtered_symbols


# ---------- TIME RANGE (Databento-safe end boundary) ----------
def db_end_utc_day() -> datetime:
    """
    Databento historical often seals availability at 00:00:00 UTC boundaries.
    Using 'now()' can exceed the available end and trigger 422.
    Clamp end to the start of the current UTC day.
    """
    return datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)


def get_existing_dates(symbols: list[str], days_back: int) -> dict[str, set]:
    if not symbols:
        return {}

    con = duckdb.connect(DB_PATH)
    try:
        cutoff = (db_end_utc_day() - timedelta(days=days_back)).date()
        status_placeholders = ", ".join(["?"] * len(COVERED_STATUSES))
        symbol_placeholders = ", ".join(["?"] * len(symbols))
        rows = con.execute(
            f"""
            SELECT parent_symbol, trade_date
            FROM backfill_status
            WHERE status IN ({status_placeholders})
              AND trade_date >= ?
              AND parent_symbol IN ({symbol_placeholders})
            """,
            [*COVERED_STATUSES, cutoff, *symbols],
        ).fetchall()

        existing_dates = {symbol: set() for symbol in symbols}
        for parent_symbol, trade_date in rows:
            existing_dates.setdefault(parent_symbol, set()).add(trade_date)
        return existing_dates
    finally:
        con.close()


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


def completed_market_session_end(dataset: str) -> datetime:
    """
    Exclusive UTC end for backfills based on completed U.S. equity market sessions.
    After the regular market close in New York, include the current market date.
    Before the close, exclude the current market date.
    """
    now_utc = datetime.now(timezone.utc)
    now_ny = now_utc.astimezone(NY_TZ)
    session_is_complete = now_ny.time() >= MARKET_SESSION_COMPLETE_TIME

    if session_is_complete:
        raw_end = now_utc
    else:
        raw_end = datetime.combine(now_ny.date(), dt_time.min, tzinfo=NY_TZ).astimezone(timezone.utc)

    return clamp_end(dataset, raw_end)


def last_completed_market_date() -> date:
    now_ny = datetime.now(timezone.utc).astimezone(NY_TZ)
    if now_ny.time() >= MARKET_SESSION_COMPLETE_TIME:
        return now_ny.date()
    return now_ny.date() - timedelta(days=1)


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
        con.execute("""
            CREATE TABLE IF NOT EXISTS backfill_status (
                parent_symbol TEXT,
                trade_date DATE,
                snapshot_rows INTEGER,
                volume_rows INTEGER,
                status TEXT
            );
        """)
    finally:
        con.close()


def delete_old_rows(days_back: int) -> None:
    cutoff = db_end_utc_day() - timedelta(days=days_back)
    cutoff_naive = cutoff.replace(tzinfo=None)
    cutoff_date = cutoff.date()

    con = duckdb.connect(DB_PATH)
    try:
        con.execute(
            """
            DELETE FROM option_snapshots_raw
            WHERE timestamp < ?
            """,
            [cutoff_naive],
        )
        con.execute(
            """
            DELETE FROM rolling_volume_history
            WHERE timestamp < ?
            """,
            [cutoff_naive],
        )
        con.execute(
            """
            DELETE FROM backfill_status
            WHERE trade_date < ?
            """,
            [cutoff_date],
        )
    finally:
        con.close()

    print(f"[INFO] deleted DB rows older than {cutoff_naive} UTC")


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


def _format_date_set(dates: set) -> str:
    if not dates:
        return "none"
    return ", ".join(d.isoformat() for d in sorted(dates))


def _format_date_preview(dates: set, limit: int = 5) -> str:
    if not dates:
        return "none"

    ordered = sorted(dates)
    preview = ", ".join(d.isoformat() for d in ordered[:limit])
    if len(ordered) > limit:
        preview += ", ..."
    return preview


def _to_utc_timestamp(x) -> pd.Timestamp:
    ts = pd.Timestamp(x)
    if ts.tzinfo is None:
        return ts.tz_localize("UTC")
    return ts.tz_convert("UTC")


def _trade_day_bounds_utc(start_day: date, end_day: date) -> tuple[pd.Timestamp, pd.Timestamp]:
    start_ts = pd.Timestamp(start_day).tz_localize("UTC")
    end_ts = pd.Timestamp(end_day).tz_localize("UTC") + pd.Timedelta(days=1)
    return start_ts, end_ts


def get_closest_strike(target: float, strikes: list[float]) -> float:
    if not strikes:
        raise RuntimeError("No strikes available.")
    return float(min(strikes, key=lambda s: abs(float(s) - float(target))))


def is_third_friday(d):
    return policy_is_third_friday(d)


def _nth_weekday_of_month(year: int, month: int, weekday: int, n: int) -> date:
    first = date(year, month, 1)
    offset = (weekday - first.weekday()) % 7
    return first + timedelta(days=offset + (n - 1) * 7)


def _last_weekday_of_month(year: int, month: int, weekday: int) -> date:
    if month == 12:
        last = date(year + 1, 1, 1) - timedelta(days=1)
    else:
        last = date(year, month + 1, 1) - timedelta(days=1)
    offset = (last.weekday() - weekday) % 7
    return last - timedelta(days=offset)


def _observed_fixed_holiday(year: int, month: int, day: int) -> date:
    holiday = date(year, month, day)
    if holiday.weekday() == 5:
        return holiday - timedelta(days=1)
    if holiday.weekday() == 6:
        return holiday + timedelta(days=1)
    return holiday


def _easter_sunday(year: int) -> date:
    a = year % 19
    b = year // 100
    c = year % 100
    d = b // 4
    e = b % 4
    f = (b + 8) // 25
    g = (b - f + 1) // 3
    h = (19 * a + b - d - g + 15) % 30
    i = c // 4
    k = c % 4
    l = (32 + 2 * e + 2 * i - h - k) % 7
    m = (a + 11 * h + 22 * l) // 451
    month = (h + l - 7 * m + 114) // 31
    day = ((h + l - 7 * m + 114) % 31) + 1
    return date(year, month, day)


@lru_cache(maxsize=None)
def _nyse_holidays(year: int) -> frozenset[date]:
    holidays = {
        _observed_fixed_holiday(year, 1, 1),
        _nth_weekday_of_month(year, 1, 0, 3),
        _nth_weekday_of_month(year, 2, 0, 3),
        _easter_sunday(year) - timedelta(days=2),
        _last_weekday_of_month(year, 5, 0),
        _observed_fixed_holiday(year, 6, 19),
        _observed_fixed_holiday(year, 7, 4),
        _nth_weekday_of_month(year, 9, 0, 1),
        _nth_weekday_of_month(year, 11, 3, 4),
        _observed_fixed_holiday(year, 12, 25),
    }

    next_new_year_observed = _observed_fixed_holiday(year + 1, 1, 1)
    if next_new_year_observed.year == year:
        holidays.add(next_new_year_observed)

    return frozenset(holidays)


def is_nyse_market_holiday(d: date) -> bool:
    return d in _nyse_holidays(d.year)


def weekly_expiration_anchor(expiration_date: date) -> date | None:
    if expiration_date.weekday() == 4:
        return expiration_date

    if expiration_date.weekday() == 3:
        friday = expiration_date + timedelta(days=1)
        if friday.weekday() == 4 and is_nyse_market_holiday(friday):
            return friday

    return None


def is_weekly_expiration_date(expiration_date: date, *, exclude_third_friday: bool = True) -> bool:
    anchor = weekly_expiration_anchor(expiration_date)
    if anchor is None:
        return False
    if exclude_third_friday and is_third_friday(anchor):
        return False
    return True


def has_any_weekly_expiration(expirations: list[str]) -> bool:
    parsed = _parse_expiration_dates(expirations)
    return any(is_weekly_expiration_date(exp_date, exclude_third_friday=True) for exp_date in parsed)


def get_friday_within_4_days(expirations: list[str], now_date):
    exp, _reason = get_eligible_expiration_with_reason(expirations, now_date)
    return exp


def _parse_expiration_dates(expirations: list[str]) -> list[date]:
    parsed = []
    for exp in expirations:
        try:
            parsed.append(datetime.strptime(str(exp), "%Y%m%d").date())
        except Exception:
            continue
    return sorted(set(parsed))


def summarize_expiration_inventory(expirations: list[str]) -> dict[str, int]:
    parsed = _parse_expiration_dates(expirations)
    weekly_expirations = [d for d in parsed if weekly_expiration_anchor(d) is not None]
    third_fridays = [d for d in weekly_expirations if is_third_friday(weekly_expiration_anchor(d))]
    weekly_non_third = [d for d in weekly_expirations if not is_third_friday(weekly_expiration_anchor(d))]
    return {
        "total": len(parsed),
        "weekly_expirations": len(weekly_expirations),
        "third_fridays": len(third_fridays),
        "weekly_non_third": len(weekly_non_third),
    }


def describe_expiration_inventory_skip(expirations: list[str]) -> str:
    inv = summarize_expiration_inventory(expirations)
    if inv["weekly_expirations"] == 0:
        return f"no weekly expirations | exp={inv['total']}"
    if inv["weekly_non_third"] == 0 and inv["third_fridays"] > 0:
        return f"only third-Friday expirations | weekly_exp={inv['weekly_expirations']}"
    if inv["weekly_non_third"] == 0:
        return f"no non-third-Friday weekly expirations | weekly_exp={inv['weekly_expirations']}"
    return (
        f"weekly_exp={inv['weekly_expirations']} weekly_non_third={inv['weekly_non_third']} "
        f"third_fridays={inv['third_fridays']}"
    )


def get_eligible_expiration_with_reason(expirations: list[str], now_date: date) -> tuple[str | None, str | None]:
    parsed = _parse_expiration_dates(expirations)
    weekly_expirations = [d for d in parsed if weekly_expiration_anchor(d) is not None]
    if not weekly_expirations:
        return None, "no weekly expirations"

    weekly_non_third = [d for d in weekly_expirations if not is_third_friday(weekly_expiration_anchor(d))]
    if not weekly_non_third:
        return None, "only third-Friday expirations"

    eligible = [
        d for d in weekly_non_third
        if 0 <= (d - now_date).days <= LOOKAHEAD_DAYS_DEFAULT
    ]
    if eligible:
        return eligible[0].strftime("%Y%m%d"), None

    in_window_expirations = [
        d for d in weekly_expirations
        if 0 <= (d - now_date).days <= LOOKAHEAD_DAYS_DEFAULT
    ]
    if in_window_expirations and all(is_third_friday(weekly_expiration_anchor(d)) for d in in_window_expirations):
        return None, "only third-Friday within lookahead"

    future_weeklies = [d for d in weekly_non_third if d >= now_date]
    if not future_weeklies:
        return None, "no future weekly expirations"

    return None, f"no weekly expiry within {LOOKAHEAD_DAYS_DEFAULT}d"


# ---------- UNDERLYING ----------
def fetch_last_days(symbols: list[str], days: int) -> dict[str, pd.DataFrame] | None:
    end = db_end_utc_day()
    start = end - timedelta(days=days)
    symbol_groups = [
        symbols[i:i + 40]
        for i in range(0, len(symbols), 40)
    ]
    cleaned: dict[str, pd.DataFrame] = {}

    for symbol_group in symbol_groups:
        df = yf.download(
            symbol_group,
            start=start,
            end=end,
            interval="1d",
            progress=False,
            auto_adjust=False,
        )
        if df is None or df.empty:
            continue

        if isinstance(df.columns, pd.MultiIndex):
            if "Open" not in df.columns.get_level_values(0):
                continue

            open_block = df["Open"]
            if isinstance(open_block, pd.Series):
                open_block = open_block.to_frame(name=symbol_group[0])

            for symbol in symbol_group:
                if symbol not in open_block.columns:
                    continue

                series = pd.to_numeric(open_block[symbol], errors="coerce").dropna()
                if series.empty:
                    continue

                cleaned[symbol] = series.astype(float).rename("underlying_price").to_frame().sort_index()
        else:
            if len(symbol_group) != 1 or "Open" not in df.columns:
                continue

            series = pd.to_numeric(df["Open"], errors="coerce").dropna()
            if series.empty:
                continue

            cleaned[symbol_group[0]] = series.astype(float).rename("underlying_price").to_frame().sort_index()

    return cleaned



def batch_definitions_for_all_symbols(symbols: list[str], days_back: int = 35):
    return create_raw_symbols_list(symbols, days_back)








# ---------- DEFINITIONS MAP + NEEDED SYMBOLS (FAST) ----------
def build_def_map(df_defs: pd.DataFrame) -> dict[tuple[float, str, date], str]:
    """
    Map (strike_f, side, exp_date) -> raw_symbol for fast lookup.
    """
    out = {}
    strike_col = "strike_f" if "strike_f" in df_defs.columns else "strike_price"
    exp_col = "exp_date" if "exp_date" in df_defs.columns else "expiration"

    for strike, side, exp_date, raw_symbol in zip(
        df_defs[strike_col],
        df_defs["instrument_class"],
        df_defs[exp_col],
        df_defs["raw_symbol"],
    ):
        if pd.isna(strike) or pd.isna(exp_date) or raw_symbol in (None, ""):
            continue
        if not isinstance(exp_date, date):
            exp_date = pd.Timestamp(exp_date).date()
        k = (float(strike), str(side), exp_date)
        out[k] = str(raw_symbol)
    return out


def build_needed_raw_symbols_from_map(
    open_price_schedule: pd.DataFrame,
    def_map: dict[tuple[float, str, date], str],
    strikes: list[float],
    expirations: list[str],
    daily_leg_map: dict[date, tuple[date, int, list[tuple[float, str]]]] | None = None,
) -> list[str]:
    """
    Pre-pass over your intraday timestamps using the same selection logic.
    Uses def_map for fast (strike, side, exp_date) -> raw_symbol.
    """
    if open_price_schedule is None or open_price_schedule.empty:
        return []

    return sorted(
        build_needed_raw_symbol_dates_from_map(
            open_price_schedule=open_price_schedule,
            def_map=def_map,
            strikes=strikes,
            expirations=expirations,
            daily_leg_map=daily_leg_map,
        )
    )


def build_needed_raw_symbol_dates_from_map(
    open_price_schedule: pd.DataFrame,
    def_map: dict[tuple[float, str, date], str],
    strikes: list[float],
    expirations: list[str],
    daily_leg_map: dict[date, tuple[date, int, list[tuple[float, str]]]] | None = None,
) -> dict[str, set[date]]:
    if open_price_schedule is None or open_price_schedule.empty:
        return {}

    if daily_leg_map is None:
        daily_leg_map = build_daily_leg_map(open_price_schedule, strikes, expirations)

    raw_dates: dict[str, set[date]] = {}
    for trade_date, (exp_date, _dte, strike_sides) in daily_leg_map.items():
        for strike, side in strike_sides:
            rs = def_map.get((float(strike), side, exp_date))
            if rs:
                raw_dates.setdefault(str(rs), set()).add(trade_date)

    return raw_dates


def summarize_leg_match_stats(
    daily_leg_map: dict[date, tuple[date, int, list[tuple[float, str]]]],
    def_map: dict[tuple[float, str, date], str],
    *,
    preview_limit: int = MATCH_MISS_PREVIEW,
) -> dict[str, object]:
    total_legs = 0
    matched_legs = 0
    missing_keys: list[str] = []

    for trade_date, (exp_date, _dte, strike_sides) in daily_leg_map.items():
        for strike, side in strike_sides:
            total_legs += 1
            key = (float(strike), str(side), exp_date)
            if key in def_map:
                matched_legs += 1
            elif len(missing_keys) < preview_limit:
                missing_keys.append(
                    f"{trade_date.isoformat()}:{exp_date.isoformat()}:{float(strike):g}:{side}"
                )

    return {
        "leg_days": len(daily_leg_map),
        "total_legs": total_legs,
        "matched_legs": matched_legs,
        "missing_preview": missing_keys,
    }


def build_daily_leg_map_with_stats(
    open_price_schedule: pd.DataFrame,
    strikes: list[float],
    expirations: list[str],
) -> tuple[dict[date, tuple[date, int, list[tuple[float, str]]]], dict[str, object]]:
    stats = {
        "source_days": 0,
        "built_days": 0,
        "skip_reason_counts": defaultdict(int),
        "skip_preview": [],
    }
    if open_price_schedule is None or open_price_schedule.empty:
        return {}, stats

    daily_leg_map = {}

    for ts, row in open_price_schedule.iterrows():
        now_date = ts.date()
        if now_date in daily_leg_map:
            continue

        stats["source_days"] += 1
        underlying_price = float(row["underlying_price"])
        exp, reason = get_eligible_expiration_with_reason(expirations, now_date)
        if exp is None:
            stats["skip_reason_counts"][reason] += 1
            if len(stats["skip_preview"]) < MATCH_MISS_PREVIEW:
                stats["skip_preview"].append(f"{now_date.isoformat()}:{reason}")
            continue

        exp_date = datetime.strptime(exp, "%Y%m%d").date()
        dte = (exp_date - now_date).days
        if dte < 0:
            stats["skip_reason_counts"]["negative dte"] += 1
            if len(stats["skip_preview"]) < MATCH_MISS_PREVIEW:
                stats["skip_preview"].append(f"{now_date.isoformat()}:negative dte")
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

    stats["built_days"] = len(daily_leg_map)
    return daily_leg_map, stats


def format_reason_counts(reason_counts: dict[str, int]) -> str:
    if not reason_counts:
        return "none"
    return ", ".join(f"{reason}={count}" for reason, count in sorted(reason_counts.items()))


def describe_empty_definition_reason(stats: dict[str, int]) -> str:
    if stats.get("raw_rows", 0) == 0:
        return "no definition rows returned"
    if stats.get("symbol_rows", 0) == 0:
        return "no rows for symbol"
    if stats.get("expiration_rows", 0) == 0:
        return "no rows for requested expiration"
    if stats.get("cp_rows", 0) == 0:
        return "no C/P rows for requested expiration"
    if stats.get("valid_rows", 0) == 0:
        return "no valid raw_symbol/strike rows"
    return "rows filtered to empty"


def _expiration_strings_from_dates(expiration_dates: set[date]) -> list[str]:
    return [d.strftime("%Y%m%d") for d in sorted(expiration_dates)]


def is_monthly_only_expiration_inventory(expiration_dates: set[date]) -> bool:
    if not expiration_dates:
        return False

    inv = summarize_expiration_inventory(_expiration_strings_from_dates(expiration_dates))
    return inv["third_fridays"] > 0 and inv["weekly_non_third"] == 0


def build_daily_leg_map(
    open_price_schedule: pd.DataFrame,
    strikes: list[float],
    expirations: list[str],
) -> dict[date, tuple[date, int, list[tuple[float, str]]]]:
    daily_leg_map, _stats = build_daily_leg_map_with_stats(open_price_schedule, strikes, expirations)
    return daily_leg_map


def _prepare_lookup_frame(df: pd.DataFrame, time_col: str | None) -> pd.DataFrame:
    if df.empty or not time_col or "symbol" not in df.columns:
        return pd.DataFrame()

    out = df[df["symbol"].notna() & df[time_col].notna()].copy()
    if out.empty:
        return out

    out = out.sort_values(["symbol", time_col]).copy()
    out["_ts_ns"] = pd.DatetimeIndex(out[time_col]).asi8
    return out


def build_market_lookup(df: pd.DataFrame) -> dict[str, dict[str, np.ndarray]]:
    required_cols = {"symbol", "_ts_ns", "bid_px_00", "ask_px_00"}
    if df.empty or not required_cols.issubset(df.columns):
        return {}

    out = {}
    for symbol, g in df.groupby("symbol", sort=False):
        out[str(symbol)] = {
            "times_ns": g["_ts_ns"].to_numpy(dtype=np.int64, copy=False),
            "bid": pd.to_numeric(g["bid_px_00"], errors="coerce").to_numpy(dtype=np.float64, copy=False),
            "ask": pd.to_numeric(g["ask_px_00"], errors="coerce").to_numpy(dtype=np.float64, copy=False),
        }
    return out


def build_trade_lookup(df: pd.DataFrame) -> dict[str, dict[str, np.ndarray]]:
    required_cols = {"symbol", "_ts_ns", "size"}
    if df.empty or not required_cols.issubset(df.columns):
        return {}

    out = {}
    for symbol, g in df.groupby("symbol", sort=False):
        sizes = pd.to_numeric(g["size"], errors="coerce").fillna(0.0).to_numpy(dtype=np.float64, copy=False)
        out[str(symbol)] = {
            "times_ns": g["_ts_ns"].to_numpy(dtype=np.int64, copy=False),
            "size_prefix": np.cumsum(sizes),
        }
    return out


def build_oi_lookup(df: pd.DataFrame) -> dict[str, dict[str, np.ndarray]]:
    required_cols = {"symbol", "_ts_ns", "open_interest"}
    if df.empty or not required_cols.issubset(df.columns):
        return {}

    out = {}
    for symbol, g in df.groupby("symbol", sort=False):
        out[str(symbol)] = {
            "times_ns": g["_ts_ns"].to_numpy(dtype=np.int64, copy=False),
            "open_interest": pd.to_numeric(g["open_interest"], errors="coerce").to_numpy(dtype=np.float64, copy=False),
        }
    return out


def _last_index_in_window(times_ns: np.ndarray, start_ns: int, end_ns: int) -> int:
    if times_ns.size == 0:
        return -1

    right = int(np.searchsorted(times_ns, end_ns, side="right")) - 1
    if right < 0 or times_ns[right] < start_ns:
        return -1
    return right


def _rolling_sum_from_prefix(times_ns: np.ndarray, prefix: np.ndarray, start_ns: int, end_ns: int) -> float:
    if times_ns.size == 0 or prefix.size == 0:
        return 0.0

    right = int(np.searchsorted(times_ns, end_ns, side="right")) - 1
    if right < 0:
        return 0.0

    left = int(np.searchsorted(times_ns, start_ns, side="left"))
    if left > right:
        return 0.0

    total = prefix[right]
    if left > 0:
        total -= prefix[left - 1]
    return float(total)


def get_contract_data_from_lookups_fast(
    strike_sides,
    days_to_expiry,
    def_map,
    exp_date,
    ts,
    underlying_price,
    mkt_lookup,
    trd_lookup,
    oi_lookup,
):
    """
    Uses precomputed per-symbol arrays so each lookup is searchsorted instead of DataFrame masking.
    """
    ts_ns = int(_to_utc_timestamp(ts).value)

    symbols = {}
    for strike, side in strike_sides:
        symbols[(strike, side)] = def_map.get((float(strike), str(side), exp_date))

    out = {}

    for strike, side in strike_sides:
        rs = symbols.get((strike, side))

        bid = ask = mid = spread = spread_pct = iv = None
        volume = 0.0
        open_interest = None

        if rs is None:
            out[(strike, side)] = (bid, ask, mid, open_interest, volume, iv, spread, spread_pct)
            continue

        oi_entry = oi_lookup.get(rs)
        if oi_entry is not None:
            idx = _last_index_in_window(
                oi_entry["times_ns"],
                ts_ns - OPEN_INTEREST_LOOKBACK_NS,
                ts_ns,
            )
            if idx >= 0:
                oi_val = oi_entry["open_interest"][idx]
                if np.isfinite(oi_val):
                    open_interest = float(oi_val)

        trd_entry = trd_lookup.get(rs)
        if trd_entry is not None:
            volume = _rolling_sum_from_prefix(
                trd_entry["times_ns"],
                trd_entry["size_prefix"],
                ts_ns - TRADE_LOOKBACK_NS,
                ts_ns,
            )

        mkt_entry = mkt_lookup.get(rs)
        if mkt_entry is None:
            out[(strike, side)] = (bid, ask, mid, open_interest, volume, iv, spread, spread_pct)
            continue

        idx = _last_index_in_window(
            mkt_entry["times_ns"],
            ts_ns - QUOTE_LOOKBACK_NS,
            ts_ns,
        )
        if idx < 0:
            out[(strike, side)] = (bid, ask, mid, open_interest, volume, iv, spread, spread_pct)
            continue

        bid_val = mkt_entry["bid"][idx]
        ask_val = mkt_entry["ask"][idx]
        bid = float(bid_val) if np.isfinite(bid_val) else 0.0
        ask = float(ask_val) if np.isfinite(ask_val) else 0.0

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


def wait_for_batch_job(batch_client, job_id: str, *, schema: str, symbol_count: int, poll_s: float = 2.0) -> None:
    last_state = None
    last_progress = None

    while True:
        jobs = batch_client.batch.list_jobs(states=["queued", "processing", "done", "expired"])
        details = next((job for job in jobs if job.get("id") == job_id), None)

        if details is None:
            time.sleep(poll_s)
            continue

        state = details.get("state")
        progress = details.get("progress")

        if state != last_state or progress != last_progress:
            if progress is None:
                print(f"[BATCH] {schema} job {job_id} ({symbol_count} symbols): state={state}")
            else:
                print(f"[BATCH] {schema} job {job_id} ({symbol_count} symbols): state={state} progress={progress}%")
            last_state = state
            last_progress = progress

        if state == "done":
            return

        if state == "expired":
            raise RuntimeError(f"Batch job expired: schema={schema} job_id={job_id}")

        time.sleep(poll_s)


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

    is_definition = schema == "definition"
    batch_client = db.Historical(DATABENTO_API_KEY)

    job = batch_client.batch.submit_job(
        dataset=dataset,
        start=_to_iso(start),
        end=_to_iso(end),
        symbols=symbols,
        schema=schema,
        split_duration=split_duration,
        stype_in=stype_in,
    )
    job_id = job["id"]
    print(f"[BATCH] submitted {schema} job {job_id} for {len(symbols)} symbols")
    wait_for_batch_job(batch_client, job_id, schema=schema, symbol_count=len(symbols), poll_s=poll_s)

    out_dir = BATCH_DIR / job_id
    out_dir.mkdir(parents=True, exist_ok=True)
    try:
        if is_definition:
            print(f"[BATCH] definition job {job_id}: download start")
        files = batch_client.batch.download(job_id=job_id, output_dir=out_dir)
        if is_definition:
            print(f"[BATCH] definition job {job_id}: download complete files={len(files)}")

        dbn_files = [f for f in sorted(files) if str(f).endswith(".dbn.zst")]
        if is_definition:
            print(f"[BATCH] definition job {job_id}: loading {len(dbn_files)} dbn file(s)")

        dfs = []
        for idx, f in enumerate(dbn_files, start=1):
            if is_definition:
                print(f"[BATCH] definition job {job_id}: load dbn {idx}/{len(dbn_files)}")
            store = db.DBNStore.from_file(f)
            dfs.append(store.to_df())

        return pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()
    finally:
        shutil.rmtree(out_dir, ignore_errors=True)


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


def is_bento_no_data_error(exc: Exception) -> bool:
    message = str(exc)
    return (
        isinstance(exc, BentoClientError)
        and "data_no_data_found_for_request" in message
    )


def run_post_definition_batch_request(request: dict) -> tuple[int, int, str, pd.DataFrame]:
    window_idx = request.get("window_idx", 1)
    total_windows = request.get("total_windows", 1)
    window_label = request.get("window_label", "all")
    chunk_idx = request["chunk_idx"]
    n_chunks = request["n_chunks"]
    schema = request["schema"]
    symbols = request["symbols"]

    print(
        f"[BATCH] window {window_idx}/{total_windows} {window_label} "
        f"chunk {chunk_idx}/{n_chunks} {schema}: symbols={len(symbols)}"
    )
    try:
        df = batch_get_df(
            dataset=request["dataset"],
            schema=schema,
            symbols=symbols,
            start=request["start"],
            end=request["end"],
            stype_in=request["stype_in"],
            split_duration=request["split_duration"],
            poll_s=request["poll_s"],
        )
    except Exception as exc:
        if is_bento_no_data_error(exc):
            print(
                f"⏭️ batch {schema} window {window_idx}/{total_windows} {window_label} "
                f"chunk {chunk_idx}/{n_chunks}: no data found for request | "
                f"symbols={len(symbols)}"
            )
            df = pd.DataFrame()
        else:
            raise
    return window_idx, chunk_idx, schema, df


def prepare_batch_result(schema: str, df: pd.DataFrame) -> pd.DataFrame:
    if df is None:
        df = pd.DataFrame()

    if schema == "statistics":
        if df.empty:
            return df

        if "stat_type" in df.columns:
            df = df[df["stat_type"] == db.StatType.OPEN_INTEREST].copy()

        if "quantity" in df.columns and "open_interest" not in df.columns:
            df = df.rename(columns={"quantity": "open_interest"})

        keep_cols = [c for c in ["symbol", "ts_event", "timestamp", "open_interest"] if c in df.columns]
        df = df[keep_cols].copy()

    tcol = "ts_event" if "ts_event" in df.columns else ("timestamp" if "timestamp" in df.columns else None)
    if tcol:
        _ensure_utc_col(df, tcol)

    return df


def collapse_trade_dates_to_windows(needed_dates: set[date]) -> list[tuple[date, date]]:
    ordered = sorted(needed_dates)
    if not ordered:
        return []

    windows = []
    window_start = ordered[0]
    window_end = ordered[0]

    for current in ordered[1:]:
        if (current - window_end).days <= 3:
            window_end = current
            continue

        windows.append((window_start, window_end))
        window_start = current
        window_end = current

    windows.append((window_start, window_end))
    return windows


def _schema_request_bounds(
    schema: str,
    start_day: date,
    end_day: date,
    end_batch,
) -> tuple[pd.Timestamp, pd.Timestamp] | None:
    start_ts, end_ts = _trade_day_bounds_utc(start_day, end_day)

    if schema == "statistics":
        start_ts -= OPEN_INTEREST_LOOKBACK
    elif schema == "cbbo-1s":
        start_ts -= QUOTE_LOOKBACK
    elif schema == "trades":
        start_ts -= TRADE_LOOKBACK

    end_cap = _to_utc_timestamp(end_batch)
    if end_ts > end_cap:
        end_ts = end_cap

    if start_ts >= end_ts:
        return None

    return start_ts, end_ts


def build_post_definition_requests(plans, end_batch) -> list[dict]:
    raw_windows: dict[tuple[date, date], set[str]] = defaultdict(set)

    for plan in plans.values():
        for raw_symbol, needed_dates in plan.get("raw_symbol_dates", {}).items():
            for start_day, end_day in collapse_trade_dates_to_windows(needed_dates):
                raw_windows[(start_day, end_day)].add(raw_symbol)

    if not raw_windows:
        return []

    requests = []
    sorted_windows = sorted(raw_windows.items(), key=lambda x: x[0])
    total_windows = len(sorted_windows)

    for window_idx, ((start_day, end_day), symbols_for_window) in enumerate(sorted_windows, start=1):
        window_symbols = sorted(symbols_for_window)
        n_chunks = (len(window_symbols) + MAX_SYMBOLS_PER_JOB - 1) // MAX_SYMBOLS_PER_JOB
        window_label = f"{start_day.isoformat()}..{end_day.isoformat()}"

        for chunk_idx, chunk in enumerate(chunk_list(window_symbols, MAX_SYMBOLS_PER_JOB), start=1):
            for schema in ("cbbo-1s", "trades", "statistics"):
                bounds = _schema_request_bounds(schema, start_day, end_day, end_batch)
                if bounds is None:
                    continue

                request_start, request_end = bounds
                requests.append({
                    "chunk_idx": chunk_idx,
                    "n_chunks": n_chunks,
                    "window_idx": window_idx,
                    "total_windows": total_windows,
                    "window_label": window_label,
                    "dataset": "OPRA.PILLAR",
                    "schema": schema,
                    "symbols": chunk,
                    "start": request_start,
                    "end": request_end,
                    "stype_in": "raw_symbol",
                    "split_duration": "day",
                    "poll_s": POLL_S,
                })

    return requests


def _prune_submit_times(submit_times: deque[float], now: float, window_s: float) -> None:
    while submit_times and now - submit_times[0] >= window_s:
        submit_times.popleft()


def _wait_for_submit_slot(
    submit_times: deque[float],
    last_submit_at: float | None,
    *,
    limit_count: int,
    window_s: float,
    min_spacing_s: float = 0.0,
) -> float:
    while True:
        now = time.monotonic()
        _prune_submit_times(submit_times, now, window_s)

        wait_for_window = 0.0
        if len(submit_times) >= limit_count:
            wait_for_window = window_s - (now - submit_times[0]) + 0.05

        wait_for_spacing = 0.0
        if last_submit_at is not None:
            wait_for_spacing = min_spacing_s - (now - last_submit_at)

        wait_s = max(wait_for_window, wait_for_spacing, 0.0)
        if wait_s <= 0:
            return time.monotonic()

        time.sleep(wait_s)


def run_post_definition_requests(requests: list[dict]) -> list[tuple[int, int, str, pd.DataFrame]]:
    if not requests:
        return []

    max_workers = min(POST_DEF_MAX_WORKERS, len(requests))
    print(
        f"[INFO] using bounded post-definition batching: "
        f"max_workers={max_workers} submit_rate<={BATCH_SUBMIT_RATE_LIMIT_PER_MIN}/min"
    )

    pending_requests = deque(requests)
    active_futures = {}
    submit_times: deque[float] = deque()
    last_submit_at: float | None = None
    results = []

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        while pending_requests or active_futures:
            while pending_requests and len(active_futures) < max_workers:
                last_submit_at = _wait_for_submit_slot(
                    submit_times,
                    last_submit_at,
                    limit_count=BATCH_SUBMIT_RATE_LIMIT_PER_MIN,
                    window_s=BATCH_SUBMIT_WINDOW_S,
                    min_spacing_s=BATCH_SUBMIT_MIN_SPACING_S,
                )
                request = pending_requests.popleft()
                future = executor.submit(run_post_definition_batch_request, request)
                active_futures[future] = request
                submit_times.append(last_submit_at)

            if not active_futures:
                continue

            done, _ = wait(active_futures, return_when=FIRST_COMPLETED)
            for future in done:
                request = active_futures.pop(future, None)
                try:
                    results.append(future.result())
                except Exception as exc:
                    schema = request["schema"] if request else "unknown"
                    symbols = len(request["symbols"]) if request else 0
                    window_label = request.get("window_label", "all") if request else "all"
                    print(
                        f"❌ batch {schema} window={window_label} symbols={symbols}: {exc}"
                    )

    return results


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


def append_status_row(results, parent_symbol, trade_date, snapshot_rows, volume_rows, status):
    results.append({
        "parent_symbol": parent_symbol,
        "trade_date": trade_date,
        "snapshot_rows": int(snapshot_rows),
        "volume_rows": int(volume_rows),
        "status": status,
    })


def append_status_rows_for_dates(
    results: list[dict],
    parent_symbol: str,
    trade_dates,
    status: str,
) -> int:
    count = 0
    for trade_date in sorted(set(trade_dates)):
        append_status_row(results, parent_symbol, trade_date, 0, 0, status)
        count += 1
    return count


def _unregister_view(con, name: str) -> None:
    try:
        con.unregister(name)
    except Exception:
        pass


def flush_results_to_db(con, snapshot_rows, volume_rows, status_rows) -> None:
    if not status_rows:
        return

    target_df = pd.DataFrame(status_rows)[["parent_symbol", "trade_date"]].drop_duplicates().copy()
    status_df = pd.DataFrame(status_rows).drop_duplicates(subset=["parent_symbol", "trade_date"], keep="last").copy()
    snapshot_df = pd.DataFrame(snapshot_rows)
    volume_df = pd.DataFrame(volume_rows)

    for df in (snapshot_df, volume_df):
        if not df.empty:
            df["timestamp"] = (
                pd.to_datetime(df["timestamp"], utc=True, errors="coerce")
                .dt.tz_convert("UTC")
                .dt.tz_localize(None)
            )

    registered_views = []
    con.execute("BEGIN")
    try:
        con.register("target_dates_view", target_df)
        registered_views.append("target_dates_view")

        con.execute("""
            DELETE FROM option_snapshots_raw
            WHERE EXISTS (
                SELECT 1
                FROM target_dates_view
                WHERE option_snapshots_raw.parent_symbol = target_dates_view.parent_symbol
                  AND DATE(option_snapshots_raw.timestamp) = target_dates_view.trade_date
            )
        """)
        con.execute("""
            DELETE FROM rolling_volume_history
            WHERE EXISTS (
                SELECT 1
                FROM target_dates_view
                WHERE rolling_volume_history.parent_symbol = target_dates_view.parent_symbol
                  AND DATE(rolling_volume_history.timestamp) = target_dates_view.trade_date
            )
        """)
        con.execute("""
            DELETE FROM backfill_status
            WHERE EXISTS (
                SELECT 1
                FROM target_dates_view
                WHERE backfill_status.parent_symbol = target_dates_view.parent_symbol
                  AND backfill_status.trade_date = target_dates_view.trade_date
            )
        """)

        if not snapshot_df.empty:
            con.register("snapshot_view", snapshot_df)
            registered_views.append("snapshot_view")
            snapshot_cols = ",".join(snapshot_df.columns)
            con.execute(f"INSERT INTO option_snapshots_raw({snapshot_cols}) SELECT {snapshot_cols} FROM snapshot_view")

        if not volume_df.empty:
            con.register("volume_view", volume_df)
            registered_views.append("volume_view")
            volume_cols = ",".join(volume_df.columns)
            con.execute(f"INSERT INTO rolling_volume_history({volume_cols}) SELECT {volume_cols} FROM volume_view")

        con.register("status_view", status_df)
        registered_views.append("status_view")
        status_cols = ",".join(status_df.columns)
        con.execute(f"INSERT INTO backfill_status({status_cols}) SELECT {status_cols} FROM status_view")

        con.execute("COMMIT")
    except Exception:
        con.execute("ROLLBACK")
        raise
    finally:
        for name in reversed(registered_views):
            _unregister_view(con, name)


def persist_status_rows(status_rows: list[dict]) -> int:
    if not status_rows:
        return 0

    con = duckdb.connect(DB_PATH)
    try:
        flush_results_to_db(con, [], [], status_rows)
    finally:
        con.close()

    return len({(row["parent_symbol"], row["trade_date"]) for row in status_rows})


# ---------- DEFINITIONS SNAPSHOT HELPERS ----------
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


def databento_symbol_key(symbol: str) -> str:
    return parent_to_underlying(str(symbol)).replace("-", "").replace(".", "").strip().upper()


def databento_parent_symbol(symbol: str) -> str:
    return f"{databento_symbol_key(symbol)}.OPT"


def get_weekly_expiration_for_trade_date(trade_date: date) -> date | None:
    exp_date, _reason = get_weekly_expiration_for_trade_date_with_reason(trade_date)
    return exp_date


def get_weekly_expiration_for_trade_date_with_reason(trade_date: date) -> tuple[date | None, str | None]:
    weekly_candidates = []
    for days_ahead in range(0, LOOKAHEAD_DAYS_DEFAULT + 1):
        candidate = trade_date + timedelta(days=days_ahead)
        anchor = weekly_expiration_anchor(candidate)
        if anchor is None:
            continue
        weekly_candidates.append((candidate, anchor))
        if is_third_friday(anchor):
            continue
        return candidate, None

    if weekly_candidates and all(is_third_friday(anchor) for _candidate, anchor in weekly_candidates):
        return None, "only third-Friday within lookahead"
    return None, f"no weekly expiry within {LOOKAHEAD_DAYS_DEFAULT}d"


def build_symbol_week_definition_requests(
    missing_dates_by_symbol: dict[str, set[date]],
    end_boundary,
) -> tuple[list[dict], list[dict], dict[str, int], dict[str, set[date]]]:
    requests = []
    skipped_status_rows: list[dict] = []
    skipped_reason_counts: dict[str, int] = defaultdict(int)
    skipped_dates_by_symbol: dict[str, set[date]] = defaultdict(set)
    end_cap = _to_utc_timestamp(end_boundary)

    for symbol in sorted(missing_dates_by_symbol):
        weekly_buckets: dict[date, set[date]] = defaultdict(set)
        for trade_date in sorted(missing_dates_by_symbol[symbol]):
            exp_date, skip_reason = get_weekly_expiration_for_trade_date_with_reason(trade_date)
            if exp_date is None:
                reason = skip_reason or "no eligible weekly-Friday bucket"
                skipped_reason_counts[reason] += 1
                if reason == "only third-Friday within lookahead":
                    append_status_row(
                        skipped_status_rows,
                        symbol,
                        trade_date,
                        0,
                        0,
                        THIRD_FRIDAY_SKIP_STATUS,
                    )
                    skipped_dates_by_symbol[symbol].add(trade_date)
                continue
            weekly_buckets[exp_date].add(trade_date)

        for exp_date, trade_dates in sorted(weekly_buckets.items()):
            snapshot_day = min(trade_dates)
            start_ts, end_ts = _trade_day_bounds_utc(snapshot_day, snapshot_day)
            request_end = min(end_ts, end_cap)
            if start_ts >= request_end:
                skipped_reason_counts["window clipped at end boundary"] += len(trade_dates)
                continue

            requests.append({
                "symbol": symbol,
                "parent": databento_parent_symbol(symbol),
                "expiration_date": exp_date,
                "snapshot_day": snapshot_day,
                "trade_dates": sorted(trade_dates),
                "start": start_ts,
                "end": request_end,
            })

    return requests, skipped_status_rows, dict(skipped_reason_counts), skipped_dates_by_symbol


def prepare_definition_snapshot(
    df: pd.DataFrame,
    *,
    symbol: str,
    expiration_date: date,
) -> pd.DataFrame:
    out, _stats = prepare_definition_snapshot_with_stats(
        df,
        symbol=symbol,
        expiration_date=expiration_date,
    )
    return out


def prepare_definition_snapshot_with_stats(
    df: pd.DataFrame,
    *,
    symbol: str,
    expiration_date: date,
) -> tuple[pd.DataFrame, dict[str, object]]:
    stats = {
        "raw_rows": 0,
        "symbol_rows": 0,
        "expiration_rows": 0,
        "cp_rows": 0,
        "valid_rows": 0,
        "final_rows": 0,
        "symbol_expiration_dates": set(),
    }
    if df is None or df.empty:
        return pd.DataFrame(), stats

    required_cols = {"underlying", "raw_symbol", "instrument_class", "strike_price", "expiration"}
    missing_cols = required_cols - set(df.columns)
    if missing_cols:
        raise RuntimeError(f"Missing definition columns for {symbol}: {sorted(missing_cols)}")

    time_col = "ts_event" if "ts_event" in df.columns else ("timestamp" if "timestamp" in df.columns else None)
    if time_col is None:
        raise RuntimeError(f"No timestamp-like definition column found for {symbol}. cols={list(df.columns)}")

    stats["raw_rows"] = len(df)
    keep_cols = [time_col, "underlying", "raw_symbol", "instrument_class", "strike_price", "expiration"]
    out = df[keep_cols].copy()
    out[time_col] = pd.to_datetime(out[time_col], utc=True, errors="coerce")
    symbol_key = databento_symbol_key(symbol)
    out["underlying_norm"] = out["underlying"].astype(str).map(databento_symbol_key)
    out["strike_f"] = pd.to_numeric(out["strike_price"], errors="coerce")
    out["exp_date"] = pd.to_datetime(out["expiration"], errors="coerce").dt.date
    out["exp_ymd"] = pd.to_datetime(out["expiration"], errors="coerce").dt.strftime("%Y%m%d")
    out["instrument_class"] = out["instrument_class"].astype(str)

    symbol_mask = out["underlying_norm"] == symbol_key
    stats["symbol_rows"] = int(symbol_mask.sum())
    stats["symbol_expiration_dates"] = set(out.loc[symbol_mask, "exp_date"].dropna().tolist())
    expiration_mask = out["exp_date"] == expiration_date
    stats["expiration_rows"] = int((symbol_mask & expiration_mask).sum())
    cp_mask = out["instrument_class"].isin(["C", "P"])
    stats["cp_rows"] = int((symbol_mask & expiration_mask & cp_mask).sum())

    out = out[symbol_mask & expiration_mask & cp_mask].copy()
    if out.empty:
        return out, stats

    stats["valid_rows"] = int(out[["raw_symbol", "strike_f", "exp_date"]].notna().all(axis=1).sum())
    out = out.dropna(subset=["raw_symbol", "strike_f", "exp_date"]).copy()
    if out.empty:
        return out, stats

    out = out.sort_values(time_col).drop_duplicates(subset=["raw_symbol"], keep="last").copy()
    stats["final_rows"] = len(out)
    return out, stats


def run_definition_timeseries_request(request: dict) -> tuple[dict, pd.DataFrame, str | None]:
    ts_client = db.Historical(DATABENTO_API_KEY)
    try:
        df = ts_client.timeseries.get_range(
            dataset="OPRA.PILLAR",
            schema="definition",
            symbols=[request["parent"]],
            stype_in="parent",
            start=request["start"],
            end=request["end"],
        ).to_df()
        return request, df, None
    except Exception as exc:
        return request, pd.DataFrame(), str(exc)


def run_definition_timeseries_requests(requests: list[dict]) -> list[tuple[dict, pd.DataFrame]]:
    if not requests:
        return []

    max_workers = min(DEF_TS_MAX_WORKERS, len(requests))
    print(f"[DEFS] requests={len(requests)} workers={max_workers} rate<={DEF_TS_RATE_LIMIT_COUNT}/{int(DEF_TS_RATE_LIMIT_WINDOW_S)}s")

    pending_requests = deque(requests)
    active_futures = {}
    submit_times: deque[float] = deque()
    last_submit_at: float | None = None
    submitted = 0
    completed = 0
    results: list[tuple[dict, pd.DataFrame]] = []

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        while pending_requests or active_futures:
            while pending_requests and len(active_futures) < max_workers:
                last_submit_at = _wait_for_submit_slot(
                    submit_times,
                    last_submit_at,
                    limit_count=DEF_TS_RATE_LIMIT_COUNT,
                    window_s=DEF_TS_RATE_LIMIT_WINDOW_S,
                )
                request = pending_requests.popleft()
                future = executor.submit(run_definition_timeseries_request, request)
                active_futures[future] = request
                submit_times.append(last_submit_at)
                submitted += 1
                if submitted % DEF_TS_SUBMIT_PROGRESS_EVERY == 0 or submitted == len(requests):
                    print(f"[DEFS] submitted {submitted}/{len(requests)}")

            if not active_futures:
                continue

            done, _ = wait(active_futures, return_when=FIRST_COMPLETED)
            for future in done:
                request = active_futures.pop(future)
                req, df, error = future.result()
                completed += 1
                if error:
                    print(
                        f"❌ defs {req['symbol']} exp={req['expiration_date'].isoformat()} "
                        f"snapshot={req['snapshot_day'].isoformat()}: {error}"
                    )
                else:
                    results.append((req, df))

                if completed % DEF_TS_PROGRESS_EVERY == 0 or completed == len(requests):
                    print(f"[DEFS] finished {completed}/{len(requests)}")

    return results


# ---------- ✅ SHARD TWO-PHASE (defs snapshots; data batched) ----------

def create_raw_symbols_list(symbols: list[str], days_back: int = 35):
    end = completed_market_session_end("OPRA.PILLAR")
    latest_available_trade_date = last_completed_market_date()

    daily_underlying = fetch_last_days(symbols, days_back)
    if not daily_underlying:
        print("[INFO] no symbols with valid underlying data")
        return [], {}

    union_raw: set[str] = set()

    eligible_syms = sorted(daily_underlying.keys())
    print(f"[INFO] underlying ok: {len(eligible_syms)} symbol(s). checking missing dates...")
    existing_dates_by_symbol = get_existing_dates(eligible_syms, days_back)

    missing_dates_by_symbol = {}
    total_target_dates = 0
    total_covered_dates = 0
    total_missing_dates = 0
    for sym in eligible_syms:
        target_dates = {
            ts.date()
            for ts in daily_underlying[sym].index
            if ts.date() <= latest_available_trade_date
        }
        existing_dates = existing_dates_by_symbol.get(sym, set())
        missing_dates = target_dates - existing_dates
        covered_dates = target_dates & existing_dates
        total_target_dates += len(target_dates)
        total_covered_dates += len(covered_dates)
        total_missing_dates += len(missing_dates)

        line = (
            f"[DATES] {sym}: target={len(target_dates)} "
            f"covered={len(covered_dates)} missing={len(missing_dates)}"
        )
        if missing_dates and len(missing_dates) <= 5:
            line += f" | missing_dates={_format_date_preview(missing_dates)}"
        print(line)

        if missing_dates:
            missing_dates_by_symbol[sym] = missing_dates

    if not missing_dates_by_symbol:
        print("[INFO] no missing dates found for any eligible symbol.")
        return [], {}

    print(
        f"[INFO] date coverage before definitions: "
        f"target={total_target_dates:,} covered={total_covered_dates:,} missing={total_missing_dates:,}"
    )

    requested_syms = sorted(missing_dates_by_symbol)
    planner_status_rows: list[dict] = []
    planner_status_counts: dict[str, int] = defaultdict(int)
    definition_requests, skipped_definition_status_rows, skipped_reason_counts, skipped_dates_by_symbol = build_symbol_week_definition_requests(
        missing_dates_by_symbol,
        end,
    )
    if skipped_reason_counts:
        skipped_definition_dates = sum(skipped_reason_counts.values())
        print(f"[INFO] missing dates without an eligible weekly-Friday bucket: {skipped_definition_dates:,}")
        print(f"[INFO] weekly-bucket skip reasons: {format_reason_counts(skipped_reason_counts)}")
    if skipped_definition_status_rows:
        planner_status_rows.extend(skipped_definition_status_rows)
        planner_status_counts[THIRD_FRIDAY_SKIP_STATUS] += len(skipped_definition_status_rows)
        print(
            f"[STATUS] queued {len(skipped_definition_status_rows):,} "
            f"{THIRD_FRIDAY_SKIP_STATUS} row(s)"
        )

    if not definition_requests:
        persisted = persist_status_rows(planner_status_rows)
        if persisted:
            print(f"[STATUS] persisted {persisted:,} planner status row(s)")
        print("[INFO] no symbol-week definition requests were generated.")
        return [], {}

    print(
        f"[INFO] requesting {len(definition_requests):,} definition snapshot(s) "
        f"across {len(requested_syms)} symbol(s) through market date {latest_available_trade_date.isoformat()}"
    )

    plans: dict[str, dict] = {}
    definition_results = run_definition_timeseries_requests(definition_requests)
    if not definition_results:
        persisted = persist_status_rows(planner_status_rows)
        if persisted:
            print(f"[STATUS] persisted {persisted:,} planner status row(s)")
        return [], {}

    definition_request_counts: dict[str, int] = defaultdict(int)
    for request in definition_requests:
        definition_request_counts[request["symbol"]] += 1

    symbol_definition_frames: dict[str, list[pd.DataFrame]] = defaultdict(list)
    symbol_raw_expiration_dates: dict[str, set[date]] = defaultdict(set)
    symbol_definition_stats: dict[str, dict[str, int]] = defaultdict(
        lambda: {
            "request_count": 0,
            "raw_rows": 0,
            "symbol_rows": 0,
            "expiration_rows": 0,
            "cp_rows": 0,
            "valid_rows": 0,
            "final_rows": 0,
        }
    )
    processed_definition_results = 0
    prepared_snapshots = 0
    empty_prepared_snapshots = 0
    prepared_rows = 0
    for request, df_defs_raw in definition_results:
        processed_definition_results += 1
        df_defs, prep_stats = prepare_definition_snapshot_with_stats(
            df_defs_raw,
            symbol=request["symbol"],
            expiration_date=request["expiration_date"],
        )
        sym_stats = symbol_definition_stats[request["symbol"]]
        sym_stats["request_count"] += 1
        for key in ("raw_rows", "symbol_rows", "expiration_rows", "cp_rows", "valid_rows", "final_rows"):
            sym_stats[key] += int(prep_stats[key])
        symbol_raw_expiration_dates[request["symbol"]].update(prep_stats.get("symbol_expiration_dates", set()))
        if df_defs.empty:
            empty_prepared_snapshots += 1
            if (
                processed_definition_results % DEF_TS_PREPARE_PROGRESS_EVERY == 0
                or processed_definition_results == len(definition_results)
            ):
                print(
                    f"[DEFS] prepare {processed_definition_results}/{len(definition_results)} "
                    f"nonempty={prepared_snapshots} empty={empty_prepared_snapshots} rows={prepared_rows:,}"
                )
            continue
        symbol_definition_frames[request["symbol"]].append(df_defs)
        prepared_snapshots += 1
        prepared_rows += len(df_defs)
        if (
            processed_definition_results % DEF_TS_PREPARE_PROGRESS_EVERY == 0
            or processed_definition_results == len(definition_results)
        ):
            print(
                f"[DEFS] prepare {processed_definition_results}/{len(definition_results)} "
                f"nonempty={prepared_snapshots} empty={empty_prepared_snapshots} rows={prepared_rows:,}"
            )

    print(
        f"[INFO] definition snapshots prepared: "
        f"nonempty={prepared_snapshots:,}/{len(definition_requests):,} "
        f"empty={empty_prepared_snapshots:,} rows={prepared_rows:,}"
    )

    for sym in requested_syms:
        missing_dates = missing_dates_by_symbol.get(sym, set())
        if not missing_dates:
            print(f"⏭️ {sym}: all last-{days_back}-day dates already present")
            continue

        third_friday_dates = skipped_dates_by_symbol.get(sym, set())
        working_missing_dates = missing_dates - third_friday_dates
        if not working_missing_dates:
            print(
                f"⏭️ {sym}: only third-Friday dates remain | "
                f"third_friday_dates={len(third_friday_dates)}"
            )
            continue

        open_price_schedule = daily_underlying[sym][
            pd.Index(daily_underlying[sym].index.date).isin(working_missing_dates)
        ].copy()

        if open_price_schedule.empty:
            print(f"⏭️ {sym}: no missing dates after filtering")
            continue

        frames = symbol_definition_frames.get(sym, [])
        sym_def_stats = symbol_definition_stats.get(sym, {})
        request_count = definition_request_counts.get(sym, 0)
        if not frames:
            raw_expiration_dates = symbol_raw_expiration_dates.get(sym, set())
            if is_monthly_only_expiration_inventory(raw_expiration_dates):
                marked_dates = append_status_rows_for_dates(
                    planner_status_rows,
                    sym,
                    working_missing_dates,
                    MONTHLY_ONLY_STATUS,
                )
                planner_status_counts[MONTHLY_ONLY_STATUS] += marked_dates
                print(
                    f"⏭️ {sym}: monthly only -> mark {marked_dates} date(s) | "
                    f"req={request_count} raw={sym_def_stats.get('raw_rows', 0):,} "
                    f"inventory={describe_expiration_inventory_skip(_expiration_strings_from_dates(raw_expiration_dates))}"
                )
                continue
            print(
                f"⏭️ {sym}: no options definitions ({describe_empty_definition_reason(sym_def_stats)}) | "
                f"req={request_count} raw={sym_def_stats.get('raw_rows', 0):,} "
                f"sym={sym_def_stats.get('symbol_rows', 0):,} "
                f"exp={sym_def_stats.get('expiration_rows', 0):,} "
                f"cp={sym_def_stats.get('cp_rows', 0):,} "
                f"valid={sym_def_stats.get('valid_rows', 0):,}"
            )
            continue

        df_defs = pd.concat(frames, ignore_index=True) if len(frames) > 1 else frames[0].copy()
        time_col = "ts_event" if "ts_event" in df_defs.columns else ("timestamp" if "timestamp" in df_defs.columns else None)
        if time_col:
            df_defs = df_defs.sort_values(time_col).drop_duplicates(subset=["raw_symbol"], keep="last").copy()

        print(
            f"[DEFS] {sym}: req={request_count} frames={len(frames)} "
            f"merged_rows={len(df_defs):,} missing_dates={len(working_missing_dates)}"
        )

        strikes = df_defs["strike_f"].dropna().unique().tolist()
        strikes.sort()
        if not strikes:
            print(f"⏭️ {sym}: no valid strikes | defs={len(df_defs):,}")
            continue

        expirations = df_defs["exp_ymd"].dropna().unique().tolist()
        if not has_any_weekly_expiration(expirations):
            print(
                f"⏭️ {sym}: {describe_expiration_inventory_skip(expirations)} -> skip | "
                f"defs={len(df_defs):,} expirations={len(expirations):,}"
            )
            continue

        def_map = build_def_map(df_defs)
        daily_leg_map, leg_build_stats = build_daily_leg_map_with_stats(open_price_schedule, strikes, expirations)
        if not daily_leg_map:
            preview = ", ".join(leg_build_stats["skip_preview"]) if leg_build_stats["skip_preview"] else "none"
            print(
                f"⏭️ {sym}: no daily legs | "
                f"days={leg_build_stats['built_days']}/{leg_build_stats['source_days']} "
                f"reasons={format_reason_counts(leg_build_stats['skip_reason_counts'])} "
                f"sample={preview}"
            )
            continue

        raw_symbol_dates = build_needed_raw_symbol_dates_from_map(
            open_price_schedule=open_price_schedule,
            def_map=def_map,
            strikes=strikes,
            expirations=expirations,
            daily_leg_map=daily_leg_map,
        )
        raw_needed = sorted(raw_symbol_dates)
        if not raw_needed:
            match_stats = summarize_leg_match_stats(daily_leg_map, def_map)
            miss_preview = ", ".join(match_stats["missing_preview"]) if match_stats["missing_preview"] else "none"
            print(
                f"⏭️ {sym}: no needed raw_symbols produced | "
                f"defs={len(df_defs):,} strikes={len(strikes):,} exp={len(expirations):,} "
                f"leg_days={match_stats['leg_days']} matched={match_stats['matched_legs']}/{match_stats['total_legs']} "
                f"miss={miss_preview}"
            )
            continue

        plans[sym] = {
            "open_price_schedule": open_price_schedule,
            "strikes": strikes,
            "expirations": expirations,
            "def_map": def_map,
            "daily_leg_map": daily_leg_map,
            "target_dates": sorted(daily_leg_map),
            "raw_symbol_dates": raw_symbol_dates,
            "raw_needed": set(raw_needed),
        }
        union_raw.update(raw_needed)
        match_stats = summarize_leg_match_stats(daily_leg_map, def_map)
        print(
            f"[PLAN] {sym}: defs={len(df_defs):,} strikes={len(strikes):,} "
            f"leg_days={match_stats['leg_days']} matched={match_stats['matched_legs']}/{match_stats['total_legs']} "
            f"raw_needed={len(raw_needed):,}"
        )

    persisted = persist_status_rows(planner_status_rows)
    if persisted:
        status_parts = [
            f"{status.lower()}={count:,}"
            for status, count in sorted(planner_status_counts.items())
            if count
        ]
        status_suffix = f" | {' '.join(status_parts)}" if status_parts else ""
        print(f"[STATUS] persisted {persisted:,} planner status row(s){status_suffix}")

    if not plans or not union_raw:
        print("[INFO] nothing to do (no plans / no raw symbols).")
        return [], {}

    union_raw_list = sorted(union_raw)
    print(f"[INFO] shard union raw_symbols={len(union_raw_list):,} -> windowed post-definition batches")
    return union_raw_list, plans





















def get_data(raw_symbols_list, plans, days_back: int = 35):
    if not raw_symbols_list or not plans:
        print("[INFO] nothing to backfill after planning.")
        return

    # ---------- PHASE 2: windowed post-definition data batches ----------
    end_batch = completed_market_session_end("OPRA.PILLAR")
    requests = build_post_definition_requests(plans, end_batch)
    if not requests:
        print("[INFO] no post-definition data requests were generated.")
        return

    total_jobs = len(requests)
    window_count = len({request["window_label"] for request in requests})
    schema_counts = defaultdict(int)
    for request in requests:
        schema_counts[request["schema"]] += 1
    print(
        f"[INFO] post-definition batch jobs={total_jobs} "
        f"across {window_count} window(s) for {len(raw_symbols_list):,} raw symbol(s)"
    )
    print(
        f"[DATA] job mix: cbbo-1s={schema_counts.get('cbbo-1s', 0)} "
        f"trades={schema_counts.get('trades', 0)} statistics={schema_counts.get('statistics', 0)}"
    )

    schema_order = {"cbbo-1s": 0, "trades": 1, "statistics": 2}
    results = run_post_definition_requests(requests)

    mkt_frames = []
    trd_frames = []
    oi_frames = []

    for _window_idx, _chunk_idx, schema, df_chunk in sorted(
        results,
        key=lambda x: (x[0], x[1], schema_order[x[2]]),
    ):
        df_chunk = prepare_batch_result(schema, df_chunk)

        if schema == "cbbo-1s" and not df_chunk.empty:
            mkt_frames.append(df_chunk)
        elif schema == "trades" and not df_chunk.empty:
            trd_frames.append(df_chunk)
        elif schema == "statistics" and not df_chunk.empty:
            oi_frames.append(df_chunk)

    mkt_df_all = pd.concat(mkt_frames, ignore_index=True) if mkt_frames else pd.DataFrame()
    trd_df_all = pd.concat(trd_frames, ignore_index=True) if trd_frames else pd.DataFrame()
    oi_df_all = pd.concat(oi_frames, ignore_index=True) if oi_frames else pd.DataFrame()
    print(
        f"[DATA] raw rows: quotes={len(mkt_df_all):,} trades={len(trd_df_all):,} oi={len(oi_df_all):,}"
    )

    mkt_tcol = "ts_event" if "ts_event" in mkt_df_all.columns else ("timestamp" if "timestamp" in mkt_df_all.columns else None)
    trd_tcol = "ts_event" if "ts_event" in trd_df_all.columns else ("timestamp" if "timestamp" in trd_df_all.columns else None)
    oi_tcol = "ts_event" if "ts_event" in oi_df_all.columns else ("timestamp" if "timestamp" in oi_df_all.columns else None)

    if not mkt_df_all.empty:
        mkt_df_all = mkt_df_all.drop_duplicates().copy()
    if not trd_df_all.empty:
        trd_df_all = trd_df_all.drop_duplicates().copy()
    if not oi_df_all.empty:
        oi_df_all = oi_df_all.drop_duplicates().copy()

    mkt_lookup = build_market_lookup(_prepare_lookup_frame(mkt_df_all, mkt_tcol))
    trd_lookup = build_trade_lookup(_prepare_lookup_frame(trd_df_all, trd_tcol))
    oi_lookup = build_oi_lookup(_prepare_lookup_frame(oi_df_all, oi_tcol))
    print(
        f"[DATA] lookup symbols: quotes={len(mkt_lookup):,} "
        f"trades={len(trd_lookup):,} oi={len(oi_lookup):,}"
    )

    # ---------- PHASE 3: per symbol compute using global dfs ----------
    snapshot_buffer = []
    volume_buffer = []
    status_buffer = []
    buffered_symbols = 0

    con = duckdb.connect(DB_PATH)
    try:
        for symbol, plan in plans.items():
            open_price_schedule = plan["open_price_schedule"]
            def_map = plan["def_map"]
            daily_leg_map = plan["daily_leg_map"]

            symbol_snapshot_results = []
            symbol_volume_results = []
            symbol_status_results = []

            for ts, row in open_price_schedule.iterrows():
                trade_date = ts.date()
                underlying_price = float(row["underlying_price"])
                daily_legs = daily_leg_map.get(trade_date)
                if daily_legs is None:
                    continue

                exp_date, days_till_expiry, strike_sides = daily_legs

                if days_till_expiry <= 1:
                    time_decay_bucket = "EXTREME"
                elif days_till_expiry <= 3:
                    time_decay_bucket = "HIGH"
                elif days_till_expiry <= 7:
                    time_decay_bucket = "MEDIUM"
                else:
                    time_decay_bucket = "LOW"

                out = get_contract_data_from_lookups_fast(
                    strike_sides,
                    days_till_expiry,
                    def_map,
                    exp_date,
                    ts,
                    underlying_price,
                    mkt_lookup,
                    trd_lookup,
                    oi_lookup,
                )

                snapshot_rows_for_day = 0
                volume_rows_for_day = 0
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
                        symbol_snapshot_results,
                        ts, symbol, underlying_price, strike, side,
                        days_till_expiry, exp_date, bucket, mid, iv,
                        time_decay_bucket
                    )
                    append_volume_row(
                        symbol_volume_results,
                        ts, symbol, side, days_till_expiry, bucket, vol,
                        time_decay_bucket
                    )
                    snapshot_rows_for_day += 1
                    volume_rows_for_day += 1

                append_status_row(
                    symbol_status_results,
                    symbol,
                    trade_date,
                    snapshot_rows_for_day,
                    volume_rows_for_day,
                    COMPLETE_STATUS,
                )

            if not symbol_status_results:
                print(f"⏭️ {symbol}: no rows produced")
                continue

            snapshot_buffer.extend(symbol_snapshot_results)
            volume_buffer.extend(symbol_volume_results)
            status_buffer.extend(symbol_status_results)
            buffered_symbols += 1

            print(
                f"✅ {symbol}: prepared {len(symbol_snapshot_results):,} snapshot rows and "
                f"{len(symbol_volume_results):,} volume rows across {len(symbol_status_results)} date(s)"
            )

            if buffered_symbols >= DB_WRITE_SYMBOL_BATCH:
                date_count = len(status_buffer)
                symbol_count = buffered_symbols
                flush_results_to_db(con, snapshot_buffer, volume_buffer, status_buffer)
                print(f"[DB] flushed {date_count:,} date(s) from {symbol_count} symbol(s)")
                snapshot_buffer.clear()
                volume_buffer.clear()
                status_buffer.clear()
                buffered_symbols = 0

        if status_buffer:
            date_count = len(status_buffer)
            symbol_count = buffered_symbols
            flush_results_to_db(con, snapshot_buffer, volume_buffer, status_buffer)
            print(f"[DB] flushed {date_count:,} date(s) from {symbol_count} symbol(s)")
    finally:
        con.close()





def wipe_batch_downloads():
    if BATCH_DIR.name != "batch_downloads":
        raise RuntimeError(f"Refusing to wipe unexpected dir: {BATCH_DIR}")
    if BATCH_DIR.exists():
        shutil.rmtree(BATCH_DIR, ignore_errors=True)
    BATCH_DIR.mkdir(parents=True, exist_ok=True)
def main():
    start_time = time.time()
    try:
        parser = argparse.ArgumentParser()
        parser.add_argument("--days-back", type=int, default=35)
        args = parser.parse_args()

        wipe_batch_downloads()
        ensure_table()
        delete_old_rows(args.days_back)

        raw_symbols = get_sp500_symbols()
        symbols = filter_supported_option_chain_symbols(raw_symbols)
        skipped_symbols = sorted({
            s.strip().upper()
            for s in raw_symbols
            if s and isinstance(s, str) and s.strip().upper() in UNSUPPORTED_OPTION_CHAIN_SYMBOLS
        })
        if skipped_symbols:
            print(
                f"[INFO] skipped unsupported option-chain symbols={len(skipped_symbols)} "
                f"symbols={', '.join(skipped_symbols)}"
            )

        print(f"[INFO] symbols={len(symbols)} days_back={args.days_back}")

        raw_symbols_list, plans = create_raw_symbols_list(symbols, args.days_back)
        get_data(raw_symbols_list, plans, args.days_back)
    finally:
        wipe_batch_downloads()
        elapsed = time.time() - start_time
        print(f"\n[INFO] total runtime: {elapsed:.2f} seconds")
  


if __name__ == "__main__":
    main()
