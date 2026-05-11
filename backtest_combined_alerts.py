"""Replay historical option data using the current live combined-alert logic."""

from __future__ import annotations

import argparse
import datetime as dt
import math
import time
import uuid
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import duckdb
import pandas as pd
import yfinance as yf


OPTIONS_DB_PATH = "options_data.db"
OUTPUT_DB_PATH = "backtest_results.duckdb"
REPORT_DIR = Path("backtest_reports")
ALERT_Z_THRESHOLD = 6.0
ALERT_COOLDOWN = dt.timedelta(minutes=12)
FIRST_ALERT_ONLY = True
QUOTE_CONFIRMATION_ENABLED = True
QUOTE_CONFIRM_WINDOW = dt.timedelta(minutes=3)
QUOTE_CONFIRM_MIN_MID_PCT = 0.03
QUOTE_CONFIRM_MIN_MID_ABS = 0.01
ALLOWED_SIGNAL_GROUPINGS = {"OTM1", "OTM2"}
ALLOWED_SIGNAL_DECAY_BUCKETS = {"HIGH", "EXTREME"}
SIGNAL_RULE_DESCRIPTION = (
    "volume-dominance: z_vol_35d >= 6, z_vol_3d >= 6, "
    "z_vol_35d > z_mid_35d and z_iv_35d, "
    "z_vol_3d > z_mid_3d and z_iv_3d; "
    "quote-confirmed within 3m: mid +3% and +0.01; "
    "quote-trigger only; first alert only; OTM1/OTM2 + HIGH/EXTREME"
)
UNDERLYING_CONFIRMATION_ENABLED = True
UNDERLYING_INTRADAY_INTERVAL = "5m"
UNDERLYING_LOOKBACK_MINUTES = 15
UNDERLYING_MAX_LOOKBACK_DAYS = 60
UNDERLYING_YF_SAFETY_DAYS = 10
STRATEGY_EXIT_MINUTES = 15
UNDERLYING_FAIL_TRIGGER = "close"
UNDERLYING_YF_BATCH_SIZE = 20
UNDERLYING_YF_MAX_ATTEMPTS = 3
UNDERLYING_YF_RETRY_DELAY_S = 1.0
MAX_FALLBACK_DAYS_TO_CHECK = 8
NY_TZ = ZoneInfo("America/New_York")
CHECKPOINTS = {
    "5m": dt.timedelta(minutes=5),
    "15m": dt.timedelta(minutes=15),
    "30m": dt.timedelta(minutes=30),
    "1h": dt.timedelta(hours=1),
}


def debug(message: str) -> None:
    print(f"[BACKTEST] {message}", flush=True)


@dataclass(frozen=True)
class UnderlyingBar:
    timestamp: dt.datetime
    open: float
    high: float
    low: float
    close: float
    volume: int | None = None


@dataclass(frozen=True)
class UnderlyingConfirmation:
    status: str
    direction: str | None = None
    breakout_level: float | None = None
    underlying_entry_price: float | None = None
    bar_timestamp: dt.datetime | None = None


def readable_utc_run_id(prefix: str, timestamp: dt.datetime) -> str:
    if timestamp.tzinfo is None:
        timestamp = timestamp.replace(tzinfo=dt.timezone.utc)
    timestamp = timestamp.astimezone(dt.timezone.utc)
    return f"{prefix}_{timestamp:%Y-%m-%d_%H-%M-%S}_UTC"


def ensure_utc_datetime(value: Any) -> dt.datetime:
    if isinstance(value, dt.datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=dt.timezone.utc)
        return value.astimezone(dt.timezone.utc)
    parsed = pd.Timestamp(value)
    if parsed.tzinfo is None:
        parsed = parsed.tz_localize("UTC")
    else:
        parsed = parsed.tz_convert("UTC")
    return parsed.to_pydatetime()


def parse_date(value: str) -> dt.date:
    return dt.date.fromisoformat(value)


def parse_parents(value: str | None) -> list[str] | None:
    if value is None:
        return None
    parents = [token.strip().upper() for token in value.split(",") if token.strip()]
    return parents or None


def debug_num(value: Any, digits: int = 2) -> str:
    try:
        if value is None:
            return "NA"
        return f"{float(value):.{digits}f}"
    except Exception:
        return "NA"


def fmt_pct(value: Any, digits: int = 2) -> str:
    try:
        if value is None:
            return "NA"
        return f"{float(value) * 100:.{digits}f}%"
    except Exception:
        return "NA"


def fmt_num(value: Any, digits: int = 4) -> str:
    try:
        if value is None:
            return "NA"
        return f"{float(value):.{digits}f}"
    except Exception:
        return "NA"


def fmt_rate(numerator: Any, denominator: Any, digits: int = 2) -> str:
    try:
        if numerator is None or denominator in (None, 0):
            return "NA"
        return f"{(float(numerator) / float(denominator)) * 100:.{digits}f}%"
    except Exception:
        return "NA"


def markdown_table(headers: list[str], rows: list[list[str]]) -> list[str]:
    if not rows:
        return ["No rows.", ""]

    lines = [
        "| " + " | ".join(headers) + " |",
        "| " + " | ".join(["---"] * len(headers)) + " |",
    ]
    for row in rows:
        lines.append("| " + " | ".join(row) + " |")
    lines.append("")
    return lines


def zscore_band(value: Any) -> str | None:
    try:
        if value is None:
            return None
        score = float(value)
    except Exception:
        return None
    if not math.isfinite(score):
        return None
    if score < 1.5:
        return "<1.5"
    if score < 3.0:
        return "1.5-3"
    if score < 5.0:
        return "3-5"
    if score < 10.0:
        return "5-10"
    return "10+"


def pct_return(current_value: float | None, start_value: float | None) -> float | None:
    if current_value is None or start_value is None or start_value == 0:
        return None
    return (float(current_value) - float(start_value)) / float(start_value)


def label_signal_return(option_return_pct: float | None) -> str:
    if option_return_pct is None:
        return "unknown"
    if option_return_pct >= 0.05:
        return "winner"
    if option_return_pct <= -0.05:
        return "loser"
    return "flat"


def apply_underlying_confirmation_date_limit(
    start_date: dt.date,
    end_date: dt.date,
) -> tuple[dt.date, dt.date]:
    if not UNDERLYING_CONFIRMATION_ENABLED:
        return start_date, end_date
    today_utc = dt.datetime.now(dt.timezone.utc).date()
    earliest_yf_date = today_utc - dt.timedelta(
        days=UNDERLYING_MAX_LOOKBACK_DAYS - UNDERLYING_YF_SAFETY_DAYS
    )
    limited_start = max(start_date, earliest_yf_date)
    limited_end = min(end_date, today_utc)
    if limited_end < limited_start:
        raise SystemExit(
            "No overlap between historical option dates and yfinance intraday "
            f"availability window starting {earliest_yf_date.isoformat()}."
        )
    return limited_start, limited_end


def completed_underlying_bars(
    bars: list[UnderlyingBar],
    timestamp: dt.datetime,
) -> list[UnderlyingBar]:
    timestamp = ensure_utc_datetime(timestamp)
    return [bar for bar in bars if bar.timestamp <= timestamp]


def evaluate_underlying_confirmation(
    bars: list[UnderlyingBar],
    *,
    side: str,
    alert_timestamp: dt.datetime,
) -> UnderlyingConfirmation:
    completed = completed_underlying_bars(bars, alert_timestamp)
    if not completed:
        return UnderlyingConfirmation(status="missing_underlying")

    entry_bar = completed[-1]
    window_start = entry_bar.timestamp - dt.timedelta(minutes=UNDERLYING_LOOKBACK_MINUTES)
    prior_bars = [
        bar
        for bar in completed
        if window_start <= bar.timestamp < entry_bar.timestamp
    ]
    if not prior_bars:
        return UnderlyingConfirmation(status="missing_underlying")

    side_value = str(side).upper()
    if side_value == "C":
        breakout_level = max(bar.high for bar in prior_bars)
        if entry_bar.close > breakout_level:
            return UnderlyingConfirmation(
                status="passed",
                direction="up",
                breakout_level=breakout_level,
                underlying_entry_price=entry_bar.close,
                bar_timestamp=entry_bar.timestamp,
            )
        return UnderlyingConfirmation(
            status="no_breakout",
            direction="up",
            breakout_level=breakout_level,
            underlying_entry_price=entry_bar.close,
            bar_timestamp=entry_bar.timestamp,
        )

    if side_value == "P":
        breakout_level = min(bar.low for bar in prior_bars)
        if entry_bar.close < breakout_level:
            return UnderlyingConfirmation(
                status="passed",
                direction="down",
                breakout_level=breakout_level,
                underlying_entry_price=entry_bar.close,
                bar_timestamp=entry_bar.timestamp,
            )
        return UnderlyingConfirmation(
            status="no_breakout",
            direction="down",
            breakout_level=breakout_level,
            underlying_entry_price=entry_bar.close,
            bar_timestamp=entry_bar.timestamp,
        )

    return UnderlyingConfirmation(status="missing_underlying")


def underlying_fail_triggered(
    *,
    side: str,
    close_price: float | None,
    breakout_level: float | None,
) -> bool:
    if close_price is None or breakout_level is None:
        return False
    side_value = str(side).upper()
    if side_value == "C":
        return float(close_price) < float(breakout_level)
    if side_value == "P":
        return float(close_price) > float(breakout_level)
    return False


def contract_symbol(
    parent_symbol: str,
    expiration_date: dt.date,
    strike: float,
    side: str,
) -> str:
    return f"{parent_symbol}|{expiration_date.isoformat()}|{float(strike):.3f}|{side}"


def combo_key(metadata: dict[str, Any]) -> tuple[str, str, str, str]:
    return (
        str(metadata["parent_symbol"]),
        str(metadata["side"]),
        str(metadata["grouping"]),
        str(metadata["decay_bucket"]),
    )


def volume_key(metadata: dict[str, Any], trade_date: dt.date) -> tuple[dt.date, str, str, str, str, int]:
    return (
        trade_date,
        str(metadata["parent_symbol"]),
        str(metadata["side"]),
        str(metadata["grouping"]),
        str(metadata["decay_bucket"]),
        int(metadata["days_to_expiry"]),
    )


def safe_zscore(
    value: Any,
    mean_value: Any,
    std_value: Any,
) -> float | None:
    if value is None or mean_value is None or std_value is None:
        return None
    try:
        value_f = float(value)
        mean_f = float(mean_value)
        std_f = float(std_value)
    except Exception:
        return None
    if not math.isfinite(value_f) or not math.isfinite(mean_f) or not math.isfinite(std_f) or std_f <= 0:
        return None
    return (value_f - mean_f) / std_f


def finite_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        value_f = float(value)
    except Exception:
        return None
    if not math.isfinite(value_f):
        return None
    return value_f


def passes_volume_dominance_rule(row: dict[str, Any]) -> bool:
    z_vol_35d = finite_float(row.get("z_vol_35d"))
    z_vol_3d = finite_float(row.get("z_vol_3d"))
    z_mid_35d = finite_float(row.get("z_mid_35d"))
    z_mid_3d = finite_float(row.get("z_mid_3d"))
    z_iv_35d = finite_float(row.get("z_iv_35d"))
    z_iv_3d = finite_float(row.get("z_iv_3d"))

    return (
        z_vol_35d is not None
        and z_vol_3d is not None
        and z_mid_35d is not None
        and z_mid_3d is not None
        and z_iv_35d is not None
        and z_iv_3d is not None
        and z_vol_35d >= ALERT_Z_THRESHOLD
        and z_vol_3d >= ALERT_Z_THRESHOLD
        and z_vol_35d > z_iv_35d
        and z_vol_35d > z_mid_35d
        and z_vol_3d > z_iv_3d
        and z_vol_3d > z_mid_3d
    )


def passes_signal_context_filter(row: dict[str, Any]) -> bool:
    return (
        str(row.get("grouping")) in ALLOWED_SIGNAL_GROUPINGS
        and str(row.get("decay_bucket")) in ALLOWED_SIGNAL_DECAY_BUCKETS
    )


def quote_confirmation_passed(reference_mid: Any, current_mid: Any) -> bool:
    reference_mid_f = finite_float(reference_mid)
    current_mid_f = finite_float(current_mid)
    if reference_mid_f is None or current_mid_f is None or reference_mid_f <= 0:
        return False
    mid_change_abs = current_mid_f - reference_mid_f
    mid_change_pct = mid_change_abs / reference_mid_f
    return (
        mid_change_abs >= QUOTE_CONFIRM_MIN_MID_ABS
        and mid_change_pct >= QUOTE_CONFIRM_MIN_MID_PCT
    )


def has_usable_stats(stats: dict[str, Any]) -> bool:
    return (
        stats["count"] >= 2
        and stats["mean"] is not None
        and stats["std"] is not None
        and float(stats["std"]) > 0
    )


def parent_filter_sql(parents: list[str] | None, column_name: str = "parent_symbol") -> tuple[str, list[Any]]:
    if not parents:
        return "", []
    placeholders = ", ".join(["?"] * len(parents))
    return f" AND {column_name} IN ({placeholders})", list(parents)


def ensure_columns(
    con: duckdb.DuckDBPyConnection,
    table_name: str,
    columns: dict[str, str],
) -> None:
    existing = {
        str(row[1])
        for row in con.execute(f"PRAGMA table_info('{table_name}')").fetchall()
    }
    for column_name, column_type in columns.items():
        if column_name not in existing:
            con.execute(f"ALTER TABLE {table_name} ADD COLUMN {column_name} {column_type}")


def ensure_output_tables(con: duckdb.DuckDBPyConnection) -> None:
    con.execute("""
        CREATE TABLE IF NOT EXISTS backtest_runs (
            run_id TEXT,
            started_at TIMESTAMP,
            completed_at TIMESTAMP,
            start_date DATE,
            end_date DATE,
            parents_filter TEXT,
            z_threshold DOUBLE,
            cooldown_minutes INTEGER,
            trade_days INTEGER,
            alerts_total INTEGER,
            outcomes_total INTEGER
        );
    """)
    con.execute("""
        CREATE TABLE IF NOT EXISTS backtest_signal_events (
            run_id TEXT,
            signal_id TEXT,
            trade_date DATE,
            alert_timestamp TIMESTAMP,
            event_source TEXT,
            alert_type TEXT,
            parent_symbol TEXT,
            contract_symbol TEXT,
            strike DOUBLE,
            expiration_date DATE,
            side TEXT,
            grouping TEXT,
            moneyness_grouping TEXT,
            decay_bucket TEXT,
            threshold DOUBLE,
            metric_value DOUBLE,
            z_35d DOUBLE,
            z_3d DOUBLE,
            option_mid DOUBLE,
            underlying_price DOUBLE,
            current_iv DOUBLE,
            rolling_volume_10m BIGINT,
            rolling_volume_30m BIGINT,
            rolling_volume_1h BIGINT,
            z_vol_35d DOUBLE,
            z_vol_3d DOUBLE,
            z_mid_35d DOUBLE,
            z_mid_3d DOUBLE,
            z_iv_35d DOUBLE,
            z_iv_3d DOUBLE,
            z_vol_35d_band TEXT,
            z_vol_3d_band TEXT,
            z_mid_35d_band TEXT,
            z_mid_3d_band TEXT,
            z_iv_35d_band TEXT,
            z_iv_3d_band TEXT,
            source_vol_3d TEXT,
            source_mid_3d TEXT,
            source_iv_3d TEXT,
            breakout_direction TEXT,
            breakout_level DOUBLE,
            underlying_entry_price DOUBLE,
            quote_reference_mid DOUBLE,
            quote_confirm_mid_change_abs DOUBLE,
            quote_confirm_mid_change_pct DOUBLE,
            quote_confirm_seconds DOUBLE,
            alert_message TEXT
        );
    """)
    con.execute("""
        CREATE TABLE IF NOT EXISTS backtest_signal_checkpoints (
            run_id TEXT,
            signal_id TEXT,
            trade_date DATE,
            alert_timestamp TIMESTAMP,
            checkpoint_label TEXT,
            due_timestamp TIMESTAMP,
            captured_timestamp TIMESTAMP,
            contract_symbol TEXT,
            parent_symbol TEXT,
            strike DOUBLE,
            expiration_date DATE,
            side TEXT,
            grouping TEXT,
            moneyness_grouping TEXT,
            decay_bucket TEXT,
            option_mid DOUBLE,
            option_quote_timestamp TIMESTAMP,
            option_quote_age_seconds DOUBLE,
            underlying_price DOUBLE,
            option_return_pct DOUBLE,
            underlying_return_pct DOUBLE,
            status TEXT
        );
    """)
    con.execute("""
        CREATE TABLE IF NOT EXISTS backtest_signal_outcomes (
            run_id TEXT,
            signal_id TEXT,
            trade_date DATE,
            alert_timestamp TIMESTAMP,
            finalized_timestamp TIMESTAMP,
            parent_symbol TEXT,
            contract_symbol TEXT,
            strike DOUBLE,
            expiration_date DATE,
            side TEXT,
            grouping TEXT,
            moneyness_grouping TEXT,
            decay_bucket TEXT,
            z_vol_35d_band TEXT,
            z_vol_3d_band TEXT,
            z_mid_35d_band TEXT,
            z_mid_3d_band TEXT,
            z_iv_35d_band TEXT,
            z_iv_3d_band TEXT,
            option_return_5m DOUBLE,
            option_return_15m DOUBLE,
            option_return_30m DOUBLE,
            option_return_1h DOUBLE,
            label_15m TEXT,
            label_30m TEXT,
            label_1h TEXT,
            max_up_pct DOUBLE,
            max_down_pct DOUBLE,
            mfe_pct DOUBLE,
            mae_pct DOUBLE,
            best_option_mid DOUBLE,
            best_option_mid_timestamp TIMESTAMP,
            worst_option_mid DOUBLE,
            worst_option_mid_timestamp TIMESTAMP,
            breakout_direction TEXT,
            breakout_level DOUBLE,
            underlying_entry_price DOUBLE,
            strategy_exit_timestamp TIMESTAMP,
            strategy_exit_reason TEXT,
            strategy_exit_option_mid DOUBLE,
            strategy_exit_underlying_price DOUBLE,
            strategy_return_pct DOUBLE
        );
    """)
    con.execute("""
        CREATE TABLE IF NOT EXISTS underlying_intraday_cache (
            parent_symbol TEXT,
            trade_date DATE,
            interval TEXT,
            timestamp TIMESTAMP,
            open DOUBLE,
            high DOUBLE,
            low DOUBLE,
            close DOUBLE,
            volume BIGINT,
            fetched_at TIMESTAMP
        );
    """)
    con.execute("""
        CREATE TABLE IF NOT EXISTS underlying_intraday_cache_status (
            parent_symbol TEXT,
            trade_date DATE,
            interval TEXT,
            status TEXT,
            rows_count INTEGER,
            fetched_at TIMESTAMP,
            message TEXT
        );
    """)
    con.execute("""
        CREATE TABLE IF NOT EXISTS backtest_signal_filter_stats (
            run_id TEXT,
            trade_date DATE,
            option_rule_candidates INTEGER,
            skipped_not_first_alert INTEGER,
            skipped_context_filter INTEGER,
            quote_confirmation_pending INTEGER,
            passed_quote_confirmation INTEGER,
            skipped_missing_reference_quote INTEGER,
            skipped_quote_confirmation INTEGER,
            skipped_quote_confirmation_expired INTEGER,
            passed_underlying_confirmation INTEGER,
            skipped_missing_underlying INTEGER,
            skipped_no_breakout INTEGER
        );
    """)
    ensure_columns(con, "backtest_signal_events", {
        "breakout_direction": "TEXT",
        "breakout_level": "DOUBLE",
        "underlying_entry_price": "DOUBLE",
        "quote_reference_mid": "DOUBLE",
        "quote_confirm_mid_change_abs": "DOUBLE",
        "quote_confirm_mid_change_pct": "DOUBLE",
        "quote_confirm_seconds": "DOUBLE",
    })
    ensure_columns(con, "backtest_signal_outcomes", {
        "breakout_direction": "TEXT",
        "breakout_level": "DOUBLE",
        "underlying_entry_price": "DOUBLE",
        "strategy_exit_timestamp": "TIMESTAMP",
        "strategy_exit_reason": "TEXT",
        "strategy_exit_option_mid": "DOUBLE",
        "strategy_exit_underlying_price": "DOUBLE",
        "strategy_return_pct": "DOUBLE",
    })
    ensure_columns(con, "backtest_signal_filter_stats", {
        "skipped_not_first_alert": "INTEGER",
        "skipped_context_filter": "INTEGER",
        "quote_confirmation_pending": "INTEGER",
        "passed_quote_confirmation": "INTEGER",
        "skipped_missing_reference_quote": "INTEGER",
        "skipped_quote_confirmation": "INTEGER",
        "skipped_quote_confirmation_expired": "INTEGER",
    })


def insert_run_started(
    con: duckdb.DuckDBPyConnection,
    *,
    run_id: str,
    started_at: dt.datetime,
    start_date: dt.date,
    end_date: dt.date,
    parents: list[str] | None,
) -> None:
    con.execute(
        """
        INSERT INTO backtest_runs (
            run_id,
            started_at,
            completed_at,
            start_date,
            end_date,
            parents_filter,
            z_threshold,
            cooldown_minutes,
            trade_days,
            alerts_total,
            outcomes_total
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            run_id,
            started_at.replace(tzinfo=None),
            None,
            start_date,
            end_date,
            ",".join(parents) if parents else None,
            ALERT_Z_THRESHOLD,
            int(ALERT_COOLDOWN.total_seconds() // 60),
            0,
            0,
            0,
        ],
    )


def finalize_run(
    con: duckdb.DuckDBPyConnection,
    *,
    run_id: str,
    completed_at: dt.datetime,
    trade_days: int,
    alerts_total: int,
    outcomes_total: int,
) -> None:
    con.execute(
        """
        UPDATE backtest_runs
        SET completed_at = ?,
            trade_days = ?,
            alerts_total = ?,
            outcomes_total = ?
        WHERE run_id = ?
        """,
        [
            completed_at.replace(tzinfo=None),
            trade_days,
            alerts_total,
            outcomes_total,
            run_id,
        ],
    )


def insert_filter_stats(
    con: duckdb.DuckDBPyConnection,
    *,
    run_id: str,
    trade_date: dt.date,
    stats: dict[str, int],
) -> None:
    con.execute(
        """
        DELETE FROM backtest_signal_filter_stats
        WHERE run_id = ? AND trade_date = ?
        """,
        [run_id, trade_date],
    )
    con.execute(
        """
        INSERT INTO backtest_signal_filter_stats (
            run_id,
            trade_date,
            option_rule_candidates,
            skipped_not_first_alert,
            skipped_context_filter,
            quote_confirmation_pending,
            passed_quote_confirmation,
            skipped_missing_reference_quote,
            skipped_quote_confirmation,
            skipped_quote_confirmation_expired,
            passed_underlying_confirmation,
            skipped_missing_underlying,
            skipped_no_breakout
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            run_id,
            trade_date,
            int(stats.get("option_rule_candidates", 0)),
            int(stats.get("skipped_not_first_alert", 0)),
            int(stats.get("skipped_context_filter", 0)),
            int(stats.get("quote_confirmation_pending", 0)),
            int(stats.get("passed_quote_confirmation", 0)),
            int(stats.get("skipped_missing_reference_quote", 0)),
            int(stats.get("skipped_quote_confirmation", 0)),
            int(stats.get("skipped_quote_confirmation_expired", 0)),
            int(stats.get("passed_underlying_confirmation", 0)),
            int(stats.get("skipped_missing_underlying", 0)),
            int(stats.get("skipped_no_breakout", 0)),
        ],
    )


def _split_batches(values: list[str], size: int) -> list[list[str]]:
    return [values[i:i + size] for i in range(0, len(values), size)]


def _normalize_yf_index_to_utc(index: pd.Index) -> pd.DatetimeIndex:
    ts_index = pd.to_datetime(index)
    if ts_index.tz is None:
        ts_index = ts_index.tz_localize(NY_TZ)
    return ts_index.tz_convert("UTC")


def _extract_yf_symbol_frame(df: pd.DataFrame, symbol: str, all_symbols: list[str]) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame()

    if isinstance(df.columns, pd.MultiIndex):
        if symbol in df.columns.get_level_values(0):
            out = df[symbol].copy()
        elif symbol in df.columns.get_level_values(1):
            out = df.xs(symbol, axis=1, level=1).copy()
        else:
            return pd.DataFrame()
    elif len(all_symbols) == 1:
        out = df.copy()
    else:
        return pd.DataFrame()

    out.columns = [str(col).lower() for col in out.columns]
    required = {"open", "high", "low", "close"}
    if not required.issubset(set(out.columns)):
        return pd.DataFrame()
    if "volume" not in out.columns:
        out["volume"] = None

    out = out[["open", "high", "low", "close", "volume"]].copy()
    out.index = _normalize_yf_index_to_utc(out.index)
    for col in ["open", "high", "low", "close"]:
        out[col] = pd.to_numeric(out[col], errors="coerce")
    out["volume"] = pd.to_numeric(out["volume"], errors="coerce")
    out = out.dropna(subset=["open", "high", "low", "close"])
    return out.sort_index()


def download_underlying_intraday_bars(
    symbols: list[str],
    trade_date: dt.date,
) -> dict[str, pd.DataFrame]:
    if not symbols:
        return {}

    start = dt.datetime.combine(trade_date, dt.time.min, tzinfo=dt.timezone.utc)
    end = start + dt.timedelta(days=1)
    tickers: list[str] | str = symbols if len(symbols) > 1 else symbols[0]
    last_error: Exception | None = None

    for attempt in range(1, UNDERLYING_YF_MAX_ATTEMPTS + 1):
        try:
            df = yf.download(
                tickers,
                start=start,
                end=end,
                interval=UNDERLYING_INTRADAY_INTERVAL,
                prepost=False,
                progress=False,
                auto_adjust=False,
                threads=False,
                group_by="ticker",
            )
            out = {
                symbol: _extract_yf_symbol_frame(df, symbol, symbols)
                for symbol in symbols
            }
            return out
        except Exception as exc:
            last_error = exc
            if attempt < UNDERLYING_YF_MAX_ATTEMPTS:
                sleep_s = UNDERLYING_YF_RETRY_DELAY_S * attempt
                debug(
                    f"yf underlying retry {attempt}/{UNDERLYING_YF_MAX_ATTEMPTS} "
                    f"date={trade_date} symbols={len(symbols)} error={exc}"
                )
                time.sleep(sleep_s)

    debug(f"yf underlying failed date={trade_date} symbols={len(symbols)} error={last_error}")
    return {}


def _bars_from_frame(df: pd.DataFrame) -> list[UnderlyingBar]:
    bars: list[UnderlyingBar] = []
    if df is None or df.empty:
        return bars
    for timestamp, row in df.iterrows():
        volume_value = row.get("volume")
        bars.append(
            UnderlyingBar(
                timestamp=ensure_utc_datetime(timestamp),
                open=float(row["open"]),
                high=float(row["high"]),
                low=float(row["low"]),
                close=float(row["close"]),
                volume=int(volume_value) if pd.notna(volume_value) else None,
            )
        )
    return bars


def load_cached_underlying_bars(
    con: duckdb.DuckDBPyConnection,
    *,
    parents: list[str],
    trade_date: dt.date,
) -> tuple[dict[str, list[UnderlyingBar]], list[str]]:
    if not parents:
        return {}, []

    placeholders = ", ".join(["?"] * len(parents))
    status_rows = con.execute(
        f"""
        SELECT parent_symbol, status
        FROM underlying_intraday_cache_status
        WHERE trade_date = ?
          AND interval = ?
          AND parent_symbol IN ({placeholders})
          AND status IN ('OK', 'EMPTY')
        """,
        [trade_date, UNDERLYING_INTRADAY_INTERVAL, *parents],
    ).fetchall()
    covered = {str(parent_symbol) for parent_symbol, _status in status_rows}

    bars_by_parent: dict[str, list[UnderlyingBar]] = {parent: [] for parent in covered}
    if covered:
        covered_list = sorted(covered)
        covered_placeholders = ", ".join(["?"] * len(covered_list))
        rows = con.execute(
            f"""
            SELECT parent_symbol, timestamp, open, high, low, close, volume
            FROM underlying_intraday_cache
            WHERE trade_date = ?
              AND interval = ?
              AND parent_symbol IN ({covered_placeholders})
            ORDER BY parent_symbol, timestamp
            """,
            [trade_date, UNDERLYING_INTRADAY_INTERVAL, *covered_list],
        ).fetchall()
        for parent_symbol, timestamp, open_price, high, low, close, volume in rows:
            bars_by_parent.setdefault(str(parent_symbol), []).append(
                UnderlyingBar(
                    timestamp=ensure_utc_datetime(timestamp),
                    open=float(open_price),
                    high=float(high),
                    low=float(low),
                    close=float(close),
                    volume=int(volume) if volume is not None else None,
                )
            )

    missing = [parent for parent in parents if parent not in covered]
    return bars_by_parent, missing


def persist_underlying_bars(
    con: duckdb.DuckDBPyConnection,
    *,
    trade_date: dt.date,
    frames_by_parent: dict[str, pd.DataFrame],
) -> dict[str, list[UnderlyingBar]]:
    fetched_at = dt.datetime.now(dt.timezone.utc).replace(tzinfo=None)
    bars_by_parent: dict[str, list[UnderlyingBar]] = {}

    for parent_symbol, df in frames_by_parent.items():
        con.execute(
            """
            DELETE FROM underlying_intraday_cache
            WHERE parent_symbol = ? AND trade_date = ? AND interval = ?
            """,
            [parent_symbol, trade_date, UNDERLYING_INTRADAY_INTERVAL],
        )
        con.execute(
            """
            DELETE FROM underlying_intraday_cache_status
            WHERE parent_symbol = ? AND trade_date = ? AND interval = ?
            """,
            [parent_symbol, trade_date, UNDERLYING_INTRADAY_INTERVAL],
        )

        bars = _bars_from_frame(df)
        bars_by_parent[parent_symbol] = bars
        if bars:
            rows = [
                {
                    "parent_symbol": parent_symbol,
                    "trade_date": trade_date,
                    "interval": UNDERLYING_INTRADAY_INTERVAL,
                    "timestamp": bar.timestamp.replace(tzinfo=None),
                    "open": bar.open,
                    "high": bar.high,
                    "low": bar.low,
                    "close": bar.close,
                    "volume": bar.volume,
                    "fetched_at": fetched_at,
                }
                for bar in bars
            ]
            rows_df = pd.DataFrame(rows)
            con.register("_underlying_cache_rows", rows_df)
            try:
                con.execute(
                    """
                    INSERT INTO underlying_intraday_cache
                    SELECT
                        parent_symbol,
                        trade_date,
                        interval,
                        timestamp,
                        open,
                        high,
                        low,
                        close,
                        volume,
                        fetched_at
                    FROM _underlying_cache_rows
                    """
                )
            finally:
                con.unregister("_underlying_cache_rows")

        con.execute(
            """
            INSERT INTO underlying_intraday_cache_status (
                parent_symbol,
                trade_date,
                interval,
                status,
                rows_count,
                fetched_at,
                message
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            [
                parent_symbol,
                trade_date,
                UNDERLYING_INTRADAY_INTERVAL,
                "OK" if bars else "EMPTY",
                len(bars),
                fetched_at,
                None if bars else "empty yfinance response",
            ],
        )

    return bars_by_parent


def ensure_underlying_bars_for_day(
    con: duckdb.DuckDBPyConnection,
    *,
    parents: list[str],
    trade_date: dt.date,
) -> dict[str, list[UnderlyingBar]]:
    if not UNDERLYING_CONFIRMATION_ENABLED:
        return {}

    unique_parents = sorted(set(parents))
    cached, missing = load_cached_underlying_bars(
        con,
        parents=unique_parents,
        trade_date=trade_date,
    )
    if missing:
        debug(f"underlying cache miss date={trade_date} parents={len(missing)}")
    for batch in _split_batches(missing, UNDERLYING_YF_BATCH_SIZE):
        frames = download_underlying_intraday_bars(batch, trade_date)
        cached.update(persist_underlying_bars(con, trade_date=trade_date, frames_by_parent=frames))
    return cached


def generate_run_report(
    con: duckdb.DuckDBPyConnection,
    *,
    run_id: str,
    report_dir: Path,
) -> Path:
    report_dir.mkdir(parents=True, exist_ok=True)
    report_path = report_dir / f"{run_id}.md"

    run_row = con.execute(
        """
        SELECT
            run_id,
            started_at,
            completed_at,
            start_date,
            end_date,
            parents_filter,
            z_threshold,
            cooldown_minutes,
            trade_days,
            alerts_total,
            outcomes_total
        FROM backtest_runs
        WHERE run_id = ?
        """,
        [run_id],
    ).fetchone()

    def fetchall(query: str, params: list[Any] | tuple[Any, ...] | None = None) -> list[tuple[Any, ...]]:
        return con.execute(query, params or []).fetchall()

    score_15m = {row[0]: row[1] for row in fetchall("""
        SELECT label_15m, COUNT(*) FROM backtest_signal_outcomes WHERE run_id = ? GROUP BY 1
    """, [run_id])}
    score_30m = {row[0]: row[1] for row in fetchall("""
        SELECT label_30m, COUNT(*) FROM backtest_signal_outcomes WHERE run_id = ? GROUP BY 1
    """, [run_id])}
    score_1h = {row[0]: row[1] for row in fetchall("""
        SELECT label_1h, COUNT(*) FROM backtest_signal_outcomes WHERE run_id = ? GROUP BY 1
    """, [run_id])}

    stats_row = con.execute("""
        SELECT
            AVG(option_return_5m), MEDIAN(option_return_5m),
            AVG(option_return_15m), MEDIAN(option_return_15m),
            AVG(option_return_30m), MEDIAN(option_return_30m),
            AVG(option_return_1h), MEDIAN(option_return_1h),
            AVG(max_up_pct), MEDIAN(max_up_pct),
            AVG(max_down_pct), MEDIAN(max_down_pct)
        FROM backtest_signal_outcomes
        WHERE run_id = ?
    """, [run_id]).fetchone()

    chance_row = con.execute("""
        SELECT
            COUNT(*) FILTER (WHERE max_up_pct < 0.05),
            COUNT(*) FILTER (WHERE max_up_pct >= 0.05 AND max_up_pct < 0.10),
            COUNT(*) FILTER (WHERE max_up_pct >= 0.10)
        FROM backtest_signal_outcomes
        WHERE run_id = ?
    """, [run_id]).fetchone()

    filter_row = con.execute("""
        SELECT
            COALESCE(SUM(option_rule_candidates), 0),
            COALESCE(SUM(skipped_not_first_alert), 0),
            COALESCE(SUM(skipped_context_filter), 0),
            COALESCE(SUM(quote_confirmation_pending), 0),
            COALESCE(SUM(passed_quote_confirmation), 0),
            COALESCE(SUM(skipped_missing_reference_quote), 0),
            COALESCE(SUM(skipped_quote_confirmation), 0),
            COALESCE(SUM(skipped_quote_confirmation_expired), 0),
            COALESCE(SUM(passed_underlying_confirmation), 0),
            COALESCE(SUM(skipped_missing_underlying), 0),
            COALESCE(SUM(skipped_no_breakout), 0)
        FROM backtest_signal_filter_stats
        WHERE run_id = ?
    """, [run_id]).fetchone()

    quote_confirmation_row = con.execute("""
        SELECT
            AVG(quote_confirm_seconds),
            MEDIAN(quote_confirm_seconds),
            AVG(quote_confirm_mid_change_abs),
            MEDIAN(quote_confirm_mid_change_abs),
            AVG(quote_confirm_mid_change_pct),
            MEDIAN(quote_confirm_mid_change_pct)
        FROM backtest_signal_events
        WHERE run_id = ?
          AND quote_confirm_seconds IS NOT NULL
    """, [run_id]).fetchone()

    strategy_stats_row = con.execute("""
        SELECT
            COUNT(*) FILTER (WHERE strategy_exit_timestamp IS NOT NULL),
            AVG(strategy_return_pct),
            MEDIAN(strategy_return_pct),
            COUNT(*) FILTER (WHERE strategy_return_pct >= 0.05),
            COUNT(*) FILTER (WHERE strategy_return_pct <= -0.05)
        FROM backtest_signal_outcomes
        WHERE run_id = ?
    """, [run_id]).fetchone()

    strategy_exit_breakdown = fetchall("""
        SELECT
            COALESCE(strategy_exit_reason, 'missing') AS exit_reason,
            COUNT(*) AS outcomes,
            AVG(strategy_return_pct),
            MEDIAN(strategy_return_pct),
            COUNT(*) FILTER (WHERE strategy_return_pct >= 0.05),
            COUNT(*) FILTER (WHERE strategy_return_pct <= -0.05)
        FROM backtest_signal_outcomes
        WHERE run_id = ?
        GROUP BY 1
        ORDER BY outcomes DESC, exit_reason
    """, [run_id])

    timing_row = con.execute("""
        SELECT
            AVG(date_diff('second', alert_timestamp, best_option_mid_timestamp) / 60.0),
            MEDIAN(date_diff('second', alert_timestamp, best_option_mid_timestamp) / 60.0),
            AVG(date_diff('second', alert_timestamp, worst_option_mid_timestamp) / 60.0),
            MEDIAN(date_diff('second', alert_timestamp, worst_option_mid_timestamp) / 60.0)
        FROM backtest_signal_outcomes
        WHERE run_id = ?
          AND best_option_mid_timestamp IS NOT NULL
          AND worst_option_mid_timestamp IS NOT NULL
    """, [run_id]).fetchone()

    time_to_best_buckets = fetchall("""
        WITH base AS (
            SELECT date_diff('second', alert_timestamp, best_option_mid_timestamp) / 60.0 AS minutes_to_best
            FROM backtest_signal_outcomes
            WHERE run_id = ?
              AND best_option_mid_timestamp IS NOT NULL
        )
        SELECT
            CASE
                WHEN minutes_to_best <= 5 THEN '<=5m'
                WHEN minutes_to_best <= 15 THEN '5m_to_15m'
                WHEN minutes_to_best <= 30 THEN '15m_to_30m'
                WHEN minutes_to_best <= 60 THEN '30m_to_1h'
                ELSE '>1h'
            END AS bucket,
            COUNT(*)
        FROM base
        GROUP BY 1
        ORDER BY CASE bucket
            WHEN '<=5m' THEN 1
            WHEN '5m_to_15m' THEN 2
            WHEN '15m_to_30m' THEN 3
            WHEN '30m_to_1h' THEN 4
            ELSE 5
        END
    """, [run_id])

    event_source_breakdown = fetchall("""
        SELECT
            e.event_source,
            COUNT(*) AS outcomes,
            AVG(o.option_return_15m),
            MEDIAN(o.option_return_15m),
            AVG(o.option_return_30m),
            MEDIAN(o.option_return_30m),
            AVG(o.option_return_1h),
            MEDIAN(o.option_return_1h),
            AVG(o.max_up_pct),
            MEDIAN(o.max_up_pct),
            COUNT(*) FILTER (WHERE o.max_up_pct < 0.05),
            COUNT(*) FILTER (WHERE o.max_up_pct >= 0.10)
        FROM backtest_signal_outcomes o
        JOIN backtest_signal_events e USING (run_id, signal_id)
        WHERE o.run_id = ?
        GROUP BY 1
        ORDER BY outcomes DESC, event_source
    """, [run_id])

    first_vs_repeat = fetchall("""
        WITH ranked AS (
            SELECT
                o.*,
                e.event_source,
                ROW_NUMBER() OVER (
                    PARTITION BY o.trade_date, o.contract_symbol
                    ORDER BY o.alert_timestamp
                ) AS alert_seq
            FROM backtest_signal_outcomes o
            JOIN backtest_signal_events e USING (run_id, signal_id)
            WHERE o.run_id = ?
        )
        SELECT
            CASE WHEN alert_seq = 1 THEN 'first' ELSE 'repeat' END AS alert_kind,
            COUNT(*) AS outcomes,
            AVG(option_return_15m),
            MEDIAN(option_return_15m),
            AVG(option_return_30m),
            MEDIAN(option_return_30m),
            AVG(option_return_1h),
            MEDIAN(option_return_1h),
            AVG(max_up_pct),
            MEDIAN(max_up_pct),
            COUNT(*) FILTER (WHERE max_up_pct < 0.05),
            COUNT(*) FILTER (WHERE max_up_pct >= 0.10)
        FROM ranked
        GROUP BY 1
        ORDER BY CASE alert_kind WHEN 'first' THEN 1 ELSE 2 END
    """, [run_id])

    first_vs_repeat_by_source = fetchall("""
        WITH ranked AS (
            SELECT
                o.*,
                e.event_source,
                ROW_NUMBER() OVER (
                    PARTITION BY o.trade_date, o.contract_symbol
                    ORDER BY o.alert_timestamp
                ) AS alert_seq
            FROM backtest_signal_outcomes o
            JOIN backtest_signal_events e USING (run_id, signal_id)
            WHERE o.run_id = ?
        )
        SELECT
            event_source,
            CASE WHEN alert_seq = 1 THEN 'first' ELSE 'repeat' END AS alert_kind,
            COUNT(*) AS outcomes,
            AVG(option_return_30m),
            MEDIAN(option_return_30m),
            AVG(max_up_pct),
            COUNT(*) FILTER (WHERE max_up_pct < 0.05)
        FROM ranked
        GROUP BY 1, 2
        ORDER BY event_source, alert_kind
    """, [run_id])

    time_of_day = fetchall("""
        WITH base AS (
            SELECT
                *,
                EXTRACT(HOUR FROM alert_timestamp) * 60 + EXTRACT(MINUTE FROM alert_timestamp) AS minute_of_day
            FROM backtest_signal_outcomes
            WHERE run_id = ?
        )
        SELECT
            CASE
                WHEN minute_of_day < 11 * 60 THEN 'open_930_1059'
                WHEN minute_of_day < 13 * 60 THEN 'late_morning_1100_1259'
                WHEN minute_of_day < 15 * 60 THEN 'afternoon_1300_1459'
                ELSE 'late_day_1500_close+'
            END AS bucket,
            COUNT(*) AS outcomes,
            AVG(option_return_15m),
            MEDIAN(option_return_15m),
            AVG(option_return_30m),
            MEDIAN(option_return_30m),
            AVG(option_return_1h),
            MEDIAN(option_return_1h),
            AVG(max_up_pct),
            MEDIAN(max_up_pct),
            COUNT(*) FILTER (WHERE max_up_pct < 0.05),
            COUNT(*) FILTER (WHERE max_up_pct >= 0.10)
        FROM base
        GROUP BY 1
        ORDER BY CASE bucket
            WHEN 'open_930_1059' THEN 1
            WHEN 'late_morning_1100_1259' THEN 2
            WHEN 'afternoon_1300_1459' THEN 3
            ELSE 4
        END
    """, [run_id])

    time_of_day_by_source = fetchall("""
        WITH base AS (
            SELECT
                o.*,
                e.event_source,
                EXTRACT(HOUR FROM o.alert_timestamp) * 60 + EXTRACT(MINUTE FROM o.alert_timestamp) AS minute_of_day
            FROM backtest_signal_outcomes o
            JOIN backtest_signal_events e USING (run_id, signal_id)
            WHERE o.run_id = ?
        )
        SELECT
            event_source,
            CASE
                WHEN minute_of_day < 11 * 60 THEN 'open_930_1059'
                WHEN minute_of_day < 13 * 60 THEN 'late_morning_1100_1259'
                WHEN minute_of_day < 15 * 60 THEN 'afternoon_1300_1459'
                ELSE 'late_day_1500_close+'
            END AS bucket,
            COUNT(*) AS outcomes,
            AVG(option_return_30m),
            MEDIAN(option_return_30m),
            AVG(max_up_pct)
        FROM base
        GROUP BY 1, 2
        ORDER BY event_source, bucket
    """, [run_id])

    daily_counts = fetchall("""
        SELECT
            e.trade_date,
            COUNT(*) AS alerts,
            AVG(o.option_return_30m),
            MEDIAN(o.option_return_30m),
            AVG(o.max_up_pct),
            COUNT(*) FILTER (WHERE o.max_up_pct < 0.05),
            COUNT(*) FILTER (WHERE o.max_up_pct >= 0.10)
        FROM backtest_signal_events e
        LEFT JOIN backtest_signal_outcomes o USING (run_id, signal_id)
        WHERE e.run_id = ?
        GROUP BY 1
        ORDER BY e.trade_date
    """, [run_id])

    def bucket_section(group_col: str, title: str) -> list[tuple[Any, ...]]:
        return fetchall(f"""
            SELECT
                {group_col} AS bucket,
                COUNT(*) AS outcomes,
                AVG(option_return_15m),
                MEDIAN(option_return_15m),
                AVG(option_return_30m),
                MEDIAN(option_return_30m),
                AVG(option_return_1h),
                MEDIAN(option_return_1h),
                AVG(max_up_pct),
                MEDIAN(max_up_pct),
                AVG(max_down_pct),
                MEDIAN(max_down_pct),
                COUNT(*) FILTER (WHERE max_up_pct < 0.05),
                COUNT(*) FILTER (WHERE max_up_pct >= 0.10),
                COUNT(*) FILTER (WHERE label_30m = 'winner'),
                COUNT(*) FILTER (WHERE label_30m = 'loser')
            FROM backtest_signal_outcomes
            WHERE run_id = ?
            GROUP BY 1
            ORDER BY outcomes DESC, bucket
        """, [run_id])

    by_side = bucket_section("side", "By Side")
    by_group = bucket_section("grouping", "By Grouping")
    by_decay = bucket_section("decay_bucket", "By Decay")

    parent_leaderboard = fetchall("""
        SELECT
            parent_symbol,
            COUNT(*) AS outcomes,
            AVG(option_return_15m),
            MEDIAN(option_return_15m),
            AVG(option_return_30m),
            MEDIAN(option_return_30m),
            AVG(option_return_1h),
            MEDIAN(option_return_1h),
            AVG(max_up_pct),
            MEDIAN(max_up_pct),
            AVG(max_down_pct),
            MEDIAN(max_down_pct),
            COUNT(*) FILTER (WHERE max_up_pct < 0.05),
            COUNT(*) FILTER (WHERE max_up_pct >= 0.10)
        FROM backtest_signal_outcomes
        WHERE run_id = ?
        GROUP BY 1
        ORDER BY outcomes DESC, parent_symbol
    """, [run_id])

    parent_top_30m = fetchall("""
        SELECT
            parent_symbol,
            COUNT(*) AS outcomes,
            AVG(option_return_30m),
            MEDIAN(option_return_30m),
            AVG(max_up_pct),
            MEDIAN(max_up_pct)
        FROM backtest_signal_outcomes
        WHERE run_id = ?
        GROUP BY 1
        HAVING COUNT(*) >= 15
        ORDER BY AVG(option_return_30m) DESC, COUNT(*) DESC
        LIMIT 25
    """, [run_id])
    parent_bottom_30m = fetchall("""
        SELECT
            parent_symbol,
            COUNT(*) AS outcomes,
            AVG(option_return_30m),
            MEDIAN(option_return_30m),
            AVG(max_up_pct),
            MEDIAN(max_up_pct)
        FROM backtest_signal_outcomes
        WHERE run_id = ?
        GROUP BY 1
        HAVING COUNT(*) >= 15
        ORDER BY AVG(option_return_30m) ASC, COUNT(*) DESC
        LIMIT 25
    """, [run_id])

    def z_band_breakdown(column_name: str) -> list[tuple[Any, ...]]:
        return fetchall(f"""
            SELECT
                {column_name} AS band,
                COUNT(*) AS outcomes,
                AVG(option_return_15m),
                MEDIAN(option_return_15m),
                AVG(option_return_30m),
                MEDIAN(option_return_30m),
                AVG(option_return_1h),
                MEDIAN(option_return_1h),
                AVG(max_up_pct),
                MEDIAN(max_up_pct),
                AVG(max_down_pct),
                MEDIAN(max_down_pct),
                COUNT(*) FILTER (WHERE max_up_pct < 0.05),
                COUNT(*) FILTER (WHERE max_up_pct >= 0.10),
                COUNT(*) FILTER (WHERE label_30m = 'winner'),
                COUNT(*) FILTER (WHERE label_30m = 'loser')
            FROM backtest_signal_outcomes
            WHERE run_id = ?
            GROUP BY 1
            ORDER BY CASE band
                WHEN '<1.5' THEN 0
                WHEN '1.5-3' THEN 1
                WHEN '3-5' THEN 2
                WHEN '5-10' THEN 3
                WHEN '10+' THEN 4
                ELSE 5
            END
        """, [run_id])

    z_band_sections = [
        ("z_vol_3d_band", "Z Vol 3D Band"),
        ("z_mid_3d_band", "Z Mid 3D Band"),
        ("z_iv_3d_band", "Z IV 3D Band"),
        ("z_vol_35d_band", "Z Vol 35D Band"),
        ("z_mid_35d_band", "Z Mid 35D Band"),
        ("z_iv_35d_band", "Z IV 35D Band"),
    ]

    three_d_combo_most_common = fetchall("""
        SELECT
            z_vol_3d_band,
            z_mid_3d_band,
            z_iv_3d_band,
            COUNT(*) AS outcomes,
            AVG(option_return_30m),
            MEDIAN(option_return_30m),
            AVG(option_return_1h),
            AVG(max_up_pct),
            COUNT(*) FILTER (WHERE max_up_pct < 0.05),
            COUNT(*) FILTER (WHERE max_up_pct >= 0.10)
        FROM backtest_signal_outcomes
        WHERE run_id = ?
        GROUP BY 1, 2, 3
        HAVING COUNT(*) >= 10
        ORDER BY outcomes DESC, z_vol_3d_band, z_mid_3d_band, z_iv_3d_band
    """, [run_id])
    three_d_combo_top = fetchall("""
        SELECT
            z_vol_3d_band,
            z_mid_3d_band,
            z_iv_3d_band,
            COUNT(*) AS outcomes,
            AVG(option_return_30m),
            MEDIAN(option_return_30m),
            AVG(option_return_1h),
            AVG(max_up_pct),
            COUNT(*) FILTER (WHERE max_up_pct < 0.05),
            COUNT(*) FILTER (WHERE max_up_pct >= 0.10)
        FROM backtest_signal_outcomes
        WHERE run_id = ?
        GROUP BY 1, 2, 3
        HAVING COUNT(*) >= 20
        ORDER BY AVG(option_return_30m) DESC, outcomes DESC
        LIMIT 30
    """, [run_id])
    three_d_combo_bottom = fetchall("""
        SELECT
            z_vol_3d_band,
            z_mid_3d_band,
            z_iv_3d_band,
            COUNT(*) AS outcomes,
            AVG(option_return_30m),
            MEDIAN(option_return_30m),
            AVG(option_return_1h),
            AVG(max_up_pct),
            COUNT(*) FILTER (WHERE max_up_pct < 0.05),
            COUNT(*) FILTER (WHERE max_up_pct >= 0.10)
        FROM backtest_signal_outcomes
        WHERE run_id = ?
        GROUP BY 1, 2, 3
        HAVING COUNT(*) >= 20
        ORDER BY AVG(option_return_30m) ASC, outcomes DESC
        LIMIT 30
    """, [run_id])

    correlations = fetchall("""
        SELECT 'z_vol_3d' AS metric, corr(z_vol_3d, option_return_15m), corr(z_vol_3d, option_return_30m), corr(z_vol_3d, option_return_1h), corr(z_vol_3d, max_up_pct), corr(z_vol_3d, max_down_pct)
        FROM backtest_signal_events e JOIN backtest_signal_outcomes o USING (run_id, signal_id)
        WHERE e.run_id = ?
        UNION ALL
        SELECT 'z_mid_3d', corr(z_mid_3d, option_return_15m), corr(z_mid_3d, option_return_30m), corr(z_mid_3d, option_return_1h), corr(z_mid_3d, max_up_pct), corr(z_mid_3d, max_down_pct)
        FROM backtest_signal_events e JOIN backtest_signal_outcomes o USING (run_id, signal_id)
        WHERE e.run_id = ?
        UNION ALL
        SELECT 'z_iv_3d', corr(z_iv_3d, option_return_15m), corr(z_iv_3d, option_return_30m), corr(z_iv_3d, option_return_1h), corr(z_iv_3d, max_up_pct), corr(z_iv_3d, max_down_pct)
        FROM backtest_signal_events e JOIN backtest_signal_outcomes o USING (run_id, signal_id)
        WHERE e.run_id = ?
        UNION ALL
        SELECT 'z_vol_35d', corr(z_vol_35d, option_return_15m), corr(z_vol_35d, option_return_30m), corr(z_vol_35d, option_return_1h), corr(z_vol_35d, max_up_pct), corr(z_vol_35d, max_down_pct)
        FROM backtest_signal_events e JOIN backtest_signal_outcomes o USING (run_id, signal_id)
        WHERE e.run_id = ?
        UNION ALL
        SELECT 'z_mid_35d', corr(z_mid_35d, option_return_15m), corr(z_mid_35d, option_return_30m), corr(z_mid_35d, option_return_1h), corr(z_mid_35d, max_up_pct), corr(z_mid_35d, max_down_pct)
        FROM backtest_signal_events e JOIN backtest_signal_outcomes o USING (run_id, signal_id)
        WHERE e.run_id = ?
        UNION ALL
        SELECT 'z_iv_35d', corr(z_iv_35d, option_return_15m), corr(z_iv_35d, option_return_30m), corr(z_iv_35d, option_return_1h), corr(z_iv_35d, max_up_pct), corr(z_iv_35d, max_down_pct)
        FROM backtest_signal_events e JOIN backtest_signal_outcomes o USING (run_id, signal_id)
        WHERE e.run_id = ?
    """, [run_id, run_id, run_id, run_id, run_id, run_id])

    best_15m = fetchall("""
        SELECT alert_timestamp, parent_symbol, contract_symbol, grouping, decay_bucket, option_return_15m, max_up_pct, max_down_pct
        FROM backtest_signal_outcomes
        WHERE run_id = ? AND option_return_15m IS NOT NULL
        ORDER BY option_return_15m DESC
        LIMIT 25
    """, [run_id])
    worst_15m = fetchall("""
        SELECT alert_timestamp, parent_symbol, contract_symbol, grouping, decay_bucket, option_return_15m, max_up_pct, max_down_pct
        FROM backtest_signal_outcomes
        WHERE run_id = ? AND option_return_15m IS NOT NULL
        ORDER BY option_return_15m ASC
        LIMIT 25
    """, [run_id])
    best_30m = fetchall("""
        SELECT alert_timestamp, parent_symbol, contract_symbol, grouping, decay_bucket, option_return_30m, max_up_pct, max_down_pct
        FROM backtest_signal_outcomes
        WHERE run_id = ? AND option_return_30m IS NOT NULL
        ORDER BY option_return_30m DESC
        LIMIT 25
    """, [run_id])
    worst_30m = fetchall("""
        SELECT alert_timestamp, parent_symbol, contract_symbol, grouping, decay_bucket, option_return_30m, max_up_pct, max_down_pct
        FROM backtest_signal_outcomes
        WHERE run_id = ? AND option_return_30m IS NOT NULL
        ORDER BY option_return_30m ASC
        LIMIT 25
    """, [run_id])
    best_1h = fetchall("""
        SELECT alert_timestamp, parent_symbol, contract_symbol, grouping, decay_bucket, option_return_1h, max_up_pct, max_down_pct
        FROM backtest_signal_outcomes
        WHERE run_id = ? AND option_return_1h IS NOT NULL
        ORDER BY option_return_1h DESC
        LIMIT 25
    """, [run_id])
    worst_1h = fetchall("""
        SELECT alert_timestamp, parent_symbol, contract_symbol, grouping, decay_bucket, option_return_1h, max_up_pct, max_down_pct
        FROM backtest_signal_outcomes
        WHERE run_id = ? AND option_return_1h IS NOT NULL
        ORDER BY option_return_1h ASC
        LIMIT 25
    """, [run_id])

    no_chance_examples = fetchall("""
        SELECT alert_timestamp, parent_symbol, contract_symbol, grouping, decay_bucket, option_return_30m, max_up_pct, max_down_pct
        FROM backtest_signal_outcomes
        WHERE run_id = ?
          AND option_return_30m IS NOT NULL
          AND max_up_pct < 0.05
        ORDER BY option_return_30m ASC
        LIMIT 50
    """, [run_id])
    overstayed_examples = fetchall("""
        SELECT alert_timestamp, parent_symbol, contract_symbol, grouping, decay_bucket, option_return_30m, max_up_pct, max_down_pct
        FROM backtest_signal_outcomes
        WHERE run_id = ?
          AND option_return_30m IS NOT NULL
          AND option_return_30m < 0
          AND max_up_pct >= 0.10
        ORDER BY max_up_pct DESC, option_return_30m ASC
        LIMIT 50
    """, [run_id])
    no_chance_by_side_group_decay = fetchall("""
        SELECT
            side,
            grouping,
            decay_bucket,
            COUNT(*) AS outcomes,
            COUNT(*) FILTER (WHERE max_up_pct < 0.05) AS no_chance_count,
            COUNT(*) FILTER (WHERE max_up_pct >= 0.10) AS good_chance_count,
            AVG(option_return_30m),
            MEDIAN(option_return_30m)
        FROM backtest_signal_outcomes
        WHERE run_id = ?
        GROUP BY 1, 2, 3
        HAVING COUNT(*) >= 10
        ORDER BY no_chance_count DESC, outcomes DESC
    """, [run_id])

    lines: list[str] = []
    lines.append(f"# Backtest Report {run_id}")
    lines.append("")
    lines.append("## Run")
    lines.append(f"- started_at: {run_row[1]}")
    lines.append(f"- completed_at: {run_row[2]}")
    lines.append(f"- date_range: {run_row[3]} -> {run_row[4]}")
    lines.append(f"- parents_filter: {run_row[5] or 'ALL'}")
    lines.append(f"- z_threshold: {run_row[6]}")
    lines.append(f"- signal_rule: {SIGNAL_RULE_DESCRIPTION}")
    lines.append(f"- cooldown_minutes: {run_row[7]}")
    lines.append(f"- trade_days: {run_row[8]}")
    lines.append(f"- alerts_total: {run_row[9]}")
    lines.append(f"- outcomes_total: {run_row[10]}")
    lines.append("")
    lines.append("## Scoreboard")
    lines.append(
        f"- 15m: winners={score_15m.get('winner', 0)} flat={score_15m.get('flat', 0)} "
        f"losers={score_15m.get('loser', 0)} unknown={score_15m.get('unknown', 0)}"
    )
    lines.append(
        f"- 30m: winners={score_30m.get('winner', 0)} flat={score_30m.get('flat', 0)} "
        f"losers={score_30m.get('loser', 0)} unknown={score_30m.get('unknown', 0)}"
    )
    lines.append(
        f"- 1h: winners={score_1h.get('winner', 0)} flat={score_1h.get('flat', 0)} "
        f"losers={score_1h.get('loser', 0)} unknown={score_1h.get('unknown', 0)}"
    )
    lines.append("")
    lines.append("## Returns")
    lines.append(f"- 5m avg={fmt_pct(stats_row[0])} median={fmt_pct(stats_row[1])}")
    lines.append(f"- 15m avg={fmt_pct(stats_row[2])} median={fmt_pct(stats_row[3])}")
    lines.append(f"- 30m avg={fmt_pct(stats_row[4])} median={fmt_pct(stats_row[5])}")
    lines.append(f"- 1h avg={fmt_pct(stats_row[6])} median={fmt_pct(stats_row[7])}")
    lines.append(f"- max_up avg={fmt_pct(stats_row[8])} median={fmt_pct(stats_row[9])}")
    lines.append(f"- max_down avg={fmt_pct(stats_row[10])} median={fmt_pct(stats_row[11])}")
    lines.append("")
    lines.append("## Chance Breakdown")
    lines.append(f"- no_chance_lt_5pct: {chance_row[0]}")
    lines.append(f"- brief_chance_5_to_10pct: {chance_row[1]}")
    lines.append(f"- good_chance_ge_10pct: {chance_row[2]}")
    lines.append("")
    lines.append("## Signal Filters")
    lines.append(f"- volume_rule_candidates: {filter_row[0]}")
    lines.append(f"- first_alert_only: {FIRST_ALERT_ONLY}")
    lines.append(f"- skipped_not_first_alert: {filter_row[1]}")
    lines.append(f"- allowed_groupings: {', '.join(sorted(ALLOWED_SIGNAL_GROUPINGS))}")
    lines.append(f"- allowed_decay_buckets: {', '.join(sorted(ALLOWED_SIGNAL_DECAY_BUCKETS))}")
    lines.append(f"- skipped_context_filter: {filter_row[2]}")
    lines.append("")
    lines.append("## Quote Confirmation Filter")
    lines.append(f"- enabled: {QUOTE_CONFIRMATION_ENABLED}")
    lines.append(f"- window_minutes: {QUOTE_CONFIRM_WINDOW.total_seconds() / 60:.0f}")
    lines.append(f"- min_mid_change_pct: {fmt_pct(QUOTE_CONFIRM_MIN_MID_PCT)}")
    lines.append(f"- min_mid_change_abs: {QUOTE_CONFIRM_MIN_MID_ABS}")
    lines.append(f"- quote_confirmation_pending: {filter_row[3]}")
    lines.append(f"- passed_quote_confirmation: {filter_row[4]}")
    lines.append(f"- skipped_missing_reference_quote: {filter_row[5]}")
    lines.append(f"- skipped_quote_confirmation: {filter_row[6]}")
    lines.append(f"- skipped_quote_confirmation_expired: {filter_row[7]}")
    lines.append(f"- avg_confirm_seconds: {fmt_num(quote_confirmation_row[0], 2)}")
    lines.append(f"- median_confirm_seconds: {fmt_num(quote_confirmation_row[1], 2)}")
    lines.append(f"- avg_mid_change_abs: {fmt_num(quote_confirmation_row[2], 4)}")
    lines.append(f"- median_mid_change_abs: {fmt_num(quote_confirmation_row[3], 4)}")
    lines.append(f"- avg_mid_change_pct: {fmt_pct(quote_confirmation_row[4])}")
    lines.append(f"- median_mid_change_pct: {fmt_pct(quote_confirmation_row[5])}")
    lines.append("")
    lines.append("## Underlying Confirmation Filter")
    lines.append(f"- enabled: {UNDERLYING_CONFIRMATION_ENABLED}")
    lines.append(f"- interval: {UNDERLYING_INTRADAY_INTERVAL}")
    lines.append(f"- lookback_minutes: {UNDERLYING_LOOKBACK_MINUTES}")
    lines.append(f"- passed_underlying_confirmation: {filter_row[8]}")
    lines.append(f"- skipped_missing_underlying: {filter_row[9]}")
    lines.append(f"- skipped_no_breakout: {filter_row[10]}")
    lines.append("")
    lines.append("## Strategy Exit")
    lines.append(f"- exit_rule: t_entry + {STRATEGY_EXIT_MINUTES}m OR earlier close fail through breakout level")
    lines.append(f"- exits_captured: {strategy_stats_row[0]}")
    lines.append(f"- strategy_return avg={fmt_pct(strategy_stats_row[1])} median={fmt_pct(strategy_stats_row[2])}")
    lines.append(f"- strategy_winners_ge_5pct: {strategy_stats_row[3]}")
    lines.append(f"- strategy_losers_le_neg_5pct: {strategy_stats_row[4]}")
    lines.append("")
    lines.append("### Strategy Exit Reason Breakdown")
    lines.extend(markdown_table(
        ["Reason", "Outcomes", "Avg Strategy Ret", "Median Strategy Ret", "Winners >=5%", "Losers <=-5%"],
        [[
            str(reason),
            str(outcomes),
            fmt_pct(avg_ret),
            fmt_pct(med_ret),
            str(winners),
            str(losers),
        ] for reason, outcomes, avg_ret, med_ret, winners, losers in strategy_exit_breakdown],
    ))
    lines.append("## Opportunity Timing")
    lines.append(f"- time_to_best avg_minutes={fmt_num(timing_row[0], 2)} median_minutes={fmt_num(timing_row[1], 2)}")
    lines.append(f"- time_to_worst avg_minutes={fmt_num(timing_row[2], 2)} median_minutes={fmt_num(timing_row[3], 2)}")
    lines.append("")
    lines.append("### Time To Best Bucket")
    lines.extend(markdown_table(
        ["Bucket", "Count"],
        [[str(bucket), str(count)] for bucket, count in time_to_best_buckets],
    ))

    lines.append("## Event Source")
    lines.extend(markdown_table(
        ["Source", "Outcomes", "Avg 15m", "Median 15m", "Avg 30m", "Median 30m", "Avg 1h", "Median 1h", "Avg Max Up", "Median Max Up", "No Chance", "Good Chance", "No Chance Rate", "Good Chance Rate"],
        [[
            str(source),
            str(outcomes),
            fmt_pct(avg15),
            fmt_pct(med15),
            fmt_pct(avg30),
            fmt_pct(med30),
            fmt_pct(avg1h),
            fmt_pct(med1h),
            fmt_pct(avg_up),
            fmt_pct(med_up),
            str(no_chance),
            str(good_chance),
            fmt_rate(no_chance, outcomes),
            fmt_rate(good_chance, outcomes),
        ] for source, outcomes, avg15, med15, avg30, med30, avg1h, med1h, avg_up, med_up, no_chance, good_chance in event_source_breakdown],
    ))

    lines.append("## First Vs Repeat Alert")
    lines.extend(markdown_table(
        ["Kind", "Outcomes", "Avg 15m", "Median 15m", "Avg 30m", "Median 30m", "Avg 1h", "Median 1h", "Avg Max Up", "Median Max Up", "No Chance", "Good Chance", "No Chance Rate", "Good Chance Rate"],
        [[
            str(kind),
            str(outcomes),
            fmt_pct(avg15),
            fmt_pct(med15),
            fmt_pct(avg30),
            fmt_pct(med30),
            fmt_pct(avg1h),
            fmt_pct(med1h),
            fmt_pct(avg_up),
            fmt_pct(med_up),
            str(no_chance),
            str(good_chance),
            fmt_rate(no_chance, outcomes),
            fmt_rate(good_chance, outcomes),
        ] for kind, outcomes, avg15, med15, avg30, med30, avg1h, med1h, avg_up, med_up, no_chance, good_chance in first_vs_repeat],
    ))

    lines.append("## First Vs Repeat By Event Source")
    lines.extend(markdown_table(
        ["Source", "Kind", "Outcomes", "Avg 30m", "Median 30m", "Avg Max Up", "No Chance", "No Chance Rate"],
        [[
            str(source),
            str(kind),
            str(outcomes),
            fmt_pct(avg30),
            fmt_pct(med30),
            fmt_pct(avg_up),
            str(no_chance),
            fmt_rate(no_chance, outcomes),
        ] for source, kind, outcomes, avg30, med30, avg_up, no_chance in first_vs_repeat_by_source],
    ))

    lines.append("## Time Of Day")
    lines.extend(markdown_table(
        ["Bucket", "Outcomes", "Avg 15m", "Median 15m", "Avg 30m", "Median 30m", "Avg 1h", "Median 1h", "Avg Max Up", "Median Max Up", "No Chance", "Good Chance", "No Chance Rate", "Good Chance Rate"],
        [[
            str(bucket),
            str(outcomes),
            fmt_pct(avg15),
            fmt_pct(med15),
            fmt_pct(avg30),
            fmt_pct(med30),
            fmt_pct(avg1h),
            fmt_pct(med1h),
            fmt_pct(avg_up),
            fmt_pct(med_up),
            str(no_chance),
            str(good_chance),
            fmt_rate(no_chance, outcomes),
            fmt_rate(good_chance, outcomes),
        ] for bucket, outcomes, avg15, med15, avg30, med30, avg1h, med1h, avg_up, med_up, no_chance, good_chance in time_of_day],
    ))

    lines.append("## Time Of Day By Event Source")
    lines.extend(markdown_table(
        ["Source", "Bucket", "Outcomes", "Avg 30m", "Median 30m", "Avg Max Up"],
        [[
            str(source),
            str(bucket),
            str(outcomes),
            fmt_pct(avg30),
            fmt_pct(med30),
            fmt_pct(avg_up),
        ] for source, bucket, outcomes, avg30, med30, avg_up in time_of_day_by_source],
    ))

    lines.append("## Daily Breakdown")
    lines.extend(markdown_table(
        ["Date", "Alerts", "Avg 30m", "Median 30m", "Avg Max Up", "No Chance", "Good Chance", "No Chance Rate", "Good Chance Rate"],
        [[
            str(trade_date),
            str(alerts),
            fmt_pct(avg30),
            fmt_pct(med30),
            fmt_pct(avg_up),
            str(no_chance),
            str(good_chance),
            fmt_rate(no_chance, alerts),
            fmt_rate(good_chance, alerts),
        ] for trade_date, alerts, avg30, med30, avg_up, no_chance, good_chance in daily_counts],
    ))

    for title, rows in [("By Side", by_side), ("By Grouping", by_group), ("By Decay", by_decay)]:
        lines.append(f"## {title}")
        lines.extend(markdown_table(
            ["Bucket", "Outcomes", "Avg 15m", "Median 15m", "Avg 30m", "Median 30m", "Avg 1h", "Median 1h", "Avg Max Up", "Median Max Up", "Avg Max Down", "Median Max Down", "No Chance", "Good Chance", "30m Winners", "30m Losers", "No Chance Rate", "Good Chance Rate"],
            [[
                str(bucket),
                str(outcomes),
                fmt_pct(avg15),
                fmt_pct(med15),
                fmt_pct(avg30),
                fmt_pct(med30),
                fmt_pct(avg1h),
                fmt_pct(med1h),
                fmt_pct(avg_up),
                fmt_pct(med_up),
                fmt_pct(avg_down),
                fmt_pct(med_down),
                str(no_chance),
                str(good_chance),
                str(winners30),
                str(losers30),
                fmt_rate(no_chance, outcomes),
                fmt_rate(good_chance, outcomes),
            ] for bucket, outcomes, avg15, med15, avg30, med30, avg1h, med1h, avg_up, med_up, avg_down, med_down, no_chance, good_chance, winners30, losers30 in rows],
        ))

    lines.append("## Parent Leaderboard (All Parents)")
    lines.extend(markdown_table(
        ["Parent", "Outcomes", "Avg 15m", "Median 15m", "Avg 30m", "Median 30m", "Avg 1h", "Median 1h", "Avg Max Up", "Median Max Up", "Avg Max Down", "Median Max Down", "No Chance", "Good Chance", "No Chance Rate", "Good Chance Rate"],
        [[
            str(parent),
            str(outcomes),
            fmt_pct(avg15),
            fmt_pct(med15),
            fmt_pct(avg30),
            fmt_pct(med30),
            fmt_pct(avg1h),
            fmt_pct(med1h),
            fmt_pct(avg_up),
            fmt_pct(med_up),
            fmt_pct(avg_down),
            fmt_pct(med_down),
            str(no_chance),
            str(good_chance),
            fmt_rate(no_chance, outcomes),
            fmt_rate(good_chance, outcomes),
        ] for parent, outcomes, avg15, med15, avg30, med30, avg1h, med1h, avg_up, med_up, avg_down, med_down, no_chance, good_chance in parent_leaderboard],
    ))

    lines.append("## Parent Top 30m (Min 15 Outcomes)")
    lines.extend(markdown_table(
        ["Parent", "Outcomes", "Avg 30m", "Median 30m", "Avg Max Up", "Median Max Up"],
        [[str(parent), str(outcomes), fmt_pct(avg30), fmt_pct(med30), fmt_pct(avg_up), fmt_pct(med_up)] for parent, outcomes, avg30, med30, avg_up, med_up in parent_top_30m],
    ))

    lines.append("## Parent Bottom 30m (Min 15 Outcomes)")
    lines.extend(markdown_table(
        ["Parent", "Outcomes", "Avg 30m", "Median 30m", "Avg Max Up", "Median Max Up"],
        [[str(parent), str(outcomes), fmt_pct(avg30), fmt_pct(med30), fmt_pct(avg_up), fmt_pct(med_up)] for parent, outcomes, avg30, med30, avg_up, med_up in parent_bottom_30m],
    ))

    for column_name, title in z_band_sections:
        rows = z_band_breakdown(column_name)
        lines.append(f"## {title}")
        lines.extend(markdown_table(
            ["Band", "Outcomes", "Avg 15m", "Median 15m", "Avg 30m", "Median 30m", "Avg 1h", "Median 1h", "Avg Max Up", "Median Max Up", "Avg Max Down", "Median Max Down", "No Chance", "Good Chance", "30m Winners", "30m Losers", "No Chance Rate", "Good Chance Rate"],
            [[
                str(band),
                str(outcomes),
                fmt_pct(avg15),
                fmt_pct(med15),
                fmt_pct(avg30),
                fmt_pct(med30),
                fmt_pct(avg1h),
                fmt_pct(med1h),
                fmt_pct(avg_up),
                fmt_pct(med_up),
                fmt_pct(avg_down),
                fmt_pct(med_down),
                str(no_chance),
                str(good_chance),
                str(winners30),
                str(losers30),
                fmt_rate(no_chance, outcomes),
                fmt_rate(good_chance, outcomes),
            ] for band, outcomes, avg15, med15, avg30, med30, avg1h, med1h, avg_up, med_up, avg_down, med_down, no_chance, good_chance, winners30, losers30 in rows],
        ))

    lines.append("## 3D Z-Band Combo Most Common (Min 10)")
    lines.extend(markdown_table(
        ["Vol 3D", "Mid 3D", "IV 3D", "Outcomes", "Avg 30m", "Median 30m", "Avg 1h", "Avg Max Up", "No Chance", "Good Chance", "No Chance Rate", "Good Chance Rate"],
        [[
            str(vol_band),
            str(mid_band),
            str(iv_band),
            str(outcomes),
            fmt_pct(avg30),
            fmt_pct(med30),
            fmt_pct(avg1h),
            fmt_pct(avg_up),
            str(no_chance),
            str(good_chance),
            fmt_rate(no_chance, outcomes),
            fmt_rate(good_chance, outcomes),
        ] for vol_band, mid_band, iv_band, outcomes, avg30, med30, avg1h, avg_up, no_chance, good_chance in three_d_combo_most_common],
    ))

    lines.append("## 3D Z-Band Combo Top 30m (Min 20)")
    lines.extend(markdown_table(
        ["Vol 3D", "Mid 3D", "IV 3D", "Outcomes", "Avg 30m", "Median 30m", "Avg 1h", "Avg Max Up", "No Chance", "Good Chance", "No Chance Rate", "Good Chance Rate"],
        [[
            str(vol_band),
            str(mid_band),
            str(iv_band),
            str(outcomes),
            fmt_pct(avg30),
            fmt_pct(med30),
            fmt_pct(avg1h),
            fmt_pct(avg_up),
            str(no_chance),
            str(good_chance),
            fmt_rate(no_chance, outcomes),
            fmt_rate(good_chance, outcomes),
        ] for vol_band, mid_band, iv_band, outcomes, avg30, med30, avg1h, avg_up, no_chance, good_chance in three_d_combo_top],
    ))

    lines.append("## 3D Z-Band Combo Bottom 30m (Min 20)")
    lines.extend(markdown_table(
        ["Vol 3D", "Mid 3D", "IV 3D", "Outcomes", "Avg 30m", "Median 30m", "Avg 1h", "Avg Max Up", "No Chance", "Good Chance", "No Chance Rate", "Good Chance Rate"],
        [[
            str(vol_band),
            str(mid_band),
            str(iv_band),
            str(outcomes),
            fmt_pct(avg30),
            fmt_pct(med30),
            fmt_pct(avg1h),
            fmt_pct(avg_up),
            str(no_chance),
            str(good_chance),
            fmt_rate(no_chance, outcomes),
            fmt_rate(good_chance, outcomes),
        ] for vol_band, mid_band, iv_band, outcomes, avg30, med30, avg1h, avg_up, no_chance, good_chance in three_d_combo_bottom],
    ))

    lines.append("## Correlations")
    lines.extend(markdown_table(
        ["Metric", "Corr 15m", "Corr 30m", "Corr 1h", "Corr Max Up", "Corr Max Down"],
        [[str(metric), fmt_num(c15, 4), fmt_num(c30, 4), fmt_num(c1h, 4), fmt_num(c_up, 4), fmt_num(c_down, 4)] for metric, c15, c30, c1h, c_up, c_down in correlations],
    ))

    for title, rows, value_label in [
        ("Best 15m", best_15m, "15m"),
        ("Worst 15m", worst_15m, "15m"),
        ("Best 30m", best_30m, "30m"),
        ("Worst 30m", worst_30m, "30m"),
        ("Best 1h", best_1h, "1h"),
        ("Worst 1h", worst_1h, "1h"),
    ]:
        lines.append(f"## {title}")
        lines.extend(markdown_table(
            ["Time", "Parent", "Contract", "Grouping", "Decay", f"Ret {value_label}", "Max Up", "Max Down"],
            [[str(alert_ts), str(parent), str(contract_value), str(grouping), str(decay_bucket), fmt_pct(ret_value), fmt_pct(max_up), fmt_pct(max_down)] for alert_ts, parent, contract_value, grouping, decay_bucket, ret_value, max_up, max_down in rows],
        ))

    lines.append("## No-Chance Examples (Max Up < 5%)")
    lines.extend(markdown_table(
        ["Time", "Parent", "Contract", "Grouping", "Decay", "Ret 30m", "Max Up", "Max Down"],
        [[str(alert_ts), str(parent), str(contract_value), str(grouping), str(decay_bucket), fmt_pct(ret30), fmt_pct(max_up), fmt_pct(max_down)] for alert_ts, parent, contract_value, grouping, decay_bucket, ret30, max_up, max_down in no_chance_examples],
    ))

    lines.append("## Overstayed Examples (Lost At 30m, But Had >= 10% Max Up)")
    lines.extend(markdown_table(
        ["Time", "Parent", "Contract", "Grouping", "Decay", "Ret 30m", "Max Up", "Max Down"],
        [[str(alert_ts), str(parent), str(contract_value), str(grouping), str(decay_bucket), fmt_pct(ret30), fmt_pct(max_up), fmt_pct(max_down)] for alert_ts, parent, contract_value, grouping, decay_bucket, ret30, max_up, max_down in overstayed_examples],
    ))

    lines.append("## No-Chance By Side/Grouping/Decay")
    lines.extend(markdown_table(
        ["Side", "Grouping", "Decay", "Outcomes", "No Chance", "Good Chance", "Avg 30m", "Median 30m", "No Chance Rate", "Good Chance Rate"],
        [[
            str(side),
            str(grouping),
            str(decay_bucket),
            str(outcomes),
            str(no_chance),
            str(good_chance),
            fmt_pct(avg30),
            fmt_pct(med30),
            fmt_rate(no_chance, outcomes),
            fmt_rate(good_chance, outcomes),
        ] for side, grouping, decay_bucket, outcomes, no_chance, good_chance, avg30, med30 in no_chance_by_side_group_decay],
    ))

    report_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return report_path


def fetch_trade_dates(
    con: duckdb.DuckDBPyConnection,
    *,
    start_date: dt.date,
    end_date: dt.date,
    parents: list[str] | None,
) -> list[dt.date]:
    parent_sql, params = parent_filter_sql(parents)
    rows = con.execute(
        f"""
        SELECT DISTINCT CAST(timestamp AS DATE) AS trade_date
        FROM option_snapshots_raw
        WHERE CAST(timestamp AS DATE) BETWEEN ? AND ?
        {parent_sql}
        ORDER BY trade_date
        """,
        [start_date, end_date, *params],
    ).fetchall()
    return [row[0] for row in rows]


def resolve_date_range(
    con: duckdb.DuckDBPyConnection,
    *,
    start_date: dt.date | None,
    end_date: dt.date | None,
    parents: list[str] | None,
) -> tuple[dt.date, dt.date]:
    parent_sql, params = parent_filter_sql(parents)
    row = con.execute(
        f"""
        SELECT
            MIN(CAST(timestamp AS DATE)) AS min_trade_date,
            MAX(CAST(timestamp AS DATE)) AS max_trade_date
        FROM option_snapshots_raw
        WHERE 1 = 1
        {parent_sql}
        """,
        params,
    ).fetchone()

    min_trade_date, max_trade_date = row
    if min_trade_date is None or max_trade_date is None:
        raise SystemExit("No historical snapshot rows found for the requested parent filter.")

    resolved_start = start_date or min_trade_date
    resolved_end = end_date or max_trade_date
    if resolved_end < resolved_start:
        raise SystemExit("--end-date must be on or after --start-date")
    return resolved_start, resolved_end


def load_day_contracts(
    con: duckdb.DuckDBPyConnection,
    *,
    trade_date: dt.date,
    parents: list[str] | None,
) -> list[dict[str, Any]]:
    parent_sql, params = parent_filter_sql(parents)
    rows = con.execute(
        f"""
        SELECT DISTINCT
            parent_symbol,
            strike,
            side,
            expiration_date,
            grouping,
            time_decay_bucket,
            days_to_expiry
        FROM option_snapshots_raw
        WHERE CAST(timestamp AS DATE) = ?
        {parent_sql}
        ORDER BY parent_symbol, expiration_date, strike, side
        """,
        [trade_date, *params],
    ).fetchall()

    contracts: list[dict[str, Any]] = []
    for parent_symbol, strike, side, expiration_date, grouping, decay_bucket, days_to_expiry in rows:
        contracts.append({
            "parent_symbol": str(parent_symbol),
            "strike": float(strike),
            "side": str(side),
            "expiration_date": expiration_date,
            "grouping": str(grouping),
            "decay_bucket": str(decay_bucket),
            "days_to_expiry": int(days_to_expiry),
            "contract_symbol": contract_symbol(
                str(parent_symbol),
                expiration_date,
                float(strike),
                str(side),
            ),
        })
    return contracts


def query_stats_for_window(
    con: duckdb.DuckDBPyConnection,
    *,
    table_name: str,
    value_column: str,
    key_df: pd.DataFrame,
    since_dt: dt.datetime,
    until_dt: dt.datetime,
) -> dict[tuple[str, str, str, str], dict[str, Any]]:
    con.register("_daily_keys", key_df)
    try:
        rows = con.execute(
            f"""
            SELECT
                k.parent_symbol,
                k.side,
                k.grouping,
                k.time_decay_bucket,
                COUNT(t.{value_column}) AS sample_count,
                AVG(t.{value_column}) AS mean_value,
                STDDEV_SAMP(t.{value_column}) AS std_value
            FROM _daily_keys AS k
            LEFT JOIN {table_name} AS t
              ON t.parent_symbol = k.parent_symbol
             AND t.side = k.side
             AND t.grouping = k.grouping
             AND t.time_decay_bucket = k.time_decay_bucket
             AND t.timestamp >= ?
             AND t.timestamp < ?
            GROUP BY 1, 2, 3, 4
            """,
            [since_dt, until_dt],
        ).fetchall()
    finally:
        con.unregister("_daily_keys")

    out: dict[tuple[str, str, str, str], dict[str, Any]] = {}
    for parent_symbol, side, grouping, decay_bucket, count, mean_value, std_value in rows:
        out[(str(parent_symbol), str(side), str(grouping), str(decay_bucket))] = {
            "count": count,
            "mean": mean_value,
            "std": std_value,
        }
    return out


def query_recent_stats_with_same_decay_fallback_as_of(
    con: duckdb.DuckDBPyConnection,
    *,
    table_name: str,
    value_column: str,
    combo: tuple[str, str, str, str],
    since_dt: dt.datetime,
    trade_date: dt.date,
) -> dict[str, Any]:
    parent_symbol, side, grouping, decay_bucket = combo
    day_start = dt.datetime.combine(trade_date, dt.time.min)
    recent = con.execute(
        f"""
        SELECT
            COUNT({value_column}) AS sample_count,
            AVG({value_column}) AS mean_value,
            STDDEV_SAMP({value_column}) AS std_value
        FROM {table_name}
        WHERE parent_symbol = ?
          AND side = ?
          AND grouping = ?
          AND time_decay_bucket = ?
          AND timestamp >= ?
          AND timestamp < ?
        """,
        [
            parent_symbol,
            side,
            grouping,
            decay_bucket,
            since_dt,
            day_start,
        ],
    ).fetchone()
    recent_stats = {
        "count": recent[0],
        "mean": recent[1],
        "std": recent[2],
        "source": "recent_3d_same_decay",
    }
    if has_usable_stats(recent_stats):
        return recent_stats

    fallback_days = con.execute(
        f"""
        SELECT CAST(timestamp AS DATE) AS sample_date
        FROM {table_name}
        WHERE parent_symbol = ?
          AND side = ?
          AND grouping = ?
          AND time_decay_bucket = ?
          AND CAST(timestamp AS DATE) < ?
        GROUP BY sample_date
        ORDER BY sample_date DESC
        LIMIT {MAX_FALLBACK_DAYS_TO_CHECK}
        """,
        [
            parent_symbol,
            side,
            grouping,
            decay_bucket,
            trade_date,
        ],
    ).fetchall()

    for (fallback_day,) in fallback_days:
        row = con.execute(
            f"""
            SELECT
                COUNT({value_column}) AS sample_count,
                AVG({value_column}) AS mean_value,
                STDDEV_SAMP({value_column}) AS std_value
            FROM {table_name}
            WHERE parent_symbol = ?
              AND side = ?
              AND grouping = ?
              AND time_decay_bucket = ?
              AND CAST(timestamp AS DATE) = ?
            """,
            [
                parent_symbol,
                side,
                grouping,
                decay_bucket,
                fallback_day,
            ],
        ).fetchone()
        fallback_stats = {
            "count": row[0],
            "mean": row[1],
            "std": row[2],
            "source": f"last_full_same_decay:{fallback_day.isoformat()}",
        }
        if has_usable_stats(fallback_stats):
            return fallback_stats

    return recent_stats


def load_baselines_for_day(
    con: duckdb.DuckDBPyConnection,
    *,
    trade_date: dt.date,
    contracts: list[dict[str, Any]],
) -> dict[tuple[str, str, str, str], dict[str, Any]]:
    combos = sorted({combo_key(contract) for contract in contracts})
    if not combos:
        return {}

    key_df = pd.DataFrame(
        combos,
        columns=["parent_symbol", "side", "grouping", "time_decay_bucket"],
    )

    day_start = dt.datetime.combine(trade_date, dt.time.min)
    stats_mid_35d = query_stats_for_window(
        con,
        table_name="option_snapshots_raw",
        value_column="mid",
        key_df=key_df,
        since_dt=day_start - dt.timedelta(days=35),
        until_dt=day_start,
    )
    stats_mid_3d = query_stats_for_window(
        con,
        table_name="option_snapshots_raw",
        value_column="mid",
        key_df=key_df,
        since_dt=day_start - dt.timedelta(days=3),
        until_dt=day_start,
    )
    stats_iv_35d = query_stats_for_window(
        con,
        table_name="option_snapshots_raw",
        value_column="iv",
        key_df=key_df,
        since_dt=day_start - dt.timedelta(days=35),
        until_dt=day_start,
    )
    stats_iv_3d = query_stats_for_window(
        con,
        table_name="option_snapshots_raw",
        value_column="iv",
        key_df=key_df,
        since_dt=day_start - dt.timedelta(days=3),
        until_dt=day_start,
    )
    stats_vol_35d = query_stats_for_window(
        con,
        table_name="rolling_volume_history",
        value_column="rolling_volume_10m",
        key_df=key_df,
        since_dt=day_start - dt.timedelta(days=35),
        until_dt=day_start,
    )
    stats_vol_3d = query_stats_for_window(
        con,
        table_name="rolling_volume_history",
        value_column="rolling_volume_10m",
        key_df=key_df,
        since_dt=day_start - dt.timedelta(days=3),
        until_dt=day_start,
    )

    baselines: dict[tuple[str, str, str, str], dict[str, Any]] = {}
    for combo in combos:
        mid_3d = dict(stats_mid_3d.get(combo, {"count": 0, "mean": None, "std": None}))
        mid_3d["source"] = "recent_3d_same_decay"
        if not has_usable_stats(mid_3d):
            mid_3d = query_recent_stats_with_same_decay_fallback_as_of(
                con,
                table_name="option_snapshots_raw",
                value_column="mid",
                combo=combo,
                since_dt=day_start - dt.timedelta(days=3),
                trade_date=trade_date,
            )

        iv_3d = dict(stats_iv_3d.get(combo, {"count": 0, "mean": None, "std": None}))
        iv_3d["source"] = "recent_3d_same_decay"
        if not has_usable_stats(iv_3d):
            iv_3d = query_recent_stats_with_same_decay_fallback_as_of(
                con,
                table_name="option_snapshots_raw",
                value_column="iv",
                combo=combo,
                since_dt=day_start - dt.timedelta(days=3),
                trade_date=trade_date,
            )

        vol_3d = dict(stats_vol_3d.get(combo, {"count": 0, "mean": None, "std": None}))
        vol_3d["source"] = "recent_3d_same_decay"
        if not has_usable_stats(vol_3d):
            vol_3d = query_recent_stats_with_same_decay_fallback_as_of(
                con,
                table_name="rolling_volume_history",
                value_column="rolling_volume_10m",
                combo=combo,
                since_dt=day_start - dt.timedelta(days=3),
                trade_date=trade_date,
            )

        baselines[combo] = {
            "mean_mid_35d": stats_mid_35d.get(combo, {}).get("mean"),
            "std_mid_35d": stats_mid_35d.get(combo, {}).get("std"),
            "mean_mid_3d": mid_3d.get("mean"),
            "std_mid_3d": mid_3d.get("std"),
            "source_mid_3d": mid_3d.get("source"),
            "mean_iv_35d": stats_iv_35d.get(combo, {}).get("mean"),
            "std_iv_35d": stats_iv_35d.get(combo, {}).get("std"),
            "mean_iv_3d": iv_3d.get("mean"),
            "std_iv_3d": iv_3d.get("std"),
            "source_iv_3d": iv_3d.get("source"),
            "mean_vol_35d": stats_vol_35d.get(combo, {}).get("mean"),
            "std_vol_35d": stats_vol_35d.get(combo, {}).get("std"),
            "mean_vol_3d": vol_3d.get("mean"),
            "std_vol_3d": vol_3d.get("std"),
            "source_vol_3d": vol_3d.get("source"),
        }
    return baselines


def build_contract_state(metadata: dict[str, Any]) -> dict[str, Any]:
    return {
        "parent_symbol": metadata["parent_symbol"],
        "contract_symbol": metadata["contract_symbol"],
        "strike": metadata["strike"],
        "expiration_date": metadata["expiration_date"],
        "side": metadata["side"],
        "grouping": metadata["grouping"],
        "decay_bucket": metadata["decay_bucket"],
        "days_to_expiry": metadata["days_to_expiry"],
        "bid": None,
        "ask": None,
        "mid": None,
        "spread": None,
        "spread_pct": None,
        "rolling_volume_10m": 0,
        "rolling_volume_30m": None,
        "rolling_volume_1h": None,
        "underlying_price": None,
        "current_iv": None,
        "mean_mid_35d": None,
        "std_mid_35d": None,
        "mean_mid_3d": None,
        "std_mid_3d": None,
        "mean_iv_35d": None,
        "std_iv_35d": None,
        "mean_iv_3d": None,
        "std_iv_3d": None,
        "mean_vol_35d": None,
        "std_vol_35d": None,
        "mean_vol_3d": None,
        "std_vol_3d": None,
        "z_mid_35d": None,
        "z_mid_3d": None,
        "z_iv_35d": None,
        "z_iv_3d": None,
        "z_vol_35d": None,
        "z_vol_3d": None,
        "source_mid_3d": None,
        "source_iv_3d": None,
        "source_vol_3d": None,
        "last_quote_ts": None,
        "last_trade_ts": None,
        "last_volume_update_ts": None,
        "updated_at": None,
        "volume_rule_active": False,
        "quote_reference_mid": None,
        "quote_confirm_mid_change_abs": None,
        "quote_confirm_mid_change_pct": None,
        "quote_confirm_seconds": None,
    }


def should_send_combined_alert(
    contract_symbol_value: str,
    now: dt.datetime,
    row: dict[str, Any],
    last_alert_sent_at: dict[str, dt.datetime],
) -> bool:
    last_sent = last_alert_sent_at.get(contract_symbol_value)
    if last_sent is not None and now - last_sent < ALERT_COOLDOWN:
        return False

    return passes_volume_dominance_rule(row)


def insert_backtest_signal_event(
    con: duckdb.DuckDBPyConnection,
    *,
    run_id: str,
    signal_id: str,
    trade_date: dt.date,
    alert_timestamp: dt.datetime,
    event_source: str,
    row: dict[str, Any],
    z_35d: float | None,
    z_3d: float | None,
    confirmation: UnderlyingConfirmation | None,
    alert_message: str,
) -> None:
    con.execute(
        """
        INSERT INTO backtest_signal_events (
            run_id,
            signal_id,
            trade_date,
            alert_timestamp,
            event_source,
            alert_type,
            parent_symbol,
            contract_symbol,
            strike,
            expiration_date,
            side,
            grouping,
            moneyness_grouping,
            decay_bucket,
            threshold,
            metric_value,
            z_35d,
            z_3d,
            option_mid,
            underlying_price,
            current_iv,
            rolling_volume_10m,
            rolling_volume_30m,
            rolling_volume_1h,
            z_vol_35d,
            z_vol_3d,
            z_mid_35d,
            z_mid_3d,
            z_iv_35d,
            z_iv_3d,
            z_vol_35d_band,
            z_vol_3d_band,
            z_mid_35d_band,
            z_mid_3d_band,
            z_iv_35d_band,
            z_iv_3d_band,
            source_vol_3d,
            source_mid_3d,
            source_iv_3d,
            breakout_direction,
            breakout_level,
            underlying_entry_price,
            quote_reference_mid,
            quote_confirm_mid_change_abs,
            quote_confirm_mid_change_pct,
            quote_confirm_seconds,
            alert_message
        ) VALUES (
            ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
        )
        """,
        [
            run_id,
            signal_id,
            trade_date,
            alert_timestamp.replace(tzinfo=None),
            event_source,
            "combined",
            row["parent_symbol"],
            row["contract_symbol"],
            row["strike"],
            row["expiration_date"],
            row["side"],
            row["grouping"],
            row["grouping"],
            row["decay_bucket"],
            ALERT_Z_THRESHOLD,
            row.get("mid"),
            z_35d,
            z_3d,
            row.get("mid"),
            row.get("underlying_price"),
            row.get("current_iv"),
            row.get("rolling_volume_10m"),
            row.get("rolling_volume_30m"),
            row.get("rolling_volume_1h"),
            row.get("z_vol_35d"),
            row.get("z_vol_3d"),
            row.get("z_mid_35d"),
            row.get("z_mid_3d"),
            row.get("z_iv_35d"),
            row.get("z_iv_3d"),
            zscore_band(row.get("z_vol_35d")),
            zscore_band(row.get("z_vol_3d")),
            zscore_band(row.get("z_mid_35d")),
            zscore_band(row.get("z_mid_3d")),
            zscore_band(row.get("z_iv_35d")),
            zscore_band(row.get("z_iv_3d")),
            row.get("source_vol_3d"),
            row.get("source_mid_3d"),
            row.get("source_iv_3d"),
            confirmation.direction if confirmation is not None else None,
            confirmation.breakout_level if confirmation is not None else None,
            confirmation.underlying_entry_price if confirmation is not None else None,
            row.get("quote_reference_mid"),
            row.get("quote_confirm_mid_change_abs"),
            row.get("quote_confirm_mid_change_pct"),
            row.get("quote_confirm_seconds"),
            alert_message,
        ],
    )


def initialize_active_signal(
    *,
    signal_id: str,
    trade_date: dt.date,
    alert_timestamp: dt.datetime,
    row: dict[str, Any],
    confirmation: UnderlyingConfirmation | None,
) -> dict[str, Any]:
    strategy_due = alert_timestamp + dt.timedelta(minutes=STRATEGY_EXIT_MINUTES)
    return {
        "signal_id": signal_id,
        "trade_date": trade_date,
        "alert_timestamp": alert_timestamp,
        "parent_symbol": row["parent_symbol"],
        "contract_symbol": row["contract_symbol"],
        "strike": row["strike"],
        "expiration_date": row["expiration_date"],
        "side": row["side"],
        "grouping": row["grouping"],
        "moneyness_grouping": row["grouping"],
        "decay_bucket": row["decay_bucket"],
        "z_vol_35d_band": zscore_band(row.get("z_vol_35d")),
        "z_vol_3d_band": zscore_band(row.get("z_vol_3d")),
        "z_mid_35d_band": zscore_band(row.get("z_mid_35d")),
        "z_mid_3d_band": zscore_band(row.get("z_mid_3d")),
        "z_iv_35d_band": zscore_band(row.get("z_iv_35d")),
        "z_iv_3d_band": zscore_band(row.get("z_iv_3d")),
        "breakout_direction": confirmation.direction if confirmation is not None else None,
        "breakout_level": confirmation.breakout_level if confirmation is not None else None,
        "underlying_entry_price": confirmation.underlying_entry_price if confirmation is not None else row.get("underlying_price"),
        "alert_option_mid": row.get("mid"),
        "alert_underlying_price": confirmation.underlying_entry_price if confirmation is not None else row.get("underlying_price"),
        "latest_option_mid": row.get("mid"),
        "latest_option_ts": row.get("last_quote_ts") or alert_timestamp,
        "latest_underlying_price": confirmation.underlying_entry_price if confirmation is not None else row.get("underlying_price"),
        "latest_underlying_ts": confirmation.bar_timestamp if confirmation is not None else alert_timestamp,
        "best_option_mid": row.get("mid"),
        "best_option_mid_ts": row.get("last_quote_ts") or alert_timestamp,
        "worst_option_mid": row.get("mid"),
        "worst_option_mid_ts": row.get("last_quote_ts") or alert_timestamp,
        "strategy_exit_due_ts": strategy_due,
        "strategy_exit_ts": None,
        "strategy_exit_reason": None,
        "strategy_exit_option_mid": None,
        "strategy_exit_underlying_price": None,
        "checkpoint_option_returns": {},
        "remaining_checkpoints": {
            label: alert_timestamp + delta
            for label, delta in CHECKPOINTS.items()
        },
    }


def insert_backtest_checkpoint(
    con: duckdb.DuckDBPyConnection,
    *,
    run_id: str,
    signal: dict[str, Any],
    checkpoint_label: str,
    due_timestamp: dt.datetime,
) -> float | None:
    option_quote_timestamp = signal.get("latest_option_ts")
    option_quote_age_seconds = None
    if option_quote_timestamp is not None:
        option_quote_age_seconds = (due_timestamp - option_quote_timestamp).total_seconds()

    option_mid = signal.get("latest_option_mid")
    underlying_price = signal.get("latest_underlying_price")
    option_return_pct = pct_return(option_mid, signal.get("alert_option_mid"))
    underlying_return_pct = pct_return(underlying_price, signal.get("alert_underlying_price"))

    status = "captured"
    if option_mid is None:
        status = "missing_option_quote"
    elif underlying_price is None:
        status = "missing_underlying_price"

    con.execute(
        """
        INSERT INTO backtest_signal_checkpoints (
            run_id,
            signal_id,
            trade_date,
            alert_timestamp,
            checkpoint_label,
            due_timestamp,
            captured_timestamp,
            contract_symbol,
            parent_symbol,
            strike,
            expiration_date,
            side,
            grouping,
            moneyness_grouping,
            decay_bucket,
            option_mid,
            option_quote_timestamp,
            option_quote_age_seconds,
            underlying_price,
            option_return_pct,
            underlying_return_pct,
            status
        ) VALUES (
            ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
        )
        """,
        [
            run_id,
            signal["signal_id"],
            signal["trade_date"],
            signal["alert_timestamp"].replace(tzinfo=None),
            checkpoint_label,
            due_timestamp.replace(tzinfo=None),
            due_timestamp.replace(tzinfo=None),
            signal["contract_symbol"],
            signal["parent_symbol"],
            signal["strike"],
            signal["expiration_date"],
            signal["side"],
            signal["grouping"],
            signal["moneyness_grouping"],
            signal["decay_bucket"],
            option_mid,
            option_quote_timestamp.replace(tzinfo=None) if option_quote_timestamp is not None else None,
            option_quote_age_seconds,
            underlying_price,
            option_return_pct,
            underlying_return_pct,
            status,
        ],
    )
    return option_return_pct


def insert_backtest_outcome(
    con: duckdb.DuckDBPyConnection,
    *,
    run_id: str,
    signal: dict[str, Any],
    finalized_timestamp: dt.datetime,
) -> None:
    option_return_5m = signal["checkpoint_option_returns"].get("5m")
    option_return_15m = signal["checkpoint_option_returns"].get("15m")
    option_return_30m = signal["checkpoint_option_returns"].get("30m")
    option_return_1h = signal["checkpoint_option_returns"].get("1h")
    best_option_mid = signal.get("best_option_mid")
    worst_option_mid = signal.get("worst_option_mid")
    alert_option_mid = signal.get("alert_option_mid")
    strategy_exit_ts = signal.get("strategy_exit_ts")
    strategy_exit_option_mid = signal.get("strategy_exit_option_mid")

    con.execute(
        """
        INSERT INTO backtest_signal_outcomes (
            run_id,
            signal_id,
            trade_date,
            alert_timestamp,
            finalized_timestamp,
            parent_symbol,
            contract_symbol,
            strike,
            expiration_date,
            side,
            grouping,
            moneyness_grouping,
            decay_bucket,
            z_vol_35d_band,
            z_vol_3d_band,
            z_mid_35d_band,
            z_mid_3d_band,
            z_iv_35d_band,
            z_iv_3d_band,
            option_return_5m,
            option_return_15m,
            option_return_30m,
            option_return_1h,
            label_15m,
            label_30m,
            label_1h,
            max_up_pct,
            max_down_pct,
            mfe_pct,
            mae_pct,
            best_option_mid,
            best_option_mid_timestamp,
            worst_option_mid,
            worst_option_mid_timestamp,
            breakout_direction,
            breakout_level,
            underlying_entry_price,
            strategy_exit_timestamp,
            strategy_exit_reason,
            strategy_exit_option_mid,
            strategy_exit_underlying_price,
            strategy_return_pct
        ) VALUES (
            ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
        )
        """,
        [
            run_id,
            signal["signal_id"],
            signal["trade_date"],
            signal["alert_timestamp"].replace(tzinfo=None),
            finalized_timestamp.replace(tzinfo=None),
            signal["parent_symbol"],
            signal["contract_symbol"],
            signal["strike"],
            signal["expiration_date"],
            signal["side"],
            signal["grouping"],
            signal["moneyness_grouping"],
            signal["decay_bucket"],
            signal.get("z_vol_35d_band"),
            signal.get("z_vol_3d_band"),
            signal.get("z_mid_35d_band"),
            signal.get("z_mid_3d_band"),
            signal.get("z_iv_35d_band"),
            signal.get("z_iv_3d_band"),
            option_return_5m,
            option_return_15m,
            option_return_30m,
            option_return_1h,
            label_signal_return(option_return_15m),
            label_signal_return(option_return_30m),
            label_signal_return(option_return_1h),
            pct_return(best_option_mid, alert_option_mid),
            pct_return(worst_option_mid, alert_option_mid),
            pct_return(best_option_mid, alert_option_mid),
            pct_return(worst_option_mid, alert_option_mid),
            best_option_mid,
            signal["best_option_mid_ts"].replace(tzinfo=None) if signal.get("best_option_mid_ts") is not None else None,
            worst_option_mid,
            signal["worst_option_mid_ts"].replace(tzinfo=None) if signal.get("worst_option_mid_ts") is not None else None,
            signal.get("breakout_direction"),
            signal.get("breakout_level"),
            signal.get("underlying_entry_price"),
            strategy_exit_ts.replace(tzinfo=None) if strategy_exit_ts is not None else None,
            signal.get("strategy_exit_reason"),
            strategy_exit_option_mid,
            signal.get("strategy_exit_underlying_price"),
            pct_return(strategy_exit_option_mid, alert_option_mid),
        ],
    )


def mark_strategy_exit(
    signal: dict[str, Any],
    *,
    timestamp: dt.datetime,
    reason: str,
) -> None:
    if signal.get("strategy_exit_ts") is not None:
        return
    signal["strategy_exit_ts"] = timestamp
    signal["strategy_exit_reason"] = reason
    signal["strategy_exit_option_mid"] = signal.get("latest_option_mid")
    signal["strategy_exit_underlying_price"] = signal.get("latest_underlying_price")


def mark_due_strategy_time_exits(
    active_signals: dict[str, dict[str, Any]],
    horizon_ts: dt.datetime,
) -> None:
    for signal in active_signals.values():
        due_ts = signal.get("strategy_exit_due_ts")
        if due_ts is not None and due_ts <= horizon_ts:
            mark_strategy_exit(signal, timestamp=due_ts, reason="time_15m")


def capture_due_checkpoints_until(
    con: duckdb.DuckDBPyConnection,
    *,
    run_id: str,
    horizon_ts: dt.datetime,
    active_signals: dict[str, dict[str, Any]],
    signals_by_contract: dict[str, set[str]],
    signals_by_parent: dict[str, set[str]],
) -> int:
    completed_signal_ids: list[str] = []
    mark_due_strategy_time_exits(active_signals, horizon_ts)

    for signal_id, signal in list(active_signals.items()):
        due_labels = [
            label
            for label, due_timestamp in signal["remaining_checkpoints"].items()
            if due_timestamp <= horizon_ts
        ]
        if not due_labels:
            continue

        for label in sorted(due_labels, key=lambda item: signal["remaining_checkpoints"][item]):
            due_timestamp = signal["remaining_checkpoints"].pop(label)
            option_return_pct = insert_backtest_checkpoint(
                con,
                run_id=run_id,
                signal=signal,
                checkpoint_label=label,
                due_timestamp=due_timestamp,
            )
            signal["checkpoint_option_returns"][label] = option_return_pct

        if not signal["remaining_checkpoints"]:
            completed_signal_ids.append(signal_id)

    for signal_id in completed_signal_ids:
        signal = active_signals.pop(signal_id, None)
        if signal is None:
            continue
        final_due = signal["alert_timestamp"] + max(CHECKPOINTS.values())
        insert_backtest_outcome(
            con,
            run_id=run_id,
            signal=signal,
            finalized_timestamp=final_due,
        )
        contract_value = signal["contract_symbol"]
        signals_by_contract[contract_value].discard(signal_id)
        if not signals_by_contract[contract_value]:
            signals_by_contract.pop(contract_value, None)
        parent_value = signal["parent_symbol"]
        signals_by_parent[parent_value].discard(signal_id)
        if not signals_by_parent[parent_value]:
            signals_by_parent.pop(parent_value, None)

    return len(completed_signal_ids)


def update_active_signals_for_quote(
    contract_value: str,
    row: dict[str, Any],
    active_signals: dict[str, dict[str, Any]],
    signals_by_contract: dict[str, set[str]],
) -> None:
    signal_ids = signals_by_contract.get(contract_value)
    if not signal_ids:
        return
    for signal_id in list(signal_ids):
        signal = active_signals.get(signal_id)
        if signal is None:
            continue
        latest_option_mid = row.get("mid")
        latest_option_ts = row.get("last_quote_ts")
        signal["latest_option_mid"] = latest_option_mid
        signal["latest_option_ts"] = latest_option_ts
        signal["latest_underlying_price"] = row.get("underlying_price")

        if latest_option_mid is None:
            continue
        if signal.get("best_option_mid") is None or latest_option_mid > signal["best_option_mid"]:
            signal["best_option_mid"] = latest_option_mid
            signal["best_option_mid_ts"] = latest_option_ts
        if signal.get("worst_option_mid") is None or latest_option_mid < signal["worst_option_mid"]:
            signal["worst_option_mid"] = latest_option_mid
            signal["worst_option_mid_ts"] = latest_option_ts


def update_active_signals_for_underlying(
    parent_symbol: str,
    bar: UnderlyingBar,
    active_signals: dict[str, dict[str, Any]],
    signals_by_parent: dict[str, set[str]],
) -> None:
    signal_ids = signals_by_parent.get(parent_symbol)
    if not signal_ids:
        return

    for signal_id in list(signal_ids):
        signal = active_signals.get(signal_id)
        if signal is None:
            continue
        signal["latest_underlying_price"] = bar.close
        signal["latest_underlying_ts"] = bar.timestamp
        if bar.timestamp <= signal["alert_timestamp"]:
            continue
        if signal.get("strategy_exit_ts") is not None:
            continue
        if underlying_fail_triggered(
            side=signal["side"],
            close_price=bar.close,
            breakout_level=signal.get("breakout_level"),
        ):
            mark_strategy_exit(signal, timestamp=bar.timestamp, reason="underlying_level_fail")


def maybe_queue_combined_alert(
    con: duckdb.DuckDBPyConnection,
    *,
    run_id: str,
    trade_date: dt.date,
    row: dict[str, Any],
    now: dt.datetime,
    event_source: str,
    underlying_bars_by_parent: dict[str, list[UnderlyingBar]],
    filter_stats: dict[str, int],
    last_alert_sent_at: dict[str, dt.datetime],
    alerted_contracts: set[str],
    active_signals: dict[str, dict[str, Any]],
    signals_by_contract: dict[str, set[str]],
    signals_by_parent: dict[str, set[str]],
) -> bool:
    contract_value = row["contract_symbol"]
    if event_source != "quote":
        return False
    if FIRST_ALERT_ONLY and contract_value in alerted_contracts:
        filter_stats["skipped_not_first_alert"] += 1
        return False
    if not should_send_combined_alert(contract_value, now, row, last_alert_sent_at):
        return False
    if not passes_signal_context_filter(row):
        filter_stats["skipped_context_filter"] += 1
        return False

    confirmation: UnderlyingConfirmation | None = None
    if UNDERLYING_CONFIRMATION_ENABLED:
        confirmation = evaluate_underlying_confirmation(
            underlying_bars_by_parent.get(str(row["parent_symbol"]), []),
            side=str(row["side"]),
            alert_timestamp=now,
        )
        if confirmation.status == "missing_underlying":
            filter_stats["skipped_missing_underlying"] += 1
            return False
        if confirmation.status != "passed":
            filter_stats["skipped_no_breakout"] += 1
            return False
        filter_stats["passed_underlying_confirmation"] += 1

    signal_id = str(uuid.uuid4())
    z_35d = min(row["z_vol_35d"], row["z_mid_35d"], row["z_iv_35d"])
    z_3d = min(row["z_vol_3d"], row["z_mid_3d"], row["z_iv_3d"])
    alert_message = (
        f"Combined alert for {contract_value}: parent={row['parent_symbol']} "
        f"underlying={debug_num(row.get('underlying_price'))} strike={debug_num(row.get('strike'), 3)} "
        f"side={row['side']} grouping={row['grouping']} "
        f"vol(z35={row['z_vol_35d']:.2f}, z3={row['z_vol_3d']:.2f}) "
        f"mid(z35={row['z_mid_35d']:.2f}, z3={row['z_mid_3d']:.2f}) "
        f"iv(z35={row['z_iv_35d']:.2f}, z3={row['z_iv_3d']:.2f})"
    )

    insert_backtest_signal_event(
        con,
        run_id=run_id,
        signal_id=signal_id,
        trade_date=trade_date,
        alert_timestamp=now,
        event_source=event_source,
        row=row,
        z_35d=z_35d,
        z_3d=z_3d,
        confirmation=confirmation,
        alert_message=alert_message,
    )
    signal = initialize_active_signal(
        signal_id=signal_id,
        trade_date=trade_date,
        alert_timestamp=now,
        row=row,
        confirmation=confirmation,
    )
    active_signals[signal_id] = signal
    signals_by_contract[contract_value].add(signal_id)
    signals_by_parent[str(row["parent_symbol"])].add(signal_id)
    last_alert_sent_at[contract_value] = now
    alerted_contracts.add(contract_value)
    return True


def replay_day(
    hist_con: duckdb.DuckDBPyConnection,
    out_con: duckdb.DuckDBPyConnection,
    *,
    run_id: str,
    trade_date: dt.date,
    parents: list[str] | None,
) -> dict[str, int]:
    contracts = load_day_contracts(hist_con, trade_date=trade_date, parents=parents)
    if not contracts:
        return {"contracts": 0, "events": 0, "alerts": 0, "outcomes": 0}

    baselines = load_baselines_for_day(hist_con, trade_date=trade_date, contracts=contracts)
    contract_metadata = {contract["contract_symbol"]: contract for contract in contracts}
    parents_for_day = sorted({str(contract["parent_symbol"]) for contract in contracts})
    underlying_bars_by_parent = ensure_underlying_bars_for_day(
        out_con,
        parents=parents_for_day,
        trade_date=trade_date,
    )

    volume_lookup: dict[tuple[dt.date, str, str, str, str, int], str] = {}
    ambiguous_volume_keys: set[tuple[dt.date, str, str, str, str, int]] = set()
    for contract in contracts:
        key = volume_key(contract, trade_date)
        existing = volume_lookup.get(key)
        if existing is not None and existing != contract["contract_symbol"]:
            ambiguous_volume_keys.add(key)
        else:
            volume_lookup[key] = contract["contract_symbol"]
    for key in ambiguous_volume_keys:
        volume_lookup.pop(key, None)

    parent_sql, parent_params = parent_filter_sql(parents)
    events = hist_con.execute(
        f"""
        SELECT *
        FROM (
            SELECT
                0 AS event_sort,
                'quote' AS event_type,
                timestamp,
                parent_symbol,
                strike,
                side,
                days_to_expiry,
                expiration_date,
                grouping,
                time_decay_bucket,
                underlying_price,
                mid,
                iv,
                CAST(NULL AS BIGINT) AS rolling_volume_10m
            FROM option_snapshots_raw
            WHERE CAST(timestamp AS DATE) = ?
            {parent_sql}

            UNION ALL

            SELECT
                1 AS event_sort,
                'volume' AS event_type,
                timestamp,
                parent_symbol,
                CAST(NULL AS DOUBLE) AS strike,
                side,
                days_to_expiry,
                CAST(NULL AS DATE) AS expiration_date,
                grouping,
                time_decay_bucket,
                CAST(NULL AS DOUBLE) AS underlying_price,
                CAST(NULL AS DOUBLE) AS mid,
                CAST(NULL AS DOUBLE) AS iv,
                rolling_volume_10m
            FROM rolling_volume_history
            WHERE CAST(timestamp AS DATE) = ?
            {parent_sql}
        )
        ORDER BY timestamp, event_sort, parent_symbol, side, grouping, time_decay_bucket
        """,
        [trade_date, *parent_params, trade_date, *parent_params],
    ).fetchall()
    underlying_events = [
        (
            2,
            "underlying",
            bar.timestamp,
            parent_symbol,
            None,
            None,
            None,
            None,
            None,
            None,
            bar.close,
            None,
            None,
            None,
        )
        for parent_symbol, bars in underlying_bars_by_parent.items()
        for bar in bars
    ]
    events = sorted(
        [*events, *underlying_events],
        key=lambda row: (ensure_utc_datetime(row[2]), int(row[0]), str(row[3])),
    )

    contract_states = {
        contract_value: build_contract_state(metadata)
        for contract_value, metadata in contract_metadata.items()
    }
    active_signals: dict[str, dict[str, Any]] = {}
    signals_by_contract: dict[str, set[str]] = defaultdict(set)
    signals_by_parent: dict[str, set[str]] = defaultdict(set)
    last_alert_sent_at: dict[str, dt.datetime] = {}
    alerted_contracts: set[str] = set()
    pending_quote_candidates: dict[str, dict[str, Any]] = {}
    filter_stats = {
        "option_rule_candidates": 0,
        "skipped_not_first_alert": 0,
        "skipped_context_filter": 0,
        "quote_confirmation_pending": 0,
        "passed_quote_confirmation": 0,
        "skipped_missing_reference_quote": 0,
        "skipped_quote_confirmation": 0,
        "skipped_quote_confirmation_expired": 0,
        "passed_underlying_confirmation": 0,
        "skipped_missing_underlying": 0,
        "skipped_no_breakout": 0,
    }

    event_count = 0
    alerts_sent = 0
    outcomes_written = 0
    last_event_ts: dt.datetime | None = None

    for (
        _event_sort,
        event_type,
        timestamp,
        parent_symbol,
        strike,
        side,
        days_to_expiry,
        expiration_date,
        grouping,
        decay_bucket,
        underlying_price,
        mid,
        iv,
        rolling_volume_10m,
    ) in events:
        event_ts = ensure_utc_datetime(timestamp)
        outcomes_written += capture_due_checkpoints_until(
            out_con,
            run_id=run_id,
            horizon_ts=event_ts,
            active_signals=active_signals,
            signals_by_contract=signals_by_contract,
            signals_by_parent=signals_by_parent,
        )

        if event_type == "underlying":
            update_active_signals_for_underlying(
                str(parent_symbol),
                UnderlyingBar(
                    timestamp=event_ts,
                    open=float(underlying_price),
                    high=float(underlying_price),
                    low=float(underlying_price),
                    close=float(underlying_price),
                    volume=None,
                ),
                active_signals,
                signals_by_parent,
            )
        elif event_type == "quote":
            expiration_date_value = expiration_date
            strike_value = float(strike)
            contract_value = contract_symbol(
                str(parent_symbol),
                expiration_date_value,
                strike_value,
                str(side),
            )
            row = contract_states.get(contract_value)
            if row is None:
                continue
            baseline = baselines.get(combo_key(row))

            row["mid"] = float(mid) if mid is not None else None
            row["underlying_price"] = float(underlying_price) if underlying_price is not None else None
            row["current_iv"] = float(iv) if iv is not None else None
            row["last_quote_ts"] = event_ts
            row["updated_at"] = event_ts

            if baseline is not None:
                row["mean_mid_35d"] = baseline.get("mean_mid_35d")
                row["std_mid_35d"] = baseline.get("std_mid_35d")
                row["mean_mid_3d"] = baseline.get("mean_mid_3d")
                row["std_mid_3d"] = baseline.get("std_mid_3d")
                row["mean_iv_35d"] = baseline.get("mean_iv_35d")
                row["std_iv_35d"] = baseline.get("std_iv_35d")
                row["mean_iv_3d"] = baseline.get("mean_iv_3d")
                row["std_iv_3d"] = baseline.get("std_iv_3d")
                row["source_mid_3d"] = baseline.get("source_mid_3d")
                row["source_iv_3d"] = baseline.get("source_iv_3d")

                row["z_mid_35d"] = safe_zscore(
                    row.get("mid"),
                    baseline.get("mean_mid_35d"),
                    baseline.get("std_mid_35d"),
                )
                row["z_mid_3d"] = safe_zscore(
                    row.get("mid"),
                    baseline.get("mean_mid_3d"),
                    baseline.get("std_mid_3d"),
                )

                if row.get("current_iv") is not None:
                    row["z_iv_35d"] = safe_zscore(
                        row.get("current_iv"),
                        baseline.get("mean_iv_35d"),
                        baseline.get("std_iv_35d"),
                    )
                    row["z_iv_3d"] = safe_zscore(
                        row.get("current_iv"),
                        baseline.get("mean_iv_3d"),
                        baseline.get("std_iv_3d"),
                    )

            update_active_signals_for_quote(
                contract_value,
                row,
                active_signals,
                signals_by_contract,
            )
            pending_quote = pending_quote_candidates.get(contract_value)
            if pending_quote is not None:
                volume_ts = ensure_utc_datetime(pending_quote["volume_timestamp"])
                if event_ts - volume_ts > QUOTE_CONFIRM_WINDOW:
                    filter_stats["skipped_quote_confirmation_expired"] += 1
                    pending_quote_candidates.pop(contract_value, None)
                elif not passes_volume_dominance_rule(row):
                    filter_stats["skipped_quote_confirmation"] += 1
                elif quote_confirmation_passed(pending_quote.get("reference_mid"), row.get("mid")):
                    filter_stats["passed_quote_confirmation"] += 1
                    pending_quote_candidates.pop(contract_value, None)
                    quote_reference_mid = finite_float(pending_quote.get("reference_mid"))
                    quote_current_mid = finite_float(row.get("mid"))
                    if quote_reference_mid is not None and quote_current_mid is not None:
                        row["quote_reference_mid"] = quote_reference_mid
                        row["quote_confirm_mid_change_abs"] = quote_current_mid - quote_reference_mid
                        row["quote_confirm_mid_change_pct"] = (
                            quote_current_mid - quote_reference_mid
                        ) / quote_reference_mid
                        row["quote_confirm_seconds"] = (event_ts - volume_ts).total_seconds()
                    if maybe_queue_combined_alert(
                        out_con,
                        run_id=run_id,
                        trade_date=trade_date,
                        row=row,
                        now=event_ts,
                        event_source="quote",
                        underlying_bars_by_parent=underlying_bars_by_parent,
                        filter_stats=filter_stats,
                        last_alert_sent_at=last_alert_sent_at,
                        alerted_contracts=alerted_contracts,
                        active_signals=active_signals,
                        signals_by_contract=signals_by_contract,
                        signals_by_parent=signals_by_parent,
                    ):
                        alerts_sent += 1
                else:
                    filter_stats["skipped_quote_confirmation"] += 1
        else:
            key = (
                trade_date,
                str(parent_symbol),
                str(side),
                str(grouping),
                str(decay_bucket),
                int(days_to_expiry),
            )
            contract_value = volume_lookup.get(key)
            if contract_value is None:
                continue
            row = contract_states.get(contract_value)
            if row is None:
                continue

            new_volume = int(rolling_volume_10m) if rolling_volume_10m is not None else 0
            if row.get("rolling_volume_10m") == new_volume:
                last_event_ts = event_ts
                event_count += 1
                continue

            baseline = baselines.get(combo_key(row))
            row["rolling_volume_10m"] = new_volume
            row["last_trade_ts"] = event_ts
            row["last_volume_update_ts"] = event_ts
            row["updated_at"] = event_ts

            if baseline is not None:
                row["mean_vol_35d"] = baseline.get("mean_vol_35d")
                row["std_vol_35d"] = baseline.get("std_vol_35d")
                row["mean_vol_3d"] = baseline.get("mean_vol_3d")
                row["std_vol_3d"] = baseline.get("std_vol_3d")
                row["source_vol_3d"] = baseline.get("source_vol_3d")

                row["z_vol_35d"] = safe_zscore(
                    new_volume,
                    baseline.get("mean_vol_35d"),
                    baseline.get("std_vol_35d"),
                )
                row["z_vol_3d"] = safe_zscore(
                    new_volume,
                    baseline.get("mean_vol_3d"),
                    baseline.get("std_vol_3d"),
                )

            volume_rule_passes = passes_volume_dominance_rule(row)
            if volume_rule_passes and not bool(row.get("volume_rule_active")):
                row["volume_rule_active"] = True
                filter_stats["option_rule_candidates"] += 1
                if FIRST_ALERT_ONLY and contract_value in alerted_contracts:
                    filter_stats["skipped_not_first_alert"] += 1
                elif not passes_signal_context_filter(row):
                    filter_stats["skipped_context_filter"] += 1
                else:
                    reference_mid = finite_float(row.get("mid"))
                    if reference_mid is None or reference_mid <= 0:
                        filter_stats["skipped_missing_reference_quote"] += 1
                    else:
                        pending_quote_candidates[contract_value] = {
                            "volume_timestamp": event_ts,
                            "reference_mid": reference_mid,
                        }
                        filter_stats["quote_confirmation_pending"] += 1
            elif not volume_rule_passes:
                row["volume_rule_active"] = False
                pending_quote_candidates.pop(contract_value, None)

        last_event_ts = event_ts
        event_count += 1

    if last_event_ts is not None and active_signals:
        final_horizon = max(
            last_event_ts,
            max(
                signal["alert_timestamp"] + max(CHECKPOINTS.values())
                for signal in active_signals.values()
            ),
        )
        outcomes_written += capture_due_checkpoints_until(
            out_con,
            run_id=run_id,
            horizon_ts=final_horizon,
            active_signals=active_signals,
            signals_by_contract=signals_by_contract,
            signals_by_parent=signals_by_parent,
        )

    insert_filter_stats(out_con, run_id=run_id, trade_date=trade_date, stats=filter_stats)

    return {
        "contracts": len(contracts),
        "events": event_count,
        "alerts": alerts_sent,
        "outcomes": outcomes_written,
    }


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--start-date", type=parse_date)
    parser.add_argument("--end-date", type=parse_date)
    parser.add_argument("--parents", type=parse_parents, default=None)
    parser.add_argument("--options-db", default=OPTIONS_DB_PATH)
    parser.add_argument("--output-db", default=OUTPUT_DB_PATH)
    parser.add_argument("--report-dir", default=str(REPORT_DIR))
    return parser


def main() -> None:
    args = build_arg_parser().parse_args()

    started_at = dt.datetime.now(dt.timezone.utc)
    run_id = readable_utc_run_id("backtest", started_at)

    hist_con = duckdb.connect(args.options_db, read_only=True)
    start_date, end_date = resolve_date_range(
        hist_con,
        start_date=args.start_date,
        end_date=args.end_date,
        parents=args.parents,
    )
    start_date, end_date = apply_underlying_confirmation_date_limit(start_date, end_date)
    out_con = duckdb.connect(args.output_db)
    ensure_output_tables(out_con)
    insert_run_started(
        out_con,
        run_id=run_id,
        started_at=started_at,
        start_date=start_date,
        end_date=end_date,
        parents=args.parents,
    )

    trade_dates = fetch_trade_dates(
        hist_con,
        start_date=start_date,
        end_date=end_date,
        parents=args.parents,
    )
    debug(
        f"start run_id={run_id} dates={len(trade_dates)} "
        f"range={start_date}..{end_date} "
        f"parents={','.join(args.parents) if args.parents else 'ALL'}"
    )

    total_alerts = 0
    total_outcomes = 0
    for idx, trade_date in enumerate(trade_dates, start=1):
        day_stats = replay_day(
            hist_con,
            out_con,
            run_id=run_id,
            trade_date=trade_date,
            parents=args.parents,
        )
        total_alerts += day_stats["alerts"]
        total_outcomes += day_stats["outcomes"]
        debug(
            f"day {idx}/{len(trade_dates)} {trade_date} "
            f"contracts={day_stats['contracts']} events={day_stats['events']} "
            f"alerts={day_stats['alerts']} outcomes={day_stats['outcomes']}"
        )

    completed_at = dt.datetime.now(dt.timezone.utc)
    finalize_run(
        out_con,
        run_id=run_id,
        completed_at=completed_at,
        trade_days=len(trade_dates),
        alerts_total=total_alerts,
        outcomes_total=total_outcomes,
    )
    report_path = generate_run_report(
        out_con,
        run_id=run_id,
        report_dir=Path(args.report_dir),
    )
    hist_con.close()
    out_con.close()
    debug(
        f"done run_id={run_id} trade_days={len(trade_dates)} "
        f"alerts={total_alerts} outcomes={total_outcomes} output={args.output_db} "
        f"report={report_path}"
    )


if __name__ == "__main__":
    main()
