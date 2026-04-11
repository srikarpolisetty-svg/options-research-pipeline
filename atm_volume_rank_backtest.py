from __future__ import annotations

"""10-minute ATM +/-2 call ranking backtest.

Assumptions where the spec was silent:
- Nearest non-expired Friday expiration with at least five call strikes is used each trade day,
  excluding third Fridays by default.
- Underlying intraday move is measured with yfinance intraday data, preferring 15-minute bars
  when the requested history fits within yfinance's intraday window, otherwise falling back to 60-minute bars.
- Entry-time config is interpreted in America/New_York and applied against UTC data.
- Execution defaults to entry at ask and exit at bid.
"""

import argparse
from datetime import date, datetime, time as dt_time, timedelta, timezone
from zoneinfo import ZoneInfo

import databento as db
import numpy as np
import pandas as pd
import yfinance as yf

from backtest_data_utils import (
    _load_option_definitions,
    _load_quotes_for_raws,
    _load_trades_for_raws,
    _request_floor,
)
from config import DATABENTO_API_KEY
from policy.expiration import is_third_friday

UTC_TZ = ZoneInfo("UTC")
SESSION_TZ = ZoneInfo("America/New_York")
MARKET_OPEN = dt_time(9, 30)
MARKET_CLOSE = dt_time(16, 0)
YFINANCE_INTRADAY_LIMIT_DAYS = 60

CONFIG = {
    "symbols": ["SPY"],
    "start": "2025-03-21",
    "end": "2026-03-20",
    "checkpoint_minutes": 10,
    "underlying_intraday_interval": "auto",
    "entry_times_local": [],
    "entry_time_start_local": None,
    "entry_time_end_local": None,
    "entry_weekdays": None,
    "volume_lookback_bars": 6,
    "strike_span": 2,
    "friday_only": True,
    "exclude_third_friday": True,
    "underlying_day_move_min": 0.01,
    "volume_z_min": 2.0,
    "max_trades_per_symbol_per_day": 1,
    "max_trades_per_day": 1,
    "max_trades_total": None,
    "take_profit_pct": 0.20,
    "stop_loss_pct": 0.10,
    "entry_price_field": "ask",
    "exit_price_field": "bid",
}


def _normalize_backtest_dates(start_value: str, end_value: str) -> tuple[date, date]:
    start_d = date.fromisoformat(start_value)
    end_d = date.fromisoformat(end_value)
    today_utc = datetime.now(timezone.utc).date()
    if end_d >= today_utc:
        end_d = today_utc - timedelta(days=1)
    if start_d >= end_d:
        raise RuntimeError("start must be before end")
    return start_d, end_d


def _coerce_history_index_utc(frame: pd.DataFrame) -> pd.DataFrame:
    frame = frame.copy()
    if frame.index.tz is None:
        frame.index = frame.index.tz_localize("UTC")
    else:
        frame.index = frame.index.tz_convert("UTC")
    return frame


def _resolve_underlying_interval(start_d: date, end_d: date, request_floor_d: date, config: dict) -> str:
    configured = str(config.get("underlying_intraday_interval", "auto")).lower()
    if configured != "auto":
        return configured

    request_start = max(start_d - timedelta(days=10), request_floor_d)
    span_days = (end_d - request_start).days
    if span_days < YFINANCE_INTRADAY_LIMIT_DAYS:
        return "15m"
    return "60m"


def _load_underlying_context(symbol: str, start_d: date, end_d: date, config: dict) -> tuple[pd.DataFrame, pd.DataFrame]:
    request_floor_d = _request_floor(end_d)
    intraday_interval = _resolve_underlying_interval(start_d, end_d, request_floor_d, config)
    intraday = yf.Ticker(symbol).history(
        start=max(start_d - timedelta(days=10), request_floor_d).isoformat(),
        end=(end_d + timedelta(days=1)).isoformat(),
        interval=intraday_interval,
        auto_adjust=False,
    )
    if intraday is None or intraday.empty or "Close" not in intraday.columns:
        raise RuntimeError(f"No intraday underlying data from yfinance for {symbol}.")

    daily = yf.Ticker(symbol).history(
        start=max(start_d - timedelta(days=10), request_floor_d).isoformat(),
        end=(end_d + timedelta(days=1)).isoformat(),
        interval="1d",
        auto_adjust=False,
    )
    if daily is None or daily.empty or "Open" not in daily.columns or "Close" not in daily.columns:
        raise RuntimeError(f"No daily underlying data from yfinance for {symbol}.")

    intraday = _coerce_history_index_utc(intraday)
    intraday["minute"] = intraday.index.floor("1min")
    intraday_df = (
        intraday.groupby("minute", as_index=False)["Close"]
        .last()
        .rename(columns={"Close": "underlying_price"})
        .sort_values("minute")
        .reset_index(drop=True)
    )

    daily = _coerce_history_index_utc(daily)
    daily["trade_date"] = daily.index.date
    daily_df = (
        daily.groupby("trade_date", as_index=False)
        .agg(day_open=("Open", "first"), day_close=("Close", "last"))
        .sort_values("trade_date")
        .reset_index(drop=True)
    )
    return intraday_df, daily_df


def _build_active_expiration_map(defs: pd.DataFrame, trading_dates: list[date], config: dict) -> pd.DataFrame:
    calls = defs[defs["instrument_class"].astype(str) == "C"].copy()
    if calls.empty:
        raise RuntimeError("No call definitions were available for the selected symbol.")

    friday_only = bool(config.get("friday_only", True))
    exclude_third_friday = bool(config.get("exclude_third_friday", True))
    if friday_only:
        calls = calls[calls["exp_date"].map(lambda d: d.weekday() == 4)].copy()
    if exclude_third_friday:
        calls = calls[~calls["exp_date"].map(is_third_friday)].copy()
    if calls.empty:
        raise RuntimeError("No weekly Friday call expirations were available after filters.")

    strike_counts = (
        calls.groupby("exp_date", as_index=False)["strike_f"]
        .nunique()
        .rename(columns={"strike_f": "strike_count"})
        .sort_values("exp_date")
        .reset_index(drop=True)
    )

    rows: list[dict[str, object]] = []
    for trade_date in trading_dates:
        valid = strike_counts[
            (strike_counts["exp_date"] >= trade_date) & (strike_counts["strike_count"] >= 5)
        ]
        if valid.empty:
            continue
        rows.append(
            {
                "trade_date": trade_date,
                "expiration_date": valid.iloc[0]["exp_date"],
            }
        )

    active = pd.DataFrame(rows)
    if active.empty:
        raise RuntimeError("No active expirations with at least five call strikes were found.")
    return active


def _build_market_minutes(trade_date: date) -> pd.DatetimeIndex:
    start_local = datetime.combine(trade_date, MARKET_OPEN, tzinfo=SESSION_TZ)
    end_local = datetime.combine(trade_date, MARKET_CLOSE, tzinfo=SESSION_TZ)
    return pd.date_range(start=start_local, end=end_local, freq="1min").tz_convert("UTC")


def _is_checkpoint_timestamp(minute_session: pd.Series, checkpoint_minutes: int) -> pd.Series:
    minute_of_day = (minute_session.dt.hour * 60) + minute_session.dt.minute
    open_minutes = (MARKET_OPEN.hour * 60) + MARKET_OPEN.minute
    close_minutes = (MARKET_CLOSE.hour * 60) + MARKET_CLOSE.minute
    return (
        (minute_of_day >= open_minutes + checkpoint_minutes)
        & (minute_of_day < close_minutes)
        & (((minute_of_day - open_minutes) % checkpoint_minutes) == 0)
    )


def _parse_time_string(value: str) -> dt_time:
    parsed = datetime.strptime(str(value), "%H:%M")
    return dt_time(parsed.hour, parsed.minute)


def _minute_of_day(series: pd.Series) -> pd.Series:
    return (series.dt.hour * 60) + series.dt.minute


def _ensure_entry_context(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df.copy()

    enriched = df.copy()
    if "minute_utc" not in enriched.columns:
        enriched["minute_utc"] = pd.to_datetime(enriched["minute"], utc=True, errors="coerce").dt.tz_convert(UTC_TZ)
    if "time_utc" not in enriched.columns:
        enriched["time_utc"] = enriched["minute_utc"].dt.strftime("%H:%M")
    if "weekday_utc" not in enriched.columns:
        enriched["weekday_utc"] = enriched["minute_utc"].dt.day_name()
    return enriched


def _apply_entry_constraints(checkpoints: pd.DataFrame, config: dict) -> pd.DataFrame:
    if checkpoints.empty:
        return checkpoints.copy()

    filtered = _ensure_entry_context(checkpoints)

    minute_session = filtered["minute_utc"].dt.tz_convert(SESSION_TZ)
    time_session = minute_session.dt.strftime("%H:%M")
    weekday_session = minute_session.dt.day_name()

    entry_weekdays = config.get("entry_weekdays")
    if entry_weekdays:
        allowed_weekdays = {str(value).strip().lower() for value in entry_weekdays}
        filtered = filtered[weekday_session.str.lower().isin(allowed_weekdays)].copy()
        minute_session = filtered["minute_utc"].dt.tz_convert(SESSION_TZ)
        time_session = minute_session.dt.strftime("%H:%M")

    entry_times_local = config.get("entry_times_local")
    if entry_times_local:
        allowed_times = {_parse_time_string(value).strftime("%H:%M") for value in entry_times_local}
        filtered = filtered[time_session.isin(allowed_times)].copy()
        minute_session = filtered["minute_utc"].dt.tz_convert(SESSION_TZ)

    start_local = config.get("entry_time_start_local")
    end_local = config.get("entry_time_end_local")
    if start_local is not None or end_local is not None:
        start_parsed = _parse_time_string(start_local) if start_local is not None else None
        end_parsed = _parse_time_string(end_local) if end_local is not None else None
        if start_parsed is not None and end_parsed is not None:
            start_total = (start_parsed.hour * 60) + start_parsed.minute
            end_total = (end_parsed.hour * 60) + end_parsed.minute
            if end_total < start_total:
                raise ValueError("entry_time_end_local must be on or after entry_time_start_local")

        minute_values = _minute_of_day(minute_session)
        if start_parsed is not None:
            start_minutes = (start_parsed.hour * 60) + start_parsed.minute
            filtered = filtered[minute_values >= start_minutes].copy()
            minute_session = filtered["minute_utc"].dt.tz_convert(SESSION_TZ)
            minute_values = _minute_of_day(minute_session)
        if end_parsed is not None:
            end_minutes = (end_parsed.hour * 60) + end_parsed.minute
            filtered = filtered[minute_values <= end_minutes].copy()

    return filtered.reset_index(drop=True)


def _prepare_option_sparse_frame(
    symbol: str,
    defs: pd.DataFrame,
    active_expirations: pd.DataFrame,
    quotes: pd.DataFrame,
    trades: pd.DataFrame,
) -> pd.DataFrame:
    active_dates_by_exp = (
        active_expirations.groupby("expiration_date")["trade_date"]
        .apply(list)
        .to_dict()
    )
    active_expiration_dates = set(active_dates_by_exp.keys())

    meta = defs[
        (defs["instrument_class"].astype(str) == "C") & (defs["exp_date"].isin(active_expiration_dates))
    ][["raw_symbol", "exp_date", "strike_f"]].drop_duplicates("raw_symbol")
    meta = meta.rename(columns={"exp_date": "expiration_date", "strike_f": "strike"})
    if meta.empty:
        raise RuntimeError(f"No option metadata matched the active expirations for {symbol}.")

    merged = quotes.merge(trades, on=["raw_symbol", "minute"], how="outer")
    merged["volume"] = merged["volume"].fillna(0.0)
    merged = merged.merge(meta, on="raw_symbol", how="inner")
    merged = merged.dropna(subset=["minute", "strike", "expiration_date"])
    merged["symbol"] = symbol
    merged = merged.sort_values(["raw_symbol", "minute"]).reset_index(drop=True)
    return merged


def _regularize_active_option_minutes(
    sparse: pd.DataFrame,
    active_expirations: pd.DataFrame,
) -> pd.DataFrame:
    if sparse.empty:
        return pd.DataFrame()

    active_dates_by_exp = (
        active_expirations.groupby("expiration_date")["trade_date"]
        .apply(list)
        .to_dict()
    )

    frames: list[pd.DataFrame] = []
    keep_cols = ["bid", "ask", "mid", "volume"]
    for raw_symbol, raw_group in sparse.groupby("raw_symbol", sort=False):
        raw_group = raw_group.sort_values("minute").copy()
        expiration_date = raw_group["expiration_date"].iloc[0]
        strike = float(raw_group["strike"].iloc[0])
        symbol = str(raw_group["symbol"].iloc[0])

        for trade_date in active_dates_by_exp.get(expiration_date, []):
            minute_grid = _build_market_minutes(trade_date)
            if minute_grid.empty:
                continue

            day_start = minute_grid[0]
            day_end = minute_grid[-1]
            day_slice = raw_group[
                (raw_group["minute"] >= day_start) & (raw_group["minute"] <= day_end)
            ][["minute", *keep_cols]].copy()

            aligned = (
                day_slice.drop_duplicates("minute", keep="last")
                .set_index("minute")
                .reindex(minute_grid)
            )
            aligned.index.name = "minute"
            aligned = aligned.reset_index()
            aligned["bid"] = pd.to_numeric(aligned["bid"], errors="coerce").ffill()
            aligned["ask"] = pd.to_numeric(aligned["ask"], errors="coerce").ffill()
            aligned["mid"] = pd.to_numeric(aligned["mid"], errors="coerce").ffill()
            aligned["volume"] = pd.to_numeric(aligned["volume"], errors="coerce").fillna(0.0)
            aligned["raw_symbol"] = str(raw_symbol)
            aligned["symbol"] = symbol
            aligned["strike"] = strike
            aligned["expiration_date"] = expiration_date
            aligned["trade_date"] = trade_date
            frames.append(aligned)

    if not frames:
        return pd.DataFrame()

    minute_df = pd.concat(frames, ignore_index=True)
    return minute_df.sort_values(["symbol", "raw_symbol", "minute"]).reset_index(drop=True)


def _build_checkpoint_features(
    minute_df: pd.DataFrame,
    checkpoint_minutes: int,
    volume_lookback_bars: int,
) -> pd.DataFrame:
    if minute_df.empty:
        return pd.DataFrame()

    grouped = minute_df.groupby(["symbol", "raw_symbol", "trade_date"], sort=False)
    minute_df = minute_df.copy()
    minute_df["current_volume"] = grouped["volume"].transform(
        lambda s: s.rolling(checkpoint_minutes, min_periods=checkpoint_minutes).sum()
    )
    minute_df["price_10m_ago"] = grouped["mid"].shift(checkpoint_minutes)

    checkpoints = minute_df[_is_checkpoint_timestamp(minute_df["minute"].dt.tz_convert(SESSION_TZ), checkpoint_minutes)].copy()
    if checkpoints.empty:
        return checkpoints

    checkpoint_grouped = checkpoints.groupby(["symbol", "raw_symbol", "trade_date"], sort=False)
    checkpoints["avg_volume_last_6_bars"] = checkpoint_grouped["current_volume"].transform(
        lambda s: s.shift(1).rolling(volume_lookback_bars, min_periods=volume_lookback_bars).mean()
    )
    checkpoints["volume_z"] = np.where(
        checkpoints["avg_volume_last_6_bars"] > 0,
        checkpoints["current_volume"] / checkpoints["avg_volume_last_6_bars"],
        np.nan,
    )
    checkpoints["price_move"] = np.where(
        checkpoints["price_10m_ago"] > 0,
        (checkpoints["mid"] - checkpoints["price_10m_ago"]) / checkpoints["price_10m_ago"],
        np.nan,
    )
    return checkpoints.reset_index(drop=True)


def _attach_underlying_context(
    checkpoints: pd.DataFrame,
    intraday_underlying: pd.DataFrame,
    daily_underlying: pd.DataFrame,
) -> pd.DataFrame:
    if checkpoints.empty:
        return checkpoints

    merged = pd.merge_asof(
        checkpoints.sort_values("minute"),
        intraday_underlying.sort_values("minute"),
        on="minute",
        direction="backward",
    )
    merged = merged.merge(daily_underlying, on="trade_date", how="left")
    merged["underlying_from_open"] = np.where(
        merged["day_open"] > 0,
        (merged["underlying_price"] / merged["day_open"]) - 1.0,
        np.nan,
    )
    return merged


def _select_candidate_window(snapshot: pd.DataFrame, underlying_price: float, strike_span: int) -> pd.DataFrame:
    if snapshot.empty or not np.isfinite(underlying_price):
        return pd.DataFrame()

    valid = snapshot[
        (snapshot["bid"] > 0)
        & (snapshot["ask"] > 0)
        & (snapshot["mid"] > 0)
        & snapshot["volume_z"].notna()
        & snapshot["price_move"].notna()
    ].copy()
    if valid.empty:
        return pd.DataFrame()

    strikes = sorted(valid["strike"].dropna().astype(float).unique().tolist())
    if len(strikes) < (2 * strike_span) + 1:
        return pd.DataFrame()

    atm_idx = min(
        range(len(strikes)),
        key=lambda idx: (abs(strikes[idx] - float(underlying_price)), abs(strikes[idx])),
    )
    start_idx = atm_idx - strike_span
    end_idx = atm_idx + strike_span
    if start_idx < 0 or end_idx >= len(strikes):
        return pd.DataFrame()

    candidate_strikes = set(strikes[start_idx : end_idx + 1])
    candidates = valid[valid["strike"].astype(float).isin(candidate_strikes)].copy()
    if len(candidates) != (2 * strike_span) + 1:
        return pd.DataFrame()

    candidates = candidates.sort_values(["strike", "raw_symbol"]).reset_index(drop=True)
    candidates["rank_price_move"] = candidates["price_move"].rank(method="first", ascending=True)
    candidates["rank_volume"] = candidates["volume_z"].rank(method="first", ascending=True)
    candidates["score"] = candidates["rank_price_move"] + candidates["rank_volume"]
    candidates["atm_strike"] = float(strikes[atm_idx])
    candidates["underlying_price_snapshot"] = float(underlying_price)
    return candidates


def _select_best_contract(snapshot: pd.DataFrame, strike_span: int) -> pd.Series | None:
    if snapshot.empty:
        return None

    underlying_price = float(snapshot["underlying_price"].iloc[0]) if snapshot["underlying_price"].notna().any() else np.nan
    candidates = _select_candidate_window(snapshot, underlying_price=underlying_price, strike_span=strike_span)
    if candidates.empty:
        return None

    ordered = candidates.sort_values(
        ["score", "price_move", "volume_z", "strike"],
        ascending=[False, False, False, True],
    ).reset_index(drop=True)
    return ordered.iloc[0]


def _generate_entries(checkpoints: pd.DataFrame, config: dict) -> pd.DataFrame:
    if checkpoints.empty:
        return pd.DataFrame()

    filtered_checkpoints = _apply_entry_constraints(checkpoints, config)
    if filtered_checkpoints.empty:
        return pd.DataFrame()

    entries: list[dict[str, object]] = []
    trades_by_symbol_day: dict[tuple[str, date], int] = {}
    trades_by_day: dict[date, int] = {}
    max_trades_per_symbol_per_day = config.get("max_trades_per_symbol_per_day")
    max_trades_per_day = config.get("max_trades_per_day")
    max_trades_total = config.get("max_trades_total")

    sorted_checkpoints = filtered_checkpoints.sort_values(
        ["minute", "symbol", "strike", "raw_symbol"]
    ).reset_index(drop=True)
    for (minute, symbol), snapshot in sorted_checkpoints.groupby(["minute", "symbol"], sort=False):
        trade_date = snapshot["trade_date"].iloc[0]
        symbol_day_key = (str(symbol), trade_date)

        if max_trades_total is not None and len(entries) >= int(max_trades_total):
            break
        if max_trades_per_day is not None and trades_by_day.get(trade_date, 0) >= int(max_trades_per_day):
            continue
        if (
            max_trades_per_symbol_per_day is not None
            and trades_by_symbol_day.get(symbol_day_key, 0) >= int(max_trades_per_symbol_per_day)
        ):
            continue

        selected = _select_best_contract(snapshot, strike_span=int(config["strike_span"]))
        if selected is None:
            continue

        underlying_from_open = selected.get("underlying_from_open")
        volume_z = selected.get("volume_z")
        entry_price = selected.get(str(config["entry_price_field"]))
        if (
            pd.isna(underlying_from_open)
            or float(underlying_from_open) < float(config["underlying_day_move_min"])
            or pd.isna(volume_z)
            or float(volume_z) <= float(config["volume_z_min"])
            or pd.isna(entry_price)
            or float(entry_price) <= 0
        ):
            continue

        entry = selected.to_dict()
        entry["entry_price"] = float(entry_price)
        entries.append(entry)
        trades_by_day[trade_date] = trades_by_day.get(trade_date, 0) + 1
        trades_by_symbol_day[symbol_day_key] = trades_by_symbol_day.get(symbol_day_key, 0) + 1

    if not entries:
        return pd.DataFrame()
    return pd.DataFrame(entries).sort_values(["symbol", "minute"]).reset_index(drop=True)


def _simulate_exit_path(
    path: pd.DataFrame,
    entry_time: pd.Timestamp,
    entry_price: float,
    take_profit_pct: float,
    stop_loss_pct: float,
    price_field: str,
) -> dict[str, object] | None:
    if path.empty or not np.isfinite(entry_price) or entry_price <= 0:
        return None

    valid_from_entry = path[path["minute"] >= entry_time].copy()
    valid_from_entry = valid_from_entry[pd.to_numeric(valid_from_entry[price_field], errors="coerce") > 0].copy()
    if valid_from_entry.empty:
        return None

    take_profit_level = entry_price * (1.0 + take_profit_pct)
    stop_loss_level = entry_price * (1.0 - stop_loss_pct)
    for row in valid_from_entry[valid_from_entry["minute"] > entry_time].itertuples(index=False):
        exit_price = float(getattr(row, price_field))
        if exit_price >= take_profit_level:
            return {
                "exit_time": row.minute,
                "exit_price": exit_price,
                "exit_reason": "TAKE_PROFIT",
                "return_pct": ((exit_price / entry_price) - 1.0) * 100.0,
            }
        if exit_price <= stop_loss_level:
            return {
                "exit_time": row.minute,
                "exit_price": exit_price,
                "exit_reason": "STOP_LOSS",
                "return_pct": ((exit_price / entry_price) - 1.0) * 100.0,
            }

    eod_row = valid_from_entry.iloc[-1]
    exit_price = float(eod_row[price_field])
    return {
        "exit_time": eod_row["minute"],
        "exit_price": exit_price,
        "exit_reason": "EOD_EXIT",
        "return_pct": ((exit_price / entry_price) - 1.0) * 100.0,
    }


def _run_trade_simulation(entries: pd.DataFrame, minute_df: pd.DataFrame, config: dict) -> pd.DataFrame:
    if entries.empty or minute_df.empty:
        return pd.DataFrame()

    trades_out: list[dict[str, object]] = []
    by_key = {
        (str(symbol), str(raw_symbol), trade_date): group.sort_values("minute").reset_index(drop=True)
        for (symbol, raw_symbol, trade_date), group in minute_df.groupby(["symbol", "raw_symbol", "trade_date"], sort=False)
    }

    for row in entries.itertuples(index=False):
        path = by_key.get((str(row.symbol), str(row.raw_symbol), row.trade_date))
        if path is None or path.empty:
            continue

        exit_result = _simulate_exit_path(
            path=path,
            entry_time=pd.Timestamp(row.minute),
            entry_price=float(row.entry_price),
            take_profit_pct=float(config["take_profit_pct"]),
            stop_loss_pct=float(config["stop_loss_pct"]),
            price_field=str(config["exit_price_field"]),
        )
        if exit_result is None:
            continue

        trades_out.append(
            {
                "symbol": str(row.symbol),
                "date": row.trade_date,
                "entry_time": pd.Timestamp(row.minute),
                "exit_time": pd.Timestamp(exit_result["exit_time"]),
                "strike": float(row.strike),
                "expiration_date": row.expiration_date,
                "raw_symbol": str(row.raw_symbol),
                "entry_price": float(row.entry_price),
                "exit_price": float(exit_result["exit_price"]),
                "return_pct": float(exit_result["return_pct"]),
                "exit_reason": str(exit_result["exit_reason"]),
                "volume_z": float(row.volume_z),
                "price_move": float(row.price_move),
                "rank_price_move": float(row.rank_price_move),
                "rank_volume": float(row.rank_volume),
                "score": float(row.score),
                "underlying_price": float(row.underlying_price),
                "underlying_from_open": float(row.underlying_from_open),
            }
        )

    if not trades_out:
        return pd.DataFrame()

    trades = pd.DataFrame(trades_out).sort_values(["entry_time", "symbol"]).reset_index(drop=True)
    trades["equity_curve"] = trades["return_pct"].cumsum()
    trades["drawdown"] = trades["equity_curve"].cummax() - trades["equity_curve"]
    return trades


def _build_summary(trades: pd.DataFrame) -> pd.DataFrame:
    if trades.empty:
        return pd.DataFrame(
            [
                {
                    "total_trades": 0,
                    "winning_trades": 0,
                    "losing_trades": 0,
                    "win_rate": 0.0,
                    "avg_win": 0.0,
                    "avg_loss": 0.0,
                    "expected_value": 0.0,
                    "equity_curve_final": 0.0,
                    "max_drawdown": 0.0,
                    "profit_factor": 0.0,
                    "avg_return_per_trade": 0.0,
                    "positive_ev": False,
                }
            ]
        )

    winners = trades[trades["return_pct"] > 0]
    losers = trades[trades["return_pct"] < 0]
    total_trades = int(len(trades))
    win_rate = float(len(winners) / total_trades)
    avg_win = float(winners["return_pct"].mean()) if not winners.empty else 0.0
    avg_loss = float(losers["return_pct"].mean()) if not losers.empty else 0.0
    expected_value = (win_rate * avg_win) + ((1.0 - win_rate) * avg_loss)
    total_wins = float(winners["return_pct"].sum()) if not winners.empty else 0.0
    total_losses = float(-losers["return_pct"].sum()) if not losers.empty else 0.0
    if total_losses > 0:
        profit_factor = total_wins / total_losses
    elif total_wins > 0:
        profit_factor = float("inf")
    else:
        profit_factor = 0.0

    return pd.DataFrame(
        [
            {
                "total_trades": total_trades,
                "winning_trades": int(len(winners)),
                "losing_trades": int(len(losers)),
                "win_rate": win_rate,
                "avg_win": avg_win,
                "avg_loss": avg_loss,
                "expected_value": expected_value,
                "equity_curve_final": float(trades["equity_curve"].iloc[-1]) if "equity_curve" in trades.columns else float(trades["return_pct"].sum()),
                "max_drawdown": float(trades["drawdown"].max()),
                "profit_factor": float(profit_factor),
                "avg_return_per_trade": float(trades["return_pct"].mean()),
                "positive_ev": bool(expected_value > 0),
            }
        ]
    )


def run_symbol_backtest(hist: db.Historical, symbol: str, start_d: date, end_d: date, config: dict) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    request_floor_d = _request_floor(end_d)
    data_start_d = max(
        start_d - timedelta(days=10),
        request_floor_d,
    )

    intraday_underlying, daily_underlying = _load_underlying_context(symbol, data_start_d, end_d, config)
    trading_dates = daily_underlying[
        (daily_underlying["trade_date"] >= start_d) & (daily_underlying["trade_date"] <= end_d)
    ]["trade_date"].tolist()
    if not trading_dates:
        raise RuntimeError(f"No trading dates found for {symbol}.")

    defs = _load_option_definitions(hist, symbol, data_start_d, end_d)
    active_expirations = _build_active_expiration_map(defs, trading_dates, config)
    active_raws = defs[
        (defs["instrument_class"].astype(str) == "C")
        & (defs["exp_date"].isin(active_expirations["expiration_date"].unique()))
    ]["raw_symbol"].dropna().astype(str).unique().tolist()
    if not active_raws:
        raise RuntimeError(f"No active option raws found for {symbol}.")

    quotes = _load_quotes_for_raws(hist, active_raws, data_start_d, end_d)
    trades = _load_trades_for_raws(hist, active_raws, data_start_d, end_d)
    sparse = _prepare_option_sparse_frame(symbol, defs, active_expirations, quotes, trades)
    minute_df = _regularize_active_option_minutes(sparse, active_expirations)
    minute_df = minute_df[
        (minute_df["trade_date"] >= start_d) & (minute_df["trade_date"] <= end_d)
    ].copy()
    if minute_df.empty:
        raise RuntimeError(f"No minute-level option data was prepared for {symbol}.")

    checkpoints = _build_checkpoint_features(
        minute_df=minute_df,
        checkpoint_minutes=int(config["checkpoint_minutes"]),
        volume_lookback_bars=int(config["volume_lookback_bars"]),
    )
    checkpoints = _attach_underlying_context(
        checkpoints=checkpoints,
        intraday_underlying=intraday_underlying,
        daily_underlying=daily_underlying,
    )
    checkpoints = checkpoints[checkpoints["trade_date"].between(start_d, end_d)].copy()
    entries = _generate_entries(checkpoints, config)
    trades_df = _run_trade_simulation(entries, minute_df, config)
    return checkpoints, entries, trades_df


def run_backtest(config: dict) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    if not DATABENTO_API_KEY:
        raise RuntimeError("DATABENTO_API_KEY is not set.")

    start_d, end_d = _normalize_backtest_dates(config["start"], config["end"])
    hist = db.Historical(DATABENTO_API_KEY)

    checkpoint_frames: list[pd.DataFrame] = []
    entry_frames: list[pd.DataFrame] = []
    trade_frames: list[pd.DataFrame] = []
    for symbol in [str(value).upper() for value in config["symbols"]]:
        checkpoints, entries, trades_df = run_symbol_backtest(hist, symbol, start_d, end_d, config)
        checkpoint_frames.append(checkpoints)
        if not entries.empty:
            entry_frames.append(entries)
        if not trades_df.empty:
            trade_frames.append(trades_df)

    checkpoints_df = pd.concat(checkpoint_frames, ignore_index=True) if checkpoint_frames else pd.DataFrame()
    entries_df = pd.concat(entry_frames, ignore_index=True) if entry_frames else pd.DataFrame()
    trades_df = pd.concat(trade_frames, ignore_index=True) if trade_frames else pd.DataFrame()
    trades_df = trades_df.sort_values(["entry_time", "symbol"]).reset_index(drop=True) if not trades_df.empty else trades_df
    if not trades_df.empty:
        trades_df["equity_curve"] = trades_df["return_pct"].cumsum()
        trades_df["drawdown"] = trades_df["equity_curve"].cummax() - trades_df["equity_curve"]

    summary_df = _build_summary(trades_df)
    return checkpoints_df, entries_df, trades_df, summary_df


def main() -> None:
    parser = argparse.ArgumentParser(description="ATM +/-2 call ranking backtest with 10-minute checkpoints.")
    parser.add_argument("--show-entries", action="store_true", help="Print selected entries.")
    args = parser.parse_args()

    print(
        f"Running ATM call ranking backtest for {','.join(CONFIG['symbols'])} "
        f"from {CONFIG['start']} to {CONFIG['end']}"
    )
    checkpoints_df, entries_df, trades_df, summary_df = run_backtest(CONFIG)

    print("\n=== SUMMARY ===")
    print(summary_df.to_string(index=False))

    print("\n=== TRADE LOG ===")
    if trades_df.empty:
        print("No trades generated.")
    else:
        print(
            trades_df[
                ["symbol", "date", "entry_time", "exit_time", "strike", "entry_price", "exit_price", "return_pct"]
            ].to_string(index=False)
        )

    if args.show_entries:
        print("\n=== ENTRIES ===")
        if entries_df.empty:
            print("No qualifying entries.")
        else:
            cols = [
                "symbol",
                "trade_date",
                "minute",
                "strike",
                "entry_price",
                "volume_z",
                "price_move",
                "rank_price_move",
                "rank_volume",
                "score",
                "underlying_from_open",
            ]
            print(entries_df[cols].to_string(index=False))

    if checkpoints_df.empty:
        print("\nNo checkpoint data was produced.")


if __name__ == "__main__":
    main()
