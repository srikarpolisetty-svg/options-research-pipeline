from __future__ import annotations

import argparse
import math
import re
from datetime import date, datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import databento as db
import numpy as np
import pandas as pd
import yfinance as yf

from backtest_data_utils import (
    MAX_REQUEST_LOOKBACK_DAYS,
    _load_option_definitions,
    _load_quotes_for_raws,
    _load_trades_for_raws,
    _request_floor,
)
from config import DATABENTO_API_KEY
from policy.expiration import is_third_friday
from policy.risk import ENTRY_QTY, TRAIL_PCT
from policy.strikes import build_strike_map

MARKET_TZ = ZoneInfo("UTC")
BE_TRIGGER_PCT = 25.0
TP1_TRIGGER_PCT = 50.0

LEG_SPECS = {
    "ATMC": {"strike_label": "ATM", "side": "C", "bucket": "ATM"},
    #"ATMP": {"strike_label": "ATM", "side": "P", "bucket": "ATM"},
    #"OTM1C": {"strike_label": "C1", "side": "C", "bucket": "OTM_1"},
    #"OTM1P": {"strike_label": "P1", "side": "P", "bucket": "OTM_1"},
   # "OTM2C": {"strike_label": "C2", "side": "C", "bucket": "OTM_2"},
   # "OTM2P": {"strike_label": "P2", "side": "P", "bucket": "OTM_2"},
}

CONFIG = {
    "symbol": "SPY",
    "start": "2025-03-20",
    "end": "2026-03-20",
    "legs": ["ATMC"],
    "friday_only": True,
    "exclude_third_friday": True,
    "entry_times_local": [],
    "entry_weekdays": None,
    "one_trade_per_leg_per_day": True,
    "one_position_at_a_time": True,
    "max_trades_per_day": 1,
    "compute_true_iv": False,
    "indicator_windows": {
        "z_short_days": 10,
        "z_long_days": 70,
        "rsi_period": 14,
        "ma_fast_days": 20,
        "ma_slow_days": 50,
    },
    "exit": {
        "mode": "risk",
        "entry_qty": ENTRY_QTY,
        "trail_pct": TRAIL_PCT,
        "be_trigger_pct": BE_TRIGGER_PCT,
        "tp1_trigger_pct": TP1_TRIGGER_PCT,
        "horizon_minutes": None,
    },
    "filters": {
         "side": {"in": ["C"]},
        # "bucket": {"in": ["ATM"]},

        # "iv": {"min": 0.02, "max": 0.50},
        # "spread": {"max": 0.25},
        # "spread_pct": {"max": 0.15},
        # "z_bid_3d": {"min": 1.5},
        # "z_ask_3d": {"min": 1.5},
         #"z_mid_3d": {"min": 1.5},
         #"z_volume_3d": {"min": 1.5},
        # "z_iv_3d": {"min": 1.5},
         #"z_volume_35d": {"min": 1.5},
          #  "z_mid_long": {"min": 1.5},
          #"z_volume_long": {"min": 1.5},
           #"z_iv_long": {"min": 1.5},
         #"underlying_rsi": {"min": 55},
         "underlying_above_ma_fast": {"eq": True},
         "underlying_above_ma_slow": {"eq": True},
    },
}


def _rsi(series: pd.Series, period: int) -> pd.Series:
    delta = series.diff()
    gain = delta.clip(lower=0.0)
    loss = -delta.clip(upper=0.0)
    avg_gain = gain.ewm(alpha=1.0 / period, adjust=False, min_periods=period).mean()
    avg_loss = loss.ewm(alpha=1.0 / period, adjust=False, min_periods=period).mean()
    rs = avg_gain / avg_loss.replace(0.0, np.nan)
    return 100.0 - (100.0 / (1.0 + rs))


def _norm_cdf(x: float) -> float:
    return 0.5 * (1.0 + math.erf(x / math.sqrt(2.0)))


def _bs_price(s: float, k: float, t: float, sigma: float, cp: str, r: float = 0.01) -> float | None:
    if s <= 0 or k <= 0 or t <= 0 or sigma <= 0:
        return None
    d1 = (math.log(s / k) + (r + 0.5 * sigma * sigma) * t) / (sigma * math.sqrt(t))
    d2 = d1 - sigma * math.sqrt(t)
    if cp == "C":
        return s * _norm_cdf(d1) - k * math.exp(-r * t) * _norm_cdf(d2)
    return k * math.exp(-r * t) * _norm_cdf(-d2) - s * _norm_cdf(-d1)


def _implied_vol(mid: float, s: float, k: float, t_years: float, cp: str) -> float | None:
    if mid is None or s is None or k is None or t_years is None:
        return None
    if mid <= 0 or s <= 0 or k <= 0 or t_years <= 0:
        return None

    lo, hi = 1e-4, 5.0
    for _ in range(70):
        vol = 0.5 * (lo + hi)
        px = _bs_price(s, k, t_years, vol, cp)
        if px is None:
            return None
        if px > mid:
            hi = vol
        else:
            lo = vol
    return 0.5 * (lo + hi)


def _rolling_z(series: pd.Series, window: int, min_periods: int = 30) -> pd.Series:
    mu = series.rolling(window=window, min_periods=min_periods).mean().shift(1)
    sd = series.rolling(window=window, min_periods=min_periods).std(ddof=0).shift(1)
    return (series - mu) / sd.replace(0, np.nan)


def _simulate_trade_for_horizon(
    by_raw: dict[str, pd.DataFrame],
    raw: str,
    entry_minute: pd.Timestamp,
    entry_px: float,
    horizon_minutes: int | None,
    entry_qty: int,
    trail_pct: float,
    be_trigger_pct: float,
    tp1_trigger_pct: float,
) -> tuple[float | None, float | None, pd.Timestamp | None, str | None]:
    g = by_raw.get(raw)
    if g is None or g.empty:
        return None, None, None, None

    qty_open = int(entry_qty)
    if qty_open <= 0:
        return None, None, None, None

    initial_cost = float(entry_px) * 100.0 * float(qty_open)
    if not np.isfinite(initial_cost) or initial_cost <= 0:
        return None, None, None, None

    target_time = None if horizon_minutes is None else (entry_minute + pd.Timedelta(minutes=horizon_minutes))
    i0 = g["minute"].searchsorted(entry_minute, side="left")
    if i0 >= len(g):
        return None, None, None, None

    realized = 0.0
    peak = float(entry_px)
    be_armed = False
    tp1_done = False
    final_exit_time: pd.Timestamp | None = None
    final_reason: str | None = None

    for i in range(int(i0), len(g)):
        row = g.iloc[int(i)]
        ts = row["minute"]
        if target_time is not None and ts > target_time:
            break

        px = float(row["bid"]) if pd.notna(row["bid"]) else float(row["mid"])
        if not np.isfinite(px) or px <= 0:
            continue

        peak = max(peak, px)
        ret_pct = ((px - entry_px) / entry_px) * 100.0

        if (not be_armed) and ret_pct >= be_trigger_pct:
            be_armed = True

        if (not tp1_done) and qty_open >= 2 and ret_pct >= tp1_trigger_pct:
            realized += (px - entry_px) * 100.0
            qty_open -= 1
            tp1_done = True
            if qty_open <= 0:
                final_exit_time = ts
                final_reason = "TP1_FULL"
                break

        trail_stop = peak * (1.0 - trail_pct)
        stop_level = max(trail_stop, entry_px) if be_armed else trail_stop
        if qty_open > 0 and px <= stop_level:
            realized += (px - entry_px) * 100.0 * float(qty_open)
            qty_open = 0
            final_exit_time = ts
            if be_armed and entry_px >= trail_stop:
                final_reason = "BE_STOP"
            else:
                final_reason = "TRAIL_STOP"
            break

    if qty_open > 0:
        if target_time is None:
            r = g.iloc[-1]
            final_reason = "DATA_END_EXIT"
        else:
            i_exit = g["minute"].searchsorted(target_time, side="left")
            if i_exit >= len(g):
                r = g.iloc[-1]
                final_reason = "DATA_END_EXIT"
            else:
                r = g.iloc[int(i_exit)]
                final_reason = "TIME_EXIT"
        exit_px = float(r["bid"]) if pd.notna(r["bid"]) else float(r["mid"])
        if not np.isfinite(exit_px) or exit_px <= 0:
            return None, None, None, None
        realized += (exit_px - entry_px) * 100.0 * float(qty_open)
        final_exit_time = r["minute"]

    ret_pct_final = 100.0 * float(realized) / float(initial_cost)
    return ret_pct_final, float(realized), final_exit_time, final_reason


def _simulate_time_exit(
    by_raw: dict[str, pd.DataFrame],
    raw: str,
    entry_minute: pd.Timestamp,
    entry_px: float,
    horizon_minutes: int,
    entry_qty: int,
) -> tuple[float | None, float | None, pd.Timestamp | None, str | None]:
    g = by_raw.get(raw)
    if g is None or g.empty or horizon_minutes is None or horizon_minutes <= 0:
        return None, None, None, None

    target_time = entry_minute + pd.Timedelta(minutes=horizon_minutes)
    i_exit = g["minute"].searchsorted(target_time, side="left")
    if i_exit >= len(g):
        row = g.iloc[-1]
        reason = "DATA_END_EXIT"
    else:
        row = g.iloc[int(i_exit)]
        reason = "TIME_EXIT"

    exit_px = float(row["bid"]) if pd.notna(row["bid"]) else float(row["mid"])
    if not np.isfinite(exit_px) or exit_px <= 0:
        return None, None, None, None

    pnl_dollars = (exit_px - entry_px) * 100.0 * float(entry_qty)
    cost = entry_px * 100.0 * float(entry_qty)
    ret_pct = 100.0 * pnl_dollars / cost if cost > 0 else None
    return ret_pct, pnl_dollars, row["minute"], reason


def _max_drawdown_dollars(trades_df: pd.DataFrame) -> float:
    if trades_df is None or trades_df.empty or "pnl_dollars" not in trades_df.columns:
        return 0.0

    order_col = "exit_time" if "exit_time" in trades_df.columns else "entry_time"
    curve = (
        trades_df[["pnl_dollars", order_col]]
        .dropna(subset=["pnl_dollars"])
        .sort_values(order_col)
        ["pnl_dollars"]
        .cumsum()
    )
    if curve.empty:
        return 0.0

    drawdown = curve.cummax() - curve
    return float(drawdown.max())


def _compute_trade_statistics(trades_df: pd.DataFrame) -> dict[str, float | int]:
    default = {
        "trades": 0,
        "wins": 0,
        "losses": 0,
        "win_rate_pct": 0.0,
        "loss_rate_pct": 0.0,
        "avg_return_pct": 0.0,
        "median_return_pct": 0.0,
        "total_pnl_dollars": 0.0,
        "total_profit_dollars": 0.0,
        "avg_pnl_dollars": 0.0,
        "gross_cost_dollars": 0.0,
        "roi_pct_on_cost": 0.0,
        "expected_value_dollars": 0.0,
        "avg_win_dollars": 0.0,
        "avg_loss_dollars_abs": 0.0,
        "profit_factor": 0.0,
        "win_loss_ratio": 0.0,
        "pnl_variance": 0.0,
        "pnl_std_dev": 0.0,
        "sharpe_ratio": 0.0,
        "max_drawdown_dollars": 0.0,
        "conditional_ev_dollars": 0.0,
    }
    if trades_df is None or trades_df.empty:
        return default

    tmp = trades_df[trades_df["ret_pct"].notna() & trades_df["pnl_dollars"].notna()].copy()
    if tmp.empty:
        return default

    winners = tmp[tmp["pnl_dollars"] > 0]
    losers = tmp[tmp["pnl_dollars"] <= 0]
    wins = int(len(winners))
    losses = int(len(losers))
    gross_cost = float((tmp["entry_ask"] * 100.0 * tmp["entry_qty"]).sum())
    total_pnl = float(tmp["pnl_dollars"].sum())
    total_wins = float(winners["pnl_dollars"].sum()) if not winners.empty else 0.0
    total_losses_abs = float((-losers["pnl_dollars"]).sum()) if not losers.empty else 0.0
    avg_win = float(winners["pnl_dollars"].mean()) if not winners.empty else 0.0
    avg_loss_abs = float((-losers["pnl_dollars"]).mean()) if not losers.empty else 0.0
    variance = float(tmp["pnl_dollars"].var(ddof=0)) if len(tmp) > 0 else 0.0
    std_dev = float(tmp["pnl_dollars"].std(ddof=0)) if len(tmp) > 0 else 0.0
    expected_value = float(tmp["pnl_dollars"].mean())

    if total_losses_abs > 0:
        profit_factor = total_wins / total_losses_abs
    elif total_wins > 0:
        profit_factor = float("inf")
    else:
        profit_factor = 0.0

    if avg_loss_abs > 0:
        win_loss_ratio = avg_win / avg_loss_abs
    elif avg_win > 0:
        win_loss_ratio = float("inf")
    else:
        win_loss_ratio = 0.0

    sharpe_ratio = expected_value / std_dev if std_dev > 0 else 0.0

    return {
        "trades": int(len(tmp)),
        "wins": wins,
        "losses": losses,
        "win_rate_pct": 100.0 * wins / len(tmp),
        "loss_rate_pct": 100.0 * losses / len(tmp),
        "avg_return_pct": float(tmp["ret_pct"].mean()),
        "median_return_pct": float(tmp["ret_pct"].median()),
        "total_pnl_dollars": total_pnl,
        "total_profit_dollars": total_pnl,
        "avg_pnl_dollars": float(tmp["pnl_dollars"].mean()),
        "gross_cost_dollars": gross_cost,
        "roi_pct_on_cost": (100.0 * total_pnl / gross_cost) if gross_cost > 0 else 0.0,
        "expected_value_dollars": expected_value,
        "avg_win_dollars": avg_win,
        "avg_loss_dollars_abs": avg_loss_abs,
        "profit_factor": profit_factor,
        "win_loss_ratio": win_loss_ratio,
        "pnl_variance": variance,
        "pnl_std_dev": std_dev,
        "sharpe_ratio": sharpe_ratio,
        "max_drawdown_dollars": _max_drawdown_dollars(tmp),
        "conditional_ev_dollars": expected_value,
    }


def _build_condition_summary(trades_df: pd.DataFrame) -> pd.DataFrame:
    if trades_df is None or trades_df.empty:
        return pd.DataFrame()

    rows: list[dict[str, float | int | str]] = []
    grouped = trades_df.groupby(["leg_key", "bucket", "side"], sort=True)
    for (leg_key, bucket, side), group in grouped:
        stats = _compute_trade_statistics(group)
        rows.append(
            {
                "leg_key": leg_key,
                "bucket": bucket,
                "side": side,
                "trades": int(stats["trades"]),
                "wins": int(stats["wins"]),
                "losses": int(stats["losses"]),
                "win_rate_pct": float(stats["win_rate_pct"]),
                "loss_rate_pct": float(stats["loss_rate_pct"]),
                "conditional_ev_dollars": float(stats["conditional_ev_dollars"]),
                "total_profit_dollars": float(stats["total_profit_dollars"]),
                "profit_factor": float(stats["profit_factor"]),
                "avg_win_dollars": float(stats["avg_win_dollars"]),
                "avg_loss_dollars_abs": float(stats["avg_loss_dollars_abs"]),
                "win_loss_ratio": float(stats["win_loss_ratio"]),
                "pnl_variance": float(stats["pnl_variance"]),
                "pnl_std_dev": float(stats["pnl_std_dev"]),
                "sharpe_ratio": float(stats["sharpe_ratio"]),
                "max_drawdown_dollars": float(stats["max_drawdown_dollars"]),
                "avg_return_pct": float(stats["avg_return_pct"]),
            }
        )
    return pd.DataFrame(rows)


def _slice_segment_frame(
    df: pd.DataFrame,
    raws: list[str],
    seg_start: date,
    seg_end: date,
) -> pd.DataFrame:
    start_ts = pd.Timestamp(seg_start, tz="UTC")
    end_ts_exclusive = pd.Timestamp(seg_end + timedelta(days=1), tz="UTC")
    return df[
        df["raw_symbol"].isin(raws)
        & (df["minute"] >= start_ts)
        & (df["minute"] < end_ts_exclusive)
    ].copy()


def _reference_spot(under_daily: pd.DataFrame, trade_date: date) -> tuple[float, date]:
    same_day = under_daily[under_daily["date"] == trade_date]
    if not same_day.empty:
        row = same_day.iloc[0]
        if pd.notna(row.get("open")):
            return float(row["open"]), row["date"]
        if pd.notna(row.get("close")):
            return float(row["close"]), row["date"]

    nxt = under_daily[under_daily["date"] > trade_date]
    if not nxt.empty:
        row = nxt.iloc[0]
        if pd.notna(row.get("open")):
            return float(row["open"]), row["date"]
        if pd.notna(row.get("close")):
            return float(row["close"]), row["date"]

    prev = under_daily[under_daily["date"] < trade_date]
    if not prev.empty:
        row = prev.iloc[-1]
        if pd.notna(row.get("open")):
            return float(row["open"]), row["date"]
        if pd.notna(row.get("close")):
            return float(row["close"]), row["date"]

    raise RuntimeError(f"No underlying open available near {trade_date}")


def _build_roll_contracts(
    defs: pd.DataFrame,
    under_daily: pd.DataFrame,
    start_d: date,
    end_d: date,
    config: dict,
) -> pd.DataFrame:
    legs = config["legs"]
    friday_only = bool(config.get("friday_only", True))
    exclude_third_friday = bool(config.get("exclude_third_friday", True))

    expirations = sorted(defs["exp_date"].dropna().unique().tolist())
    filtered_expirations: list[date] = []
    for exp_date in expirations:
        if exp_date < start_d:
            continue
        if friday_only and exp_date.weekday() != 4:
            continue
        if exclude_third_friday and is_third_friday(exp_date):
            continue
        filtered_expirations.append(exp_date)

    if not filtered_expirations:
        raise RuntimeError("No expirations matched the current roll filters.")

    rows: list[dict] = []
    trade_dates = (
        under_daily[(under_daily["date"] >= start_d) & (under_daily["date"] <= end_d)]["date"]
        .drop_duplicates()
        .tolist()
    )
    for trade_date in trade_dates:
        exp_candidates = [d for d in filtered_expirations if d >= trade_date]
        if not exp_candidates:
            continue

        exp_date = exp_candidates[0]
        segment_start = trade_date
        segment_end = trade_date
        spot_ref, spot_ref_date = _reference_spot(under_daily, trade_date)

        sub = defs[defs["exp_date"] == exp_date].copy()
        strikes = sorted(sub["strike_f"].dropna().astype(float).unique().tolist())
        if not strikes:
            continue

        strike_map = build_strike_map(spot_ref, strikes)
        for leg_key in legs:
            spec = LEG_SPECS[leg_key]
            target_strike = float(strike_map[spec["strike_label"]])
            match = sub[
                (sub["instrument_class"] == spec["side"])
                & (sub["strike_f"].astype(float) == target_strike)
            ]
            if match.empty:
                continue

            rows.append(
                {
                    "segment_start": segment_start,
                    "segment_end": segment_end,
                    "expiration_date": exp_date,
                    "spot_ref": spot_ref,
                    "spot_ref_date": spot_ref_date,
                    "leg_key": leg_key,
                    "bucket": spec["bucket"],
                    "side": spec["side"],
                    "strike_label": spec["strike_label"],
                    "strike": target_strike,
                    "raw_symbol": str(match["raw_symbol"].iloc[0]),
                }
            )

    contracts = pd.DataFrame(rows)
    if contracts.empty:
        raise RuntimeError("No rolled contracts could be built from the selected legs.")
    return contracts
def _load_underlying_context(
    symbol: str,
    start_d: date,
    end_d: date,
    ma_fast_days: int,
    ma_slow_days: int,
    rsi_period: int,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    request_floor_d = _request_floor(end_d)
    hourly = yf.Ticker(symbol).history(
        start=max(start_d - timedelta(days=30), request_floor_d).isoformat(),
        end=(end_d + timedelta(days=1)).isoformat(),
        interval="60m",
        auto_adjust=False,
    )
    if hourly is None or hourly.empty or "Close" not in hourly.columns:
        raise RuntimeError(f"No hourly underlying data from yfinance for {symbol}.")

    daily = yf.Ticker(symbol).history(
        start=max(start_d - timedelta(days=max(ma_slow_days + 30, rsi_period + 30)), request_floor_d).isoformat(),
        end=(end_d + timedelta(days=1)).isoformat(),
        interval="1d",
        auto_adjust=False,
    )
    if daily is None or daily.empty or "Close" not in daily.columns:
        raise RuntimeError(f"No daily underlying data from yfinance for {symbol}.")

    hourly = hourly.copy()
    if hourly.index.tz is None:
        hourly.index = hourly.index.tz_localize("UTC")
    else:
        hourly.index = hourly.index.tz_convert("UTC")
    hourly["minute"] = hourly.index.floor("1min")
    hourly_df = (
        hourly.groupby("minute", as_index=False)["Close"]
        .last()
        .rename(columns={"Close": "underlying_price"})
        .sort_values("minute")
        .reset_index(drop=True)
    )
    hourly_df["minute_ns"] = hourly_df["minute"].astype("int64")

    daily = daily.copy()
    if daily.index.tz is None:
        daily.index = daily.index.tz_localize("UTC")
    else:
        daily.index = daily.index.tz_convert("UTC")
    daily["date"] = daily.index.date
    daily_df = (
        daily.groupby("date", as_index=False)["Close"]
        .last()
        .rename(columns={"Close": "daily_close"})
        .sort_values("date")
        .reset_index(drop=True)
    )
    daily_df["underlying_ma_fast"] = daily_df["daily_close"].rolling(ma_fast_days, min_periods=ma_fast_days).mean()
    daily_df["underlying_ma_slow"] = daily_df["daily_close"].rolling(ma_slow_days, min_periods=ma_slow_days).mean()
    daily_df["underlying_rsi"] = _rsi(daily_df["daily_close"], rsi_period)
    daily_df["prev_close"] = daily_df["daily_close"].shift(1)
    daily_df["underlying_ma_fast"] = daily_df["underlying_ma_fast"].shift(1)
    daily_df["underlying_ma_slow"] = daily_df["underlying_ma_slow"].shift(1)
    daily_df["underlying_rsi"] = daily_df["underlying_rsi"].shift(1)
    return hourly_df, daily_df


def _resolve_filter_column(df: pd.DataFrame, column: str, config: dict) -> str | None:
    if column in df.columns:
        return column

    match = re.match(r"^z_(.+)_(\d+)d$", column)
    if match:
        feature = match.group(1)
        days = int(match.group(2))
        windows = config.get("indicator_windows", {})
        short_days = int(windows.get("z_short_days", 0) or 0)
        long_days = int(windows.get("z_long_days", 0) or 0)
        if days == short_days and f"z_{feature}_short" in df.columns:
            return f"z_{feature}_short"
        if days == long_days and f"z_{feature}_long" in df.columns:
            return f"z_{feature}_long"
        if days == 3 and f"z_{feature}_short" in df.columns:
            return f"z_{feature}_short"
        if days == 35 and f"z_{feature}_long" in df.columns:
            return f"z_{feature}_long"

    return None


def _normalize_filter_keys(filters: dict[str, dict], config: dict) -> dict[str, dict]:
    normalized: dict[str, dict] = {}
    windows = config.get("indicator_windows", {})
    short_days = int(windows.get("z_short_days", 0) or 0)
    long_days = int(windows.get("z_long_days", 0) or 0)

    for column, rules in filters.items():
        match = re.match(r"^z_(.+)_(\d+)d$", column)
        if match:
            feature = match.group(1)
            days = int(match.group(2))
            if days == short_days:
                normalized[f"z_{feature}_short"] = rules
                continue
            if days == long_days:
                normalized[f"z_{feature}_long"] = rules
                continue
            if days == 3:
                normalized[f"z_{feature}_short"] = rules
                continue
            if days == 35:
                normalized[f"z_{feature}_long"] = rules
                continue
        normalized[column] = rules

    return normalized


def _apply_filters(df: pd.DataFrame, filters: dict[str, dict], config: dict) -> pd.Series:
    mask = pd.Series(True, index=df.index)
    for column, rules in _normalize_filter_keys(filters, config).items():
        resolved_column = _resolve_filter_column(df, column, config)
        if resolved_column is None:
            raise KeyError(f"Unknown filter column: {column}")

        series = df[resolved_column]
        if "min" in rules:
            mask &= pd.to_numeric(series, errors="coerce") >= rules["min"]
        if "max" in rules:
            mask &= pd.to_numeric(series, errors="coerce") <= rules["max"]
        if "eq" in rules:
            mask &= series == rules["eq"]
        if "ne" in rules:
            mask &= series != rules["ne"]
        if "in" in rules:
            mask &= series.isin(rules["in"])
        if "not_in" in rules:
            mask &= ~series.isin(rules["not_in"])
        if "not_null" in rules:
            mask &= series.notna() if bool(rules["not_null"]) else series.isna()
    return mask


def _enrich_segment_frame(
    seg_quotes: pd.DataFrame,
    seg_trades: pd.DataFrame,
    meta_by_raw: dict[str, dict],
    under_hourly: pd.DataFrame,
    under_daily_features: pd.DataFrame,
    compute_true_iv: bool,
) -> pd.DataFrame:
    if seg_quotes is None or seg_quotes.empty:
        return pd.DataFrame()

    df = seg_quotes.merge(seg_trades, on=["raw_symbol", "minute"], how="left")
    df["volume"] = df["volume"].fillna(0.0)
    df = df.sort_values(["minute", "raw_symbol"]).reset_index(drop=True)
    df["minute_ns"] = df["minute"].astype("int64")
    df = pd.merge_asof(
        df,
        under_hourly.sort_values("minute_ns"),
        on="minute_ns",
        direction="backward",
    )
    df = df.drop(columns=["minute_ns"])
    if "minute_x" in df.columns:
        df = df.rename(columns={"minute_x": "minute"})
    if "minute_y" in df.columns:
        df = df.drop(columns=["minute_y"])

    df["date"] = df["minute"].dt.date
    df = df.merge(under_daily_features, on="date", how="left")
    df["underlying_price"] = df["underlying_price"].ffill().bfill()

    df["leg_key"] = df["raw_symbol"].map(lambda x: meta_by_raw[x]["leg_key"])
    df["bucket"] = df["raw_symbol"].map(lambda x: meta_by_raw[x]["bucket"])
    df["side"] = df["raw_symbol"].map(lambda x: meta_by_raw[x]["side"])
    df["strike"] = df["raw_symbol"].map(lambda x: meta_by_raw[x]["strike"])
    df["expiration_date"] = df["raw_symbol"].map(lambda x: meta_by_raw[x]["expiration_date"])
    df["segment_start"] = df["raw_symbol"].map(lambda x: meta_by_raw[x]["segment_start"])
    df["segment_end"] = df["raw_symbol"].map(lambda x: meta_by_raw[x]["segment_end"])
    df["days_to_expiry"] = (
        (pd.to_datetime(df["expiration_date"], utc=True) - df["minute"]).dt.total_seconds() / 86400.0
    ).clip(lower=1 / 390.0)

    if compute_true_iv:
        df["iv"] = df.apply(
            lambda row: _implied_vol(
                mid=float(row["mid"]) if pd.notna(row["mid"]) else None,
                s=float(row["underlying_price"]) if pd.notna(row["underlying_price"]) else None,
                k=float(row["strike"]) if pd.notna(row["strike"]) else None,
                t_years=float(row["days_to_expiry"]) / 365.0 if pd.notna(row["days_to_expiry"]) else None,
                cp=str(row["side"]),
            ),
            axis=1,
        )
    else:
        df["iv"] = (df["mid"] / df["underlying_price"]).replace([np.inf, -np.inf], np.nan)

    df["spread"] = df["ask"] - df["bid"]
    df["spread_pct"] = np.where(df["mid"] > 0, df["spread"] / df["mid"], np.nan)
    df["underlying_above_ma_fast"] = df["underlying_price"] > df["underlying_ma_fast"]
    df["underlying_above_ma_slow"] = df["underlying_price"] > df["underlying_ma_slow"]
    df["underlying_pct_from_ma_fast"] = np.where(
        df["underlying_ma_fast"] > 0,
        ((df["underlying_price"] / df["underlying_ma_fast"]) - 1.0) * 100.0,
        np.nan,
    )
    df["underlying_pct_from_ma_slow"] = np.where(
        df["underlying_ma_slow"] > 0,
        ((df["underlying_price"] / df["underlying_ma_slow"]) - 1.0) * 100.0,
        np.nan,
    )
    df["minute_local"] = df["minute"].dt.tz_convert(MARKET_TZ)
    df["entry_date"] = df["minute_local"].dt.date
    df["time_local"] = df["minute_local"].dt.strftime("%H:%M")
    df["weekday_local"] = df["minute_local"].dt.day_name()
    return df


def _compute_feature_columns(df: pd.DataFrame, short_days: int, long_days: int) -> pd.DataFrame:
    short_bars = short_days * 390
    long_bars = long_days * 390
    features = ["bid", "ask", "mid", "volume", "iv", "spread", "spread_pct"]
    for feature in features:
        df[f"z_{feature}_{short_days}d"] = df.groupby("leg_key")[feature].transform(
            lambda s: _rolling_z(s, short_bars)
        )
        df[f"z_{feature}_{long_days}d"] = df.groupby("leg_key")[feature].transform(
            lambda s: _rolling_z(s, long_bars)
        )
        df[f"z_{feature}_short"] = df[f"z_{feature}_{short_days}d"]
        df[f"z_{feature}_long"] = df[f"z_{feature}_{long_days}d"]
    return df


def _time_window_mask(minute_local: pd.Series, entry_times: list[str], tolerance_minutes: int = 5) -> pd.Series:
    mask = pd.Series(False, index=minute_local.index)
    minute_of_day = (minute_local.dt.hour * 60) + minute_local.dt.minute
    for entry_time in entry_times:
        if not entry_time:
            continue
        parts = entry_time.split(":")
        if len(parts) != 2:
            raise ValueError(f"Invalid entry time: {entry_time}")
        target_minutes = (int(parts[0]) * 60) + int(parts[1])
        mask |= (minute_of_day - target_minutes).abs() <= tolerance_minutes
    return mask


def _build_signals(df: pd.DataFrame, config: dict) -> pd.DataFrame:
    signals = df.copy()
    entry_times = config.get("entry_times_local")
    if entry_times:
        signals = signals[_time_window_mask(signals["minute_local"], entry_times)].copy()

    entry_weekdays = config.get("entry_weekdays")
    if entry_weekdays:
        signals = signals[signals["weekday_local"].isin(entry_weekdays)].copy()

    if signals.empty:
        return signals

    filter_mask = _apply_filters(signals, config.get("filters", {}), config)
    signals = signals[filter_mask].copy()
    signals = signals.sort_values(["minute", "leg_key"]).reset_index(drop=True)

    if config.get("one_trade_per_leg_per_day", True):
        signals = signals.drop_duplicates(["entry_date", "leg_key"], keep="first").reset_index(drop=True)

    max_trades_per_day = config.get("max_trades_per_day")
    if max_trades_per_day is not None:
        signals["trade_rank_day"] = signals.groupby("entry_date").cumcount()
        signals = signals[signals["trade_rank_day"] < int(max_trades_per_day)].copy()
        signals = signals.drop(columns=["trade_rank_day"])

    return signals


def _run_trade_simulation(df: pd.DataFrame, signals: pd.DataFrame, config: dict) -> pd.DataFrame:
    if signals.empty:
        return pd.DataFrame()

    by_raw = {
        raw: g.sort_values("minute").reset_index(drop=True)
        for raw, g in df.groupby("raw_symbol")
    }

    exit_cfg = config["exit"]
    trades_out: list[dict] = []
    last_exit_time: pd.Timestamp | None = None

    for row in signals.itertuples(index=False):
        entry_time = pd.Timestamp(row.minute)
        if config.get("one_position_at_a_time") and last_exit_time is not None and entry_time <= last_exit_time:
            continue

        entry_px = float(row.ask) if pd.notna(row.ask) else float(row.mid)
        if not np.isfinite(entry_px) or entry_px <= 0:
            continue

        if exit_cfg["mode"] == "time":
            ret_pct, pnl_dollars, exit_time, exit_reason = _simulate_time_exit(
                by_raw=by_raw,
                raw=str(row.raw_symbol),
                entry_minute=entry_time,
                entry_px=entry_px,
                horizon_minutes=int(exit_cfg["horizon_minutes"]),
                entry_qty=int(exit_cfg["entry_qty"]),
            )
        else:
            ret_pct, pnl_dollars, exit_time, exit_reason = _simulate_trade_for_horizon(
                by_raw=by_raw,
                raw=str(row.raw_symbol),
                entry_minute=entry_time,
                entry_px=entry_px,
                horizon_minutes=exit_cfg["horizon_minutes"],
                entry_qty=int(exit_cfg["entry_qty"]),
                trail_pct=float(exit_cfg["trail_pct"]),
                be_trigger_pct=float(exit_cfg["be_trigger_pct"]),
                tp1_trigger_pct=float(exit_cfg["tp1_trigger_pct"]),
            )

        trades_out.append(
            {
                "entry_time": entry_time,
                "entry_date": row.entry_date,
                "time_local": row.time_local,
                "symbol": config["symbol"],
                "raw_symbol": row.raw_symbol,
                "leg_key": row.leg_key,
                "bucket": row.bucket,
                "side": row.side,
                "strike": float(row.strike),
                "expiration_date": row.expiration_date,
                "entry_ask": entry_px,
                "entry_qty": int(exit_cfg["entry_qty"]),
                "bid": float(row.bid),
                "ask": float(row.ask),
                "mid": float(row.mid),
                "volume": float(row.volume),
                "iv": float(row.iv) if pd.notna(row.iv) else np.nan,
                "spread": float(row.spread) if pd.notna(row.spread) else np.nan,
                "spread_pct": float(row.spread_pct) if pd.notna(row.spread_pct) else np.nan,
                "underlying_price": float(row.underlying_price) if pd.notna(row.underlying_price) else np.nan,
                "underlying_rsi": float(row.underlying_rsi) if pd.notna(row.underlying_rsi) else np.nan,
                "underlying_ma_fast": float(row.underlying_ma_fast) if pd.notna(row.underlying_ma_fast) else np.nan,
                "underlying_ma_slow": float(row.underlying_ma_slow) if pd.notna(row.underlying_ma_slow) else np.nan,
                "underlying_above_ma_fast": bool(row.underlying_above_ma_fast) if pd.notna(row.underlying_above_ma_fast) else False,
                "underlying_above_ma_slow": bool(row.underlying_above_ma_slow) if pd.notna(row.underlying_above_ma_slow) else False,
                "z_bid_short": float(row.z_bid_short) if pd.notna(row.z_bid_short) else np.nan,
                "z_bid_long": float(row.z_bid_long) if pd.notna(row.z_bid_long) else np.nan,
                "z_ask_short": float(row.z_ask_short) if pd.notna(row.z_ask_short) else np.nan,
                "z_ask_long": float(row.z_ask_long) if pd.notna(row.z_ask_long) else np.nan,
                "z_mid_short": float(row.z_mid_short) if pd.notna(row.z_mid_short) else np.nan,
                "z_mid_long": float(row.z_mid_long) if pd.notna(row.z_mid_long) else np.nan,
                "z_volume_short": float(row.z_volume_short) if pd.notna(row.z_volume_short) else np.nan,
                "z_volume_long": float(row.z_volume_long) if pd.notna(row.z_volume_long) else np.nan,
                "z_iv_short": float(row.z_iv_short) if pd.notna(row.z_iv_short) else np.nan,
                "z_iv_long": float(row.z_iv_long) if pd.notna(row.z_iv_long) else np.nan,
                "ret_pct": float(ret_pct) if ret_pct is not None else np.nan,
                "pnl_dollars": float(pnl_dollars) if pnl_dollars is not None else np.nan,
                "exit_time": exit_time,
                "exit_reason": exit_reason,
            }
        )

        if exit_time is not None:
            last_exit_time = pd.Timestamp(exit_time)

    return pd.DataFrame(trades_out)


def _build_summary(trades_df: pd.DataFrame, signals_df: pd.DataFrame, config: dict) -> pd.DataFrame:
    stats = _compute_trade_statistics(trades_df)
    return pd.DataFrame(
        [
            {
                "symbol": config["symbol"],
                "start": config["start"],
                "end": config["end"],
                "legs": ",".join(config["legs"]),
                "entry_times_local": ",".join(config["entry_times_local"]) if config.get("entry_times_local") else "ALL",
                "filter_count": len(config.get("filters", {})),
                "signals": int(len(signals_df)),
                "trades": int(stats["trades"]),
                "wins": int(stats["wins"]),
                "losses": int(stats["losses"]),
                "win_rate_pct": float(stats["win_rate_pct"]),
                "loss_rate_pct": float(stats["loss_rate_pct"]),
                "avg_return_pct": float(stats["avg_return_pct"]),
                "median_return_pct": float(stats["median_return_pct"]),
                "total_pnl_dollars": float(stats["total_pnl_dollars"]),
                "total_profit_dollars": float(stats["total_profit_dollars"]),
                "avg_pnl_dollars": float(stats["avg_pnl_dollars"]),
                "expected_value_dollars": float(stats["expected_value_dollars"]),
                "profit_factor": float(stats["profit_factor"]),
                "avg_win_dollars": float(stats["avg_win_dollars"]),
                "avg_loss_dollars_abs": float(stats["avg_loss_dollars_abs"]),
                "win_loss_ratio": float(stats["win_loss_ratio"]),
                "pnl_variance": float(stats["pnl_variance"]),
                "pnl_std_dev": float(stats["pnl_std_dev"]),
                "sharpe_ratio": float(stats["sharpe_ratio"]),
                "max_drawdown_dollars": float(stats["max_drawdown_dollars"]),
                "gross_cost_dollars": float(stats["gross_cost_dollars"]),
                "roi_pct_on_cost": float(stats["roi_pct_on_cost"]),
                "exit_mode": config["exit"]["mode"],
                "entry_qty": int(config["exit"]["entry_qty"]),
            }
        ]
    )


def run_backtest(config: dict) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    symbol = str(config["symbol"]).upper()
    start_d = date.fromisoformat(config["start"])
    end_d = date.fromisoformat(config["end"])
    today_utc = datetime.now(timezone.utc).date()
    if end_d >= today_utc:
        end_d = today_utc - timedelta(days=1)
    request_floor_d = _request_floor(end_d)
    if start_d < request_floor_d:
        print(
            f"requested start {start_d.isoformat()} exceeds the {MAX_REQUEST_LOOKBACK_DAYS}-day cap; "
            f"using {request_floor_d.isoformat()} instead"
        )
        start_d = request_floor_d
    if start_d >= end_d:
        raise RuntimeError("start must be before end")

    windows = config["indicator_windows"]
    prebuffer_days = max(
        60,
        int(windows["z_long_days"]) + 10,
        int(windows["ma_slow_days"]) + 10,
        int(windows["rsi_period"]) + 10,
    )
    data_start_d = max(start_d - timedelta(days=prebuffer_days), request_floor_d)

    hist = db.Historical(DATABENTO_API_KEY)
    under_daily_ref = yf.Ticker(symbol).history(
        start=max(data_start_d - timedelta(days=10), request_floor_d).isoformat(),
        end=(end_d + timedelta(days=1)).isoformat(),
        interval="1d",
        auto_adjust=False,
    )
    if under_daily_ref is None or under_daily_ref.empty or "Close" not in under_daily_ref.columns:
        raise RuntimeError(f"No daily underlying data from yfinance for {symbol}.")
    under_daily_ref = under_daily_ref.copy()
    if under_daily_ref.index.tz is None:
        under_daily_ref.index = under_daily_ref.index.tz_localize("UTC")
    else:
        under_daily_ref.index = under_daily_ref.index.tz_convert("UTC")
    under_daily_ref["date"] = under_daily_ref.index.date
    under_daily_ref = (
        under_daily_ref.groupby("date", as_index=False)
        .agg(open=("Open", "first"), close=("Close", "last"))
        .sort_values("date")
        .reset_index(drop=True)
    )

    defs = _load_option_definitions(hist, symbol, data_start_d, end_d)
    contracts = _build_roll_contracts(defs, under_daily_ref, data_start_d, end_d, config)
    print(f"rolled contracts selected: {len(contracts)}")

    under_hourly, under_daily_features = _load_underlying_context(
        symbol=symbol,
        start_d=data_start_d,
        end_d=end_d,
        ma_fast_days=int(windows["ma_fast_days"]),
        ma_slow_days=int(windows["ma_slow_days"]),
        rsi_period=int(windows["rsi_period"]),
    )

    all_raws = sorted(contracts["raw_symbol"].unique().tolist())
    all_quotes = _load_quotes_for_raws(hist, all_raws, data_start_d, end_d)
    all_trades = _load_trades_for_raws(hist, all_raws, data_start_d, end_d)

    frames: list[pd.DataFrame] = []
    grouped = contracts.groupby(["segment_start", "segment_end", "expiration_date"], as_index=False)
    for _, seg_group in grouped:
        seg_group = seg_group.copy()
        seg_start = seg_group["segment_start"].iloc[0]
        seg_end = seg_group["segment_end"].iloc[0]
        raws = sorted(seg_group["raw_symbol"].unique().tolist())
        meta_by_raw = {
            str(row.raw_symbol): {
                "leg_key": str(row.leg_key),
                "bucket": str(row.bucket),
                "side": str(row.side),
                "strike": float(row.strike),
                "expiration_date": row.expiration_date,
                "segment_start": row.segment_start,
                "segment_end": row.segment_end,
            }
            for row in seg_group.itertuples(index=False)
        }

        seg_quotes = _slice_segment_frame(all_quotes, raws, seg_start, seg_end)
        seg_trades = _slice_segment_frame(all_trades, raws, seg_start, seg_end)
        seg_df = _enrich_segment_frame(
            seg_quotes=seg_quotes,
            seg_trades=seg_trades,
            meta_by_raw=meta_by_raw,
            under_hourly=under_hourly,
            under_daily_features=under_daily_features,
            compute_true_iv=bool(config.get("compute_true_iv", False)),
        )
        if not seg_df.empty:
            frames.append(seg_df)

    if not frames:
        raise RuntimeError("No minute-level option data was loaded for the selected config.")

    df = pd.concat(frames, ignore_index=True)
    df = df.sort_values(["leg_key", "minute", "raw_symbol"]).reset_index(drop=True)
    df = _compute_feature_columns(
        df,
        short_days=int(windows["z_short_days"]),
        long_days=int(windows["z_long_days"]),
    )

    df = df[(df["entry_date"] >= start_d) & (df["entry_date"] <= end_d)].copy()
    signals = _build_signals(df, config)
    trades = _run_trade_simulation(df, signals, config)
    summary = _build_summary(trades, signals, config)
    return df, signals, trades, summary


def main() -> None:
    p = argparse.ArgumentParser(description="Config-driven options backtester with generic filters and indicators.")
    p.add_argument("--show-columns", action="store_true", help="Print all filterable columns after data is built.")
    args = p.parse_args()

    print(
        f"Running configurable backtest for {CONFIG['symbol']} from {CONFIG['start']} to {CONFIG['end']}"
    )
    feature_df, signals_df, trades_df, summary_df = run_backtest(CONFIG)

    print("\n=== SUMMARY ===")
    print(summary_df.to_string(index=False))

    print("\n=== SIGNALS (head) ===")
    if signals_df.empty:
        print("No signals matched the current filters.")
    else:
        print(signals_df.head(20).to_string(index=False))
        print(f"\nTotal signals: {len(signals_df)}")

    print("\n=== TRADES (head) ===")
    if trades_df.empty:
        print("No trades generated.")
    else:
        print(trades_df.head(20).to_string(index=False))
        print(f"\nTotal trades: {len(trades_df)}")
        print("\n=== CONDITIONAL EV BY LEG ===")
        by_leg = _build_condition_summary(trades_df)
        print(by_leg.to_string(index=False))

    if args.show_columns:
        print("\n=== FILTERABLE COLUMNS ===")
        for column in sorted(feature_df.columns):
            print(column)


if __name__ == "__main__":
    main()
