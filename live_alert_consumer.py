"""Consume live Kafka market records, serve an in-process dashboard, and emit alerts."""

from __future__ import annotations

import datetime as dt
import json
import math
import threading
import uuid
from typing import Any

import pandas as pd
import yfinance as yf
from confluent_kafka import Consumer, Producer

from historical_baselines import load_historical_baselines
from live_dashboard_server import build_dashboard_payload_from_frames, load_raw_universe, start_dashboard_server
from message import send_text


KAFKA_TOPIC = "market-records"
SIGNAL_TOPIC = "signal-events"
ALERT_Z_THRESHOLD = 1.5
TEN_MINUTES = dt.timedelta(minutes=10)
THIRTY_MINUTES = dt.timedelta(minutes=30)
ONE_HOUR = dt.timedelta(hours=1)
UNDERLYING_REFRESH = dt.timedelta(minutes=20)
ALERT_COOLDOWN = dt.timedelta(minutes=12)
EVENT_LOG_INTERVAL = 5_000


def debug(message: str) -> None:
    print(f"[LIVE CONSUMER] {message}", flush=True)


def debug_num(value: Any, digits: int = 2) -> str:
    try:
        if value is None:
            return "NA"
        return f"{float(value):.{digits}f}"
    except Exception:
        return "NA"


consumer = Consumer({
    "bootstrap.servers": "localhost:9092",
    "group.id": "calc-group",
    "auto.offset.reset": "earliest",
})
consumer.subscribe([KAFKA_TOPIC])
debug(f"kafka subscribed topic={KAFKA_TOPIC} group=calc-group")
signal_producer = Producer({
    "bootstrap.servers": "localhost:9092",
})
debug(f"signal producer ready topic={SIGNAL_TOPIC}")

state_lock = threading.RLock()

symbol_mapper: dict[str, list[dict[str, Any]]] = {}
underlying_price_cache: dict[str, dict[str, Any]] = {}
raw_symbols_rolling_volume_addition: dict[str, list[tuple[dt.datetime, int]]] = {}
rolling_volume_10m: dict[str, int] = {}
rolling_volume_30m: dict[str, int] = {}
rolling_volume_1h: dict[str, int] = {}
live_contract_state: dict[tuple[str, str, str, str, str], dict[str, Any]] = {}
alert_history_rows: list[dict[str, Any]] = []
last_alert_sent_at: dict[str, dt.datetime] = {}
missing_baseline_keys: set[tuple[str, str, str, str]] = set()
invalid_baseline_stats: set[tuple[str, str, str, str, str]] = set()
debug("loading baselines")
baselines = load_historical_baselines()
debug(f"baselines loaded keys={len(baselines)}")

runtime_status = {
    "component": "consumer",
    "started_at": dt.datetime.now(dt.timezone.utc),
    "last_heartbeat_ts": None,
    "last_event_ts": None,
    "last_quote_ts": None,
    "last_volume_ts": None,
    "events_total": 0,
    "quote_events_total": 0,
    "volume_events_total": 0,
    "alerts_total": 0,
    "tracked_contract_rows": 0,
    "tracked_parent_symbols": 0,
    "missing_baseline_count": 0,
    "invalid_baseline_count": 0,
}


def parse_event_timestamp(timestamp: str) -> dt.datetime:
    parsed = dt.datetime.fromisoformat(timestamp)
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=dt.timezone.utc)
    return parsed.astimezone(dt.timezone.utc)


def parse_strike_from_raw_symbol(raw_symbol: str) -> float:
    return int(str(raw_symbol)[-8:]) / 1000.0


def parse_expiration_date_from_raw_symbol(raw_symbol: str) -> dt.date:
    return dt.datetime.strptime(str(raw_symbol)[-15:-9], "%y%m%d").date()


def days_to_expiry_from_raw_symbol(raw_symbol: str, now: dt.datetime) -> int:
    expiration_date = parse_expiration_date_from_raw_symbol(raw_symbol)
    return (expiration_date - now.date()).days


def get_underlying_price(parent_symbol: str, now: dt.datetime) -> float | None:
    cached = underlying_price_cache.get(parent_symbol)
    if cached is not None and now - cached["timestamp"] < UNDERLYING_REFRESH:
        return cached["price"]

    hist = yf.Ticker(parent_symbol).history(period="1d", interval="1m")
    close_series = hist["Close"].dropna() if "Close" in hist.columns else None
    if close_series is None or close_series.empty:
        return None

    price = float(close_series.iloc[-1])
    underlying_price_cache[parent_symbol] = {
        "price": price,
        "timestamp": now,
    }
    return price


def bs_iv_bisect(
    mid: float | None,
    underlying_price: float | None,
    strike: float,
    days_to_expiry: int,
    call_put: str,
) -> float | None:
    if mid is None or mid <= 0 or underlying_price is None or underlying_price <= 0:
        return None

    time_to_expiry = float(days_to_expiry) / 365.0
    if time_to_expiry <= 0:
        return None

    r = 0.01
    lo, hi = 1e-6, 5.0

    def normal_cdf(x: float) -> float:
        return 0.5 * (1.0 + math.erf(x / math.sqrt(2.0)))

    def bs_price(sigma: float) -> float:
        d1 = (
            math.log(underlying_price / strike)
            + (r + 0.5 * sigma * sigma) * time_to_expiry
        ) / (sigma * math.sqrt(time_to_expiry))
        d2 = d1 - sigma * math.sqrt(time_to_expiry)
        if call_put == "C":
            return (
                underlying_price * normal_cdf(d1)
                - strike * math.exp(-r * time_to_expiry) * normal_cdf(d2)
            )
        return (
            strike * math.exp(-r * time_to_expiry) * normal_cdf(-d2)
            - underlying_price * normal_cdf(-d1)
        )

    for _ in range(60):
        sigma = 0.5 * (lo + hi)
        if bs_price(sigma) > mid:
            hi = sigma
        else:
            lo = sigma

    return 0.5 * (lo + hi)


def get_baseline(parent_symbol: str, side: str, grouping: str, decay_bucket: str) -> dict[str, Any] | None:
    key = (parent_symbol, side, grouping, decay_bucket)
    baseline = baselines.get(key)
    if baseline is None:
        with state_lock:
            if key not in missing_baseline_keys:
                print(f"[WARN] missing baseline for {key}")
                missing_baseline_keys.add(key)
                runtime_status["missing_baseline_count"] = len(missing_baseline_keys)
    return baseline


def safe_zscore(
    value,
    mean_value,
    std_value,
    *,
    stat_name: str,
    baseline_key: tuple[str, str, str, str],
) -> float | None:
    if value is None or mean_value is None or std_value is None:
        bad_key = (*baseline_key, stat_name)
        with state_lock:
            if bad_key not in invalid_baseline_stats:
                print(f"[WARN] unusable baseline stats for {bad_key}")
                invalid_baseline_stats.add(bad_key)
                runtime_status["invalid_baseline_count"] = len(invalid_baseline_stats)
        return None

    try:
        value_f = float(value)
        mean_f = float(mean_value)
        std_f = float(std_value)
    except Exception:
        bad_key = (*baseline_key, stat_name)
        with state_lock:
            if bad_key not in invalid_baseline_stats:
                print(f"[WARN] non-numeric baseline stats for {bad_key}")
                invalid_baseline_stats.add(bad_key)
                runtime_status["invalid_baseline_count"] = len(invalid_baseline_stats)
        return None

    if not math.isfinite(value_f) or not math.isfinite(mean_f) or not math.isfinite(std_f) or std_f <= 0:
        bad_key = (*baseline_key, stat_name)
        with state_lock:
            if bad_key not in invalid_baseline_stats:
                print(f"[WARN] invalid baseline std/value for {bad_key}")
                invalid_baseline_stats.add(bad_key)
                runtime_status["invalid_baseline_count"] = len(invalid_baseline_stats)
        return None

    return (value_f - mean_f) / std_f


def contract_state_key(raw_symbol: str, metadata: dict[str, Any]) -> tuple[str, str, str, str, str]:
    return (
        str(metadata["parent_symbol"]),
        str(raw_symbol),
        str(metadata["side"]),
        str(metadata["grouping"]),
        str(metadata["decay_bucket"]),
    )


def get_or_create_contract_state(
    raw_symbol: str,
    metadata: dict[str, Any],
    now: dt.datetime,
) -> tuple[tuple[str, str, str, str, str], dict[str, Any]]:
    key = contract_state_key(raw_symbol, metadata)
    row = live_contract_state.get(key)
    if row is None:
        row = {
            "parent_symbol": metadata["parent_symbol"],
            "raw_option_symbol": raw_symbol,
            "side": metadata["side"],
            "grouping": metadata["grouping"],
            "decay_bucket": metadata["decay_bucket"],
            "days_to_expiry": days_to_expiry_from_raw_symbol(raw_symbol, now),
            "strike": parse_strike_from_raw_symbol(raw_symbol),
            "expiration_date": parse_expiration_date_from_raw_symbol(raw_symbol),
            "bid": None,
            "ask": None,
            "mid": None,
            "spread": None,
            "spread_pct": None,
            "rolling_volume_10m": 0,
            "rolling_volume_30m": 0,
            "rolling_volume_1h": 0,
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
            "last_quote_ts": None,
            "last_trade_ts": None,
            "last_volume_update_ts": None,
            "updated_at": None,
        }
        live_contract_state[key] = row

    row["days_to_expiry"] = days_to_expiry_from_raw_symbol(raw_symbol, now)
    row["strike"] = parse_strike_from_raw_symbol(raw_symbol)
    row["expiration_date"] = parse_expiration_date_from_raw_symbol(raw_symbol)
    return key, row


def mark_contract_state_updated(row: dict[str, Any], now: dt.datetime) -> None:
    row["updated_at"] = now


def queue_alert(
    *,
    alert_type: str,
    raw_symbol: str,
    parent_symbol: str,
    side: str,
    grouping: str,
    decay_bucket: str,
    metric_value: float | None,
    z_35d: float | None,
    z_3d: float | None,
    threshold: float,
    alert_message: str,
    now: dt.datetime,
    signal_context: dict[str, Any] | None = None,
) -> None:
    signal_id = str(uuid.uuid4())
    send_text(alert_message)
    signal_event = {
        "type": "signal_event",
        "signal_id": signal_id,
        "alert_timestamp": now.isoformat(),
        "alert_type": alert_type,
        "raw_option_symbol": raw_symbol,
        "parent_symbol": parent_symbol,
        "side": side,
        "grouping": grouping,
        "decay_bucket": decay_bucket,
        "metric_value": metric_value,
        "z_35d": z_35d,
        "z_3d": z_3d,
        "threshold": threshold,
        "alert_message": alert_message,
    }
    if signal_context:
        signal_event.update(signal_context)

    try:
        signal_producer.produce(
            SIGNAL_TOPIC,
            json.dumps(signal_event).encode("utf-8"),
        )
        signal_producer.poll(0)
    except Exception as exc:
        print(f"[WARN] failed to publish signal event for {raw_symbol}: {exc}")

    alert_row = {
        "signal_id": signal_id,
        "alert_timestamp": now,
        "alert_type": alert_type,
        "raw_option_symbol": raw_symbol,
        "parent_symbol": parent_symbol,
        "side": side,
        "grouping": grouping,
        "decay_bucket": decay_bucket,
        "metric_value": metric_value,
        "z_35d": z_35d,
        "z_3d": z_3d,
        "threshold": threshold,
        "alert_message": alert_message,
    }
    if signal_context:
        alert_row.update(signal_context)

    with state_lock:
        alert_history_rows.append(alert_row)
        runtime_status["alerts_total"] += 1
        alert_count = runtime_status["alerts_total"]
    debug(
        f"alert sent total={alert_count} parent={parent_symbol} raw={raw_symbol} "
        f"type={alert_type} z35={debug_num(z_35d)} z3={debug_num(z_3d)}"
    )


def should_send_combined_alert(
    raw_symbol: str,
    now: dt.datetime,
    row: dict[str, Any],
) -> bool:
    last_sent = last_alert_sent_at.get(raw_symbol)
    if last_sent is not None and now - last_sent < ALERT_COOLDOWN:
        return False

    z_vol_35d = row.get("z_vol_35d")
    z_vol_3d = row.get("z_vol_3d")
    z_mid_35d = row.get("z_mid_35d")
    z_mid_3d = row.get("z_mid_3d")
    z_iv_35d = row.get("z_iv_35d")
    z_iv_3d = row.get("z_iv_3d")

    return (
        z_vol_35d is not None
        and z_vol_3d is not None
        and z_mid_35d is not None
        and z_mid_3d is not None
        and z_iv_35d is not None
        and z_iv_3d is not None
        and z_vol_35d >= ALERT_Z_THRESHOLD
        and z_vol_3d >= ALERT_Z_THRESHOLD
        and z_mid_35d >= ALERT_Z_THRESHOLD
        and z_mid_3d >= ALERT_Z_THRESHOLD
        and z_iv_35d >= ALERT_Z_THRESHOLD
        and z_iv_3d >= ALERT_Z_THRESHOLD
    )


def maybe_queue_combined_alert(
    *,
    raw_symbol: str,
    metadata: dict[str, Any],
    row: dict[str, Any],
    now: dt.datetime,
) -> None:
    if not should_send_combined_alert(raw_symbol, now, row):
        return

    queue_alert(
        alert_type="combined",
        raw_symbol=raw_symbol,
        parent_symbol=metadata["parent_symbol"],
        side=metadata["side"],
        grouping=metadata["grouping"],
        decay_bucket=metadata["decay_bucket"],
        metric_value=row.get("mid"),
        z_35d=min(
            row["z_vol_35d"],
            row["z_mid_35d"],
            row["z_iv_35d"],
        ),
        z_3d=min(
            row["z_vol_3d"],
            row["z_mid_3d"],
            row["z_iv_3d"],
        ),
        threshold=ALERT_Z_THRESHOLD,
        alert_message=(
            f"Combined alert for {raw_symbol}: parent={metadata['parent_symbol']} "
            f"underlying={row.get('underlying_price')} strike={row.get('strike')} "
            f"side={metadata['side']} grouping={metadata['grouping']} "
            f"vol(z35={row['z_vol_35d']:.2f}, z3={row['z_vol_3d']:.2f}) "
            f"mid(z35={row['z_mid_35d']:.2f}, z3={row['z_mid_3d']:.2f}) "
            f"iv(z35={row['z_iv_35d']:.2f}, z3={row['z_iv_3d']:.2f})"
        ),
        now=now,
        signal_context={
            "strike": row.get("strike"),
            "option_bid": row.get("bid"),
            "option_ask": row.get("ask"),
            "option_mid": row.get("mid"),
            "option_spread_pct": row.get("spread_pct"),
            "underlying_price": row.get("underlying_price"),
            "current_iv": row.get("current_iv"),
            "rolling_volume_10m": row.get("rolling_volume_10m"),
            "rolling_volume_30m": row.get("rolling_volume_30m"),
            "rolling_volume_1h": row.get("rolling_volume_1h"),
            "z_vol_35d": row.get("z_vol_35d"),
            "z_vol_3d": row.get("z_vol_3d"),
            "z_mid_35d": row.get("z_mid_35d"),
            "z_mid_3d": row.get("z_mid_3d"),
            "z_iv_35d": row.get("z_iv_35d"),
            "z_iv_3d": row.get("z_iv_3d"),
            "option_quote_timestamp": (
                row["last_quote_ts"].isoformat()
                if row.get("last_quote_ts") is not None
                else None
            ),
        },
    )
    last_alert_sent_at[raw_symbol] = now


def refresh_runtime_status(now: dt.datetime) -> None:
    with state_lock:
        runtime_status["last_heartbeat_ts"] = now
        runtime_status["tracked_contract_rows"] = len(live_contract_state)
        runtime_status["tracked_parent_symbols"] = len({
            row["parent_symbol"] for row in live_contract_state.values()
        })


def snapshot_dashboard_state() -> tuple[pd.DataFrame, pd.DataFrame, dict[str, Any]]:
    with state_lock:
        state_rows = [row.copy() for row in live_contract_state.values()]
        alerts_rows = [row.copy() for row in alert_history_rows]
        runtime_snapshot = dict(runtime_status)

    state_df = pd.DataFrame(state_rows)
    alerts_df = pd.DataFrame(alerts_rows)
    return state_df, alerts_df, runtime_snapshot


def build_dashboard_payload(
    *,
    parent_symbol: str | None,
    side: str | None,
    grouping: str | None,
    decay_bucket: str | None,
    raw_search: str | None,
    alert_limit: int,
    alert_scope: str | None,
) -> dict:
    raw_df = load_raw_universe()
    state_df, alerts_df, runtime_snapshot = snapshot_dashboard_state()
    return build_dashboard_payload_from_frames(
        raw_df=raw_df,
        state_df=state_df,
        runtime_status=runtime_snapshot,
        alerts_df=alerts_df,
        parent_symbol=parent_symbol,
        side=side,
        grouping=grouping,
        decay_bucket=decay_bucket,
        raw_search=raw_search,
        alert_limit=alert_limit,
        alert_scope=alert_scope,
    )


def update_rolling_volume(raw_symbol: str, now: dt.datetime) -> None:
    entries = raw_symbols_rolling_volume_addition.setdefault(raw_symbol, [])
    old_rolling_volume_10m = rolling_volume_10m.get(raw_symbol, 0)
    old_rolling_volume_30m = rolling_volume_30m.get(raw_symbol, 0)
    old_rolling_volume_1h = rolling_volume_1h.get(raw_symbol, 0)
    raw_symbols_rolling_volume_addition[raw_symbol] = [
        (entry_ts, entry_vol)
        for entry_ts, entry_vol in entries
        if now - entry_ts <= ONE_HOUR
    ]

    buffered_entries = raw_symbols_rolling_volume_addition[raw_symbol]
    current_volume_10m = sum(
        entry_vol
        for entry_ts, entry_vol in buffered_entries
        if now - entry_ts <= TEN_MINUTES
    )
    current_volume_30m = sum(
        entry_vol
        for entry_ts, entry_vol in buffered_entries
        if now - entry_ts <= THIRTY_MINUTES
    )
    current_volume_1h = sum(entry_vol for _, entry_vol in buffered_entries)
    rolling_volume_10m[raw_symbol] = current_volume_10m
    rolling_volume_30m[raw_symbol] = current_volume_30m
    rolling_volume_1h[raw_symbol] = current_volume_1h

    if (
        current_volume_10m == old_rolling_volume_10m
        and current_volume_30m == old_rolling_volume_30m
        and current_volume_1h == old_rolling_volume_1h
    ) or raw_symbol not in symbol_mapper:
        return

    for metadata in symbol_mapper[raw_symbol]:
        baseline_key = (
            metadata["parent_symbol"],
            metadata["side"],
            metadata["grouping"],
            metadata["decay_bucket"],
        )
        average = get_baseline(*baseline_key)

        with state_lock:
            _key, row = get_or_create_contract_state(raw_symbol, metadata, now)
            row["rolling_volume_10m"] = current_volume_10m
            row["rolling_volume_30m"] = current_volume_30m
            row["rolling_volume_1h"] = current_volume_1h
            row["last_trade_ts"] = now
            row["last_volume_update_ts"] = now

            if average is not None:
                row["mean_vol_35d"] = average.get("mean_vol_35d")
                row["std_vol_35d"] = average.get("std_vol_35d")
                row["mean_vol_3d"] = average.get("mean_vol_3d")
                row["std_vol_3d"] = average.get("std_vol_3d")

                z_vol_35d = safe_zscore(
                    current_volume_10m,
                    average.get("mean_vol_35d"),
                    average.get("std_vol_35d"),
                    stat_name="vol_35d",
                    baseline_key=baseline_key,
                )
                z_vol_3d = safe_zscore(
                    current_volume_10m,
                    average.get("mean_vol_3d"),
                    average.get("std_vol_3d"),
                    stat_name="vol_3d",
                    baseline_key=baseline_key,
                )
                row["z_vol_35d"] = z_vol_35d
                row["z_vol_3d"] = z_vol_3d

                maybe_queue_combined_alert(
                    raw_symbol=raw_symbol,
                    metadata=metadata,
                    row=row,
                    now=now,
                )

            mark_contract_state_updated(row, now)


def process_quote_event(event: dict[str, Any]) -> None:
    raw_symbol = event["raw_symbol"]
    if raw_symbol not in symbol_mapper:
        return

    bid = event["bid"]
    ask = event["ask"]
    mid = event["mid"]
    spread = event["spread"]
    spread_pct = event["spread_pct"]
    now = parse_event_timestamp(event["timestamp"])

    metadata_rows = symbol_mapper[raw_symbol]
    strike = parse_strike_from_raw_symbol(raw_symbol)
    days_to_expiry = days_to_expiry_from_raw_symbol(raw_symbol, now)
    parent_symbol = metadata_rows[0]["parent_symbol"]
    underlying_price = get_underlying_price(parent_symbol, now)

    with state_lock:
        runtime_status["last_quote_ts"] = now

    for metadata in metadata_rows:
        baseline_key = (
            metadata["parent_symbol"],
            metadata["side"],
            metadata["grouping"],
            metadata["decay_bucket"],
        )
        average = get_baseline(*baseline_key)
        current_iv = bs_iv_bisect(mid, underlying_price, strike, days_to_expiry, metadata["side"])

        with state_lock:
            _key, row = get_or_create_contract_state(raw_symbol, metadata, now)
            row["bid"] = bid
            row["ask"] = ask
            row["mid"] = mid
            row["spread"] = spread
            row["spread_pct"] = spread_pct
            row["underlying_price"] = underlying_price
            row["last_quote_ts"] = now
            row["current_iv"] = current_iv

            if average is not None:
                row["mean_mid_35d"] = average.get("mean_mid_35d")
                row["std_mid_35d"] = average.get("std_mid_35d")
                row["mean_mid_3d"] = average.get("mean_mid_3d")
                row["std_mid_3d"] = average.get("std_mid_3d")
                row["mean_iv_35d"] = average.get("mean_iv_35d")
                row["std_iv_35d"] = average.get("std_iv_35d")
                row["mean_iv_3d"] = average.get("mean_iv_3d")
                row["std_iv_3d"] = average.get("std_iv_3d")

                z_mid_35d = safe_zscore(
                    mid,
                    average.get("mean_mid_35d"),
                    average.get("std_mid_35d"),
                    stat_name="mid_35d",
                    baseline_key=baseline_key,
                )
                z_mid_3d = safe_zscore(
                    mid,
                    average.get("mean_mid_3d"),
                    average.get("std_mid_3d"),
                    stat_name="mid_3d",
                    baseline_key=baseline_key,
                )
                row["z_mid_35d"] = z_mid_35d
                row["z_mid_3d"] = z_mid_3d

                if current_iv is not None:
                    z_iv_35d = safe_zscore(
                        current_iv,
                        average.get("mean_iv_35d"),
                        average.get("std_iv_35d"),
                        stat_name="iv_35d",
                        baseline_key=baseline_key,
                    )
                    z_iv_3d = safe_zscore(
                        current_iv,
                        average.get("mean_iv_3d"),
                        average.get("std_iv_3d"),
                        stat_name="iv_3d",
                        baseline_key=baseline_key,
                    )
                    row["z_iv_35d"] = z_iv_35d
                    row["z_iv_3d"] = z_iv_3d

                maybe_queue_combined_alert(
                    raw_symbol=raw_symbol,
                    metadata=metadata,
                    row=row,
                    now=now,
                )

            mark_contract_state_updated(row, now)


dashboard_server = start_dashboard_server(build_dashboard_payload)
debug("dashboard ready url=http://127.0.0.1:8765")

try:
    while True:
        msg = consumer.poll(1.0)
        now_utc = dt.datetime.now(dt.timezone.utc)

        if msg is None:
            for raw_symbol in list(raw_symbols_rolling_volume_addition):
                update_rolling_volume(raw_symbol, now_utc)
            refresh_runtime_status(now_utc)
            continue

        if msg.error():
            debug(f"kafka error {msg.error()}")
            refresh_runtime_status(now_utc)
            continue

        event = json.loads(msg.value().decode("utf-8"))

        with state_lock:
            runtime_status["events_total"] += 1
            events_total = runtime_status["events_total"]

        if event["type"] == "symbol_mapper":
            with state_lock:
                symbol_mapper = event["symbol_mapper"]
            mapped_raws = len(symbol_mapper)
            mapped_rows = sum(len(rows) for rows in symbol_mapper.values())
            mapped_parents = len({
                row["parent_symbol"]
                for rows in symbol_mapper.values()
                for row in rows
            })
            debug(f"symbol mapper loaded raws={mapped_raws} rows={mapped_rows} parents={mapped_parents}")
        elif event["type"] == "volume_record":
            trade_volume = int(event["volume"])
            raw_symbol = event["raw_symbol"]
            event_ts = parse_event_timestamp(event["timestamp"])
            with state_lock:
                runtime_status["last_event_ts"] = event_ts
                runtime_status["last_volume_ts"] = event_ts
                runtime_status["volume_events_total"] += 1
            raw_symbols_rolling_volume_addition.setdefault(raw_symbol, []).append((event_ts, trade_volume))
            update_rolling_volume(raw_symbol, event_ts)
        elif event["type"] == "quote_record":
            event_ts = parse_event_timestamp(event["timestamp"])
            with state_lock:
                runtime_status["last_event_ts"] = event_ts
                runtime_status["quote_events_total"] += 1
            process_quote_event(event)
        else:
            debug(f"unknown event type={event.get('type')}")

        refresh_runtime_status(now_utc)
        if events_total == 1 or events_total % EVENT_LOG_INTERVAL == 0:
            with state_lock:
                debug(
                    f"events total={runtime_status['events_total']} "
                    f"quotes={runtime_status['quote_events_total']} "
                    f"volumes={runtime_status['volume_events_total']} "
                    f"live_rows={len(live_contract_state)} "
                    f"alerts={runtime_status['alerts_total']}"
                )
except KeyboardInterrupt:
    debug("shutdown requested")
finally:
    consumer.close()
    signal_producer.flush(5.0)
    dashboard_server.shutdown()
    dashboard_server.server_close()
    debug("shutdown complete")
