"""Track emitted signal events in a separate DuckDB using its own Kafka consumer."""

from __future__ import annotations

import datetime as dt
import json
import math
from collections import defaultdict
from typing import Any

import duckdb
import yfinance as yf
from confluent_kafka import Consumer


MARKET_TOPIC = "market-records"
SIGNAL_TOPIC = "signal-events"
SIGNAL_DB_PATH = "signal_tracking.duckdb"
UNDERLYING_REFRESH = dt.timedelta(minutes=1)
SIGNAL_FLAT_RETURN_BAND = 0.05
EVENT_LOG_INTERVAL = 5_000
CHECKPOINTS = {
    "5m": dt.timedelta(minutes=5),
    "15m": dt.timedelta(minutes=15),
    "30m": dt.timedelta(minutes=30),
    "1h": dt.timedelta(hours=1),
}


def debug(message: str) -> None:
    print(f"[SIGNAL TRACKER] {message}", flush=True)


consumer = Consumer({
    "bootstrap.servers": "localhost:9092",
    "group.id": "signal-tracker-group",
    "auto.offset.reset": "latest",
})
consumer.subscribe([SIGNAL_TOPIC, MARKET_TOPIC])
debug(f"kafka subscribed topics={SIGNAL_TOPIC},{MARKET_TOPIC} group=signal-tracker-group")

underlying_price_cache: dict[str, dict[str, Any]] = {}
active_signals: dict[str, dict[str, Any]] = {}
signals_by_raw_symbol: dict[str, set[str]] = defaultdict(set)


def parse_strike_from_raw_symbol(raw_symbol: str | None) -> float | None:
    if raw_symbol is None:
        return None
    try:
        return int(str(raw_symbol)[-8:]) / 1000.0
    except Exception:
        return None


def parse_expiration_date_from_raw_symbol(raw_symbol: str | None) -> dt.date | None:
    if raw_symbol is None:
        return None
    try:
        return dt.datetime.strptime(str(raw_symbol)[-15:-9], "%y%m%d").date()
    except Exception:
        return None


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


def parse_event_timestamp(timestamp: str) -> dt.datetime:
    parsed = dt.datetime.fromisoformat(timestamp)
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=dt.timezone.utc)
    return parsed.astimezone(dt.timezone.utc)


def get_underlying_price(parent_symbol: str, now: dt.datetime) -> tuple[float | None, dt.datetime | None]:
    cached = underlying_price_cache.get(parent_symbol)
    if cached is not None and now - cached["timestamp"] < UNDERLYING_REFRESH:
        return cached["price"], cached["timestamp"]

    hist = yf.Ticker(parent_symbol).history(period="1d", interval="1m")
    close_series = hist["Close"].dropna() if "Close" in hist.columns else None
    if close_series is None or close_series.empty:
        return None, None

    price = float(close_series.iloc[-1])
    stamp = now
    underlying_price_cache[parent_symbol] = {
        "price": price,
        "timestamp": stamp,
    }
    return price, stamp


def ensure_tables(con: duckdb.DuckDBPyConnection) -> None:
    con.execute("""
        CREATE TABLE IF NOT EXISTS signal_events (
            signal_id TEXT,
            alert_date DATE,
            alert_timestamp TIMESTAMP,
            alert_type TEXT,
            parent_symbol TEXT,
            raw_option_symbol TEXT,
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
            option_bid DOUBLE,
            option_ask DOUBLE,
            option_mid DOUBLE,
            option_spread_pct DOUBLE,
            underlying_price DOUBLE,
            current_iv DOUBLE,
            rolling_volume_10m BIGINT,
            rolling_volume_30m BIGINT,
            rolling_volume_1h BIGINT,
            z_vol_35d DOUBLE,
            z_vol_3d DOUBLE,
            z_vol_35d_band TEXT,
            z_vol_3d_band TEXT,
            z_mid_35d DOUBLE,
            z_mid_3d DOUBLE,
            z_mid_35d_band TEXT,
            z_mid_3d_band TEXT,
            z_iv_35d DOUBLE,
            z_iv_3d DOUBLE,
            z_iv_35d_band TEXT,
            z_iv_3d_band TEXT,
            option_quote_timestamp TIMESTAMP,
            alert_message TEXT
        );
    """)
    con.execute("""
        CREATE TABLE IF NOT EXISTS signal_checkpoints (
            signal_id TEXT,
            alert_date DATE,
            alert_timestamp TIMESTAMP,
            checkpoint_label TEXT,
            due_timestamp TIMESTAMP,
            captured_timestamp TIMESTAMP,
            raw_option_symbol TEXT,
            parent_symbol TEXT,
            strike DOUBLE,
            expiration_date DATE,
            side TEXT,
            grouping TEXT,
            moneyness_grouping TEXT,
            decay_bucket TEXT,
            option_bid DOUBLE,
            option_ask DOUBLE,
            option_mid DOUBLE,
            option_quote_timestamp TIMESTAMP,
            option_quote_age_seconds DOUBLE,
            underlying_price DOUBLE,
            underlying_price_timestamp TIMESTAMP,
            option_return_pct DOUBLE,
            underlying_return_pct DOUBLE,
            status TEXT
        );
    """)
    con.execute("""
        CREATE TABLE IF NOT EXISTS signal_outcomes (
            signal_id TEXT,
            alert_date DATE,
            alert_timestamp TIMESTAMP,
            finalized_timestamp TIMESTAMP,
            parent_symbol TEXT,
            raw_option_symbol TEXT,
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
            option_return_15m DOUBLE,
            option_return_30m DOUBLE,
            label_15m TEXT,
            label_30m TEXT,
            max_up_pct DOUBLE,
            max_down_pct DOUBLE,
            mfe_pct DOUBLE,
            mae_pct DOUBLE,
            best_option_mid DOUBLE,
            best_option_mid_timestamp TIMESTAMP,
            worst_option_mid DOUBLE,
            worst_option_mid_timestamp TIMESTAMP
        );
    """)
    schema_upgrades = {
        "signal_events": [
            ("alert_date", "DATE"),
            ("strike", "DOUBLE"),
            ("expiration_date", "DATE"),
            ("moneyness_grouping", "TEXT"),
            ("z_vol_35d_band", "TEXT"),
            ("z_vol_3d_band", "TEXT"),
            ("z_mid_35d_band", "TEXT"),
            ("z_mid_3d_band", "TEXT"),
            ("z_iv_35d_band", "TEXT"),
            ("z_iv_3d_band", "TEXT"),
        ],
        "signal_checkpoints": [
            ("alert_date", "DATE"),
            ("alert_timestamp", "TIMESTAMP"),
            ("strike", "DOUBLE"),
            ("expiration_date", "DATE"),
            ("side", "TEXT"),
            ("grouping", "TEXT"),
            ("moneyness_grouping", "TEXT"),
            ("decay_bucket", "TEXT"),
        ],
        "signal_outcomes": [
            ("alert_date", "DATE"),
            ("strike", "DOUBLE"),
            ("expiration_date", "DATE"),
            ("side", "TEXT"),
            ("grouping", "TEXT"),
            ("moneyness_grouping", "TEXT"),
            ("decay_bucket", "TEXT"),
            ("z_vol_35d_band", "TEXT"),
            ("z_vol_3d_band", "TEXT"),
            ("z_mid_35d_band", "TEXT"),
            ("z_mid_3d_band", "TEXT"),
            ("z_iv_35d_band", "TEXT"),
            ("z_iv_3d_band", "TEXT"),
            ("max_up_pct", "DOUBLE"),
            ("max_down_pct", "DOUBLE"),
        ],
    }
    for table_name, columns in schema_upgrades.items():
        for column_name, column_type in columns:
            con.execute(f"ALTER TABLE {table_name} ADD COLUMN IF NOT EXISTS {column_name} {column_type}")


def insert_signal_event(con: duckdb.DuckDBPyConnection, event: dict[str, Any]) -> None:
    alert_timestamp = parse_event_timestamp(event["alert_timestamp"])
    raw_symbol = event.get("raw_option_symbol")
    strike = event.get("strike")
    if strike is None:
        strike = parse_strike_from_raw_symbol(raw_symbol)
    expiration_date = parse_expiration_date_from_raw_symbol(raw_symbol)
    z_vol_35d = event.get("z_vol_35d")
    z_vol_3d = event.get("z_vol_3d")
    z_mid_35d = event.get("z_mid_35d")
    z_mid_3d = event.get("z_mid_3d")
    z_iv_35d = event.get("z_iv_35d")
    z_iv_3d = event.get("z_iv_3d")

    con.execute(
        """
        INSERT INTO signal_events (
            signal_id,
            alert_date,
            alert_timestamp,
            alert_type,
            parent_symbol,
            raw_option_symbol,
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
            option_bid,
            option_ask,
            option_mid,
            option_spread_pct,
            underlying_price,
            current_iv,
            rolling_volume_10m,
            rolling_volume_30m,
            rolling_volume_1h,
            z_vol_35d,
            z_vol_3d,
            z_vol_35d_band,
            z_vol_3d_band,
            z_mid_35d,
            z_mid_3d,
            z_mid_35d_band,
            z_mid_3d_band,
            z_iv_35d,
            z_iv_3d,
            z_iv_35d_band,
            z_iv_3d_band,
            option_quote_timestamp,
            alert_message
        ) VALUES (
            ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
        )
        """,
        [
            event["signal_id"],
            alert_timestamp.date(),
            alert_timestamp.replace(tzinfo=None),
            event.get("alert_type"),
            event.get("parent_symbol"),
            raw_symbol,
            strike,
            expiration_date,
            event.get("side"),
            event.get("grouping"),
            event.get("grouping"),
            event.get("decay_bucket"),
            event.get("threshold"),
            event.get("metric_value"),
            event.get("z_35d"),
            event.get("z_3d"),
            event.get("option_bid"),
            event.get("option_ask"),
            event.get("option_mid"),
            event.get("option_spread_pct"),
            event.get("underlying_price"),
            event.get("current_iv"),
            event.get("rolling_volume_10m"),
            event.get("rolling_volume_30m"),
            event.get("rolling_volume_1h"),
            z_vol_35d,
            z_vol_3d,
            zscore_band(z_vol_35d),
            zscore_band(z_vol_3d),
            z_mid_35d,
            z_mid_3d,
            zscore_band(z_mid_35d),
            zscore_band(z_mid_3d),
            z_iv_35d,
            z_iv_3d,
            zscore_band(z_iv_35d),
            zscore_band(z_iv_3d),
            (
                parse_event_timestamp(event["option_quote_timestamp"]).replace(tzinfo=None)
                if event.get("option_quote_timestamp")
                else None
            ),
            event.get("alert_message"),
        ],
    )


def initialize_active_signal(event: dict[str, Any]) -> dict[str, Any]:
    alert_timestamp = parse_event_timestamp(event["alert_timestamp"])
    raw_symbol = event["raw_option_symbol"]
    strike = event.get("strike")
    if strike is None:
        strike = parse_strike_from_raw_symbol(raw_symbol)
    initial_option_mid = event.get("option_mid")
    initial_quote_timestamp = (
        parse_event_timestamp(event["option_quote_timestamp"])
        if event.get("option_quote_timestamp")
        else alert_timestamp
    )
    return {
        "signal_id": event["signal_id"],
        "alert_timestamp": alert_timestamp,
        "parent_symbol": event["parent_symbol"],
        "raw_option_symbol": raw_symbol,
        "strike": strike,
        "expiration_date": parse_expiration_date_from_raw_symbol(raw_symbol),
        "side": event.get("side"),
        "grouping": event.get("grouping"),
        "moneyness_grouping": event.get("grouping"),
        "decay_bucket": event.get("decay_bucket"),
        "z_vol_35d_band": zscore_band(event.get("z_vol_35d")),
        "z_vol_3d_band": zscore_band(event.get("z_vol_3d")),
        "z_mid_35d_band": zscore_band(event.get("z_mid_35d")),
        "z_mid_3d_band": zscore_band(event.get("z_mid_3d")),
        "z_iv_35d_band": zscore_band(event.get("z_iv_35d")),
        "z_iv_3d_band": zscore_band(event.get("z_iv_3d")),
        "alert_option_mid": initial_option_mid,
        "alert_underlying_price": event.get("underlying_price"),
        "latest_option_bid": event.get("option_bid"),
        "latest_option_ask": event.get("option_ask"),
        "latest_option_mid": initial_option_mid,
        "latest_option_ts": initial_quote_timestamp,
        "best_option_mid": initial_option_mid,
        "best_option_mid_ts": initial_quote_timestamp if initial_option_mid is not None else None,
        "worst_option_mid": initial_option_mid,
        "worst_option_mid_ts": initial_quote_timestamp if initial_option_mid is not None else None,
        "checkpoint_option_returns": {},
        "remaining_checkpoints": {
            label: alert_timestamp + delta
            for label, delta in CHECKPOINTS.items()
        },
    }


def track_signal_event(con: duckdb.DuckDBPyConnection, event: dict[str, Any]) -> None:
    insert_signal_event(con, event)
    signal = initialize_active_signal(event)
    active_signals[signal["signal_id"]] = signal
    signals_by_raw_symbol[signal["raw_option_symbol"]].add(signal["signal_id"])
    debug(
        f"signal stored active={len(active_signals)} parent={signal['parent_symbol']} "
        f"raw={signal['raw_option_symbol']} side={signal.get('side')} group={signal.get('grouping')}"
    )


def update_signal_quote_state(event: dict[str, Any]) -> None:
    raw_symbol = event.get("raw_symbol")
    if raw_symbol not in signals_by_raw_symbol:
        return

    quote_timestamp = parse_event_timestamp(event["timestamp"])
    for signal_id in list(signals_by_raw_symbol[raw_symbol]):
        signal = active_signals.get(signal_id)
        if signal is None:
            continue
        signal["latest_option_bid"] = event.get("bid")
        signal["latest_option_ask"] = event.get("ask")
        signal["latest_option_mid"] = event.get("mid")
        signal["latest_option_ts"] = quote_timestamp
        latest_option_mid = event.get("mid")
        if latest_option_mid is None:
            continue
        if signal.get("best_option_mid") is None or latest_option_mid > signal["best_option_mid"]:
            signal["best_option_mid"] = latest_option_mid
            signal["best_option_mid_ts"] = quote_timestamp
        if signal.get("worst_option_mid") is None or latest_option_mid < signal["worst_option_mid"]:
            signal["worst_option_mid"] = latest_option_mid
            signal["worst_option_mid_ts"] = quote_timestamp


def pct_return(current_value: float | None, start_value: float | None) -> float | None:
    if current_value is None or start_value is None or start_value == 0:
        return None
    return (float(current_value) - float(start_value)) / float(start_value)


def label_signal_return(option_return_pct: float | None) -> str:
    if option_return_pct is None:
        return "unknown"
    if option_return_pct >= SIGNAL_FLAT_RETURN_BAND:
        return "winner"
    if option_return_pct <= -SIGNAL_FLAT_RETURN_BAND:
        return "loser"
    return "flat"


def insert_checkpoint(
    con: duckdb.DuckDBPyConnection,
    *,
    signal: dict[str, Any],
    checkpoint_label: str,
    due_timestamp: dt.datetime,
    captured_timestamp: dt.datetime,
    underlying_price: float | None,
    underlying_price_timestamp: dt.datetime | None,
) -> None:
    option_quote_timestamp = signal.get("latest_option_ts")
    option_quote_age_seconds = None
    if option_quote_timestamp is not None:
        option_quote_age_seconds = (captured_timestamp - option_quote_timestamp).total_seconds()

    option_mid = signal.get("latest_option_mid")
    option_return_pct = pct_return(option_mid, signal.get("alert_option_mid"))
    underlying_return_pct = pct_return(underlying_price, signal.get("alert_underlying_price"))
    status = "captured"
    if option_mid is None:
        status = "missing_option_quote"
    elif underlying_price is None:
        status = "missing_underlying_price"

    con.execute(
        """
        INSERT INTO signal_checkpoints (
            signal_id,
            alert_date,
            alert_timestamp,
            checkpoint_label,
            due_timestamp,
            captured_timestamp,
            raw_option_symbol,
            parent_symbol,
            strike,
            expiration_date,
            side,
            grouping,
            moneyness_grouping,
            decay_bucket,
            option_bid,
            option_ask,
            option_mid,
            option_quote_timestamp,
            option_quote_age_seconds,
            underlying_price,
            underlying_price_timestamp,
            option_return_pct,
            underlying_return_pct,
            status
        ) VALUES (
            ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
        )
        """,
        [
            signal["signal_id"],
            signal["alert_timestamp"].date(),
            signal["alert_timestamp"].replace(tzinfo=None),
            checkpoint_label,
            due_timestamp.replace(tzinfo=None),
            captured_timestamp.replace(tzinfo=None),
            signal["raw_option_symbol"],
            signal["parent_symbol"],
            signal.get("strike"),
            signal.get("expiration_date"),
            signal.get("side"),
            signal.get("grouping"),
            signal.get("moneyness_grouping"),
            signal.get("decay_bucket"),
            signal.get("latest_option_bid"),
            signal.get("latest_option_ask"),
            option_mid,
            option_quote_timestamp.replace(tzinfo=None) if option_quote_timestamp is not None else None,
            option_quote_age_seconds,
            underlying_price,
            underlying_price_timestamp.replace(tzinfo=None) if underlying_price_timestamp is not None else None,
            option_return_pct,
            underlying_return_pct,
            status,
        ],
    )
    debug(
        f"checkpoint stored label={checkpoint_label} parent={signal['parent_symbol']} "
        f"raw={signal['raw_option_symbol']} opt_ret={option_return_pct} status={status}"
    )
    return option_return_pct


def insert_signal_outcome(
    con: duckdb.DuckDBPyConnection,
    *,
    signal: dict[str, Any],
    finalized_timestamp: dt.datetime,
) -> None:
    option_return_15m = signal["checkpoint_option_returns"].get("15m")
    option_return_30m = signal["checkpoint_option_returns"].get("30m")
    best_option_mid = signal.get("best_option_mid")
    worst_option_mid = signal.get("worst_option_mid")
    alert_option_mid = signal.get("alert_option_mid")

    con.execute(
        """
        INSERT INTO signal_outcomes (
            signal_id,
            alert_date,
            alert_timestamp,
            finalized_timestamp,
            parent_symbol,
            raw_option_symbol,
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
            option_return_15m,
            option_return_30m,
            label_15m,
            label_30m,
            max_up_pct,
            max_down_pct,
            mfe_pct,
            mae_pct,
            best_option_mid,
            best_option_mid_timestamp,
            worst_option_mid,
            worst_option_mid_timestamp
        ) VALUES (
            ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
        )
        """,
        [
            signal["signal_id"],
            signal["alert_timestamp"].date(),
            signal["alert_timestamp"].replace(tzinfo=None),
            finalized_timestamp.replace(tzinfo=None),
            signal["parent_symbol"],
            signal["raw_option_symbol"],
            signal.get("strike"),
            signal.get("expiration_date"),
            signal.get("side"),
            signal.get("grouping"),
            signal.get("moneyness_grouping"),
            signal.get("decay_bucket"),
            signal.get("z_vol_35d_band"),
            signal.get("z_vol_3d_band"),
            signal.get("z_mid_35d_band"),
            signal.get("z_mid_3d_band"),
            signal.get("z_iv_35d_band"),
            signal.get("z_iv_3d_band"),
            option_return_15m,
            option_return_30m,
            label_signal_return(option_return_15m),
            label_signal_return(option_return_30m),
            pct_return(best_option_mid, alert_option_mid),
            pct_return(worst_option_mid, alert_option_mid),
            pct_return(best_option_mid, alert_option_mid),
            pct_return(worst_option_mid, alert_option_mid),
            best_option_mid,
            signal["best_option_mid_ts"].replace(tzinfo=None) if signal.get("best_option_mid_ts") is not None else None,
            worst_option_mid,
            signal["worst_option_mid_ts"].replace(tzinfo=None) if signal.get("worst_option_mid_ts") is not None else None,
        ],
    )
    debug(
        f"outcome stored parent={signal['parent_symbol']} raw={signal['raw_option_symbol']} "
        f"15m={label_signal_return(option_return_15m)} 30m={label_signal_return(option_return_30m)}"
    )


def capture_due_checkpoints(con: duckdb.DuckDBPyConnection, now: dt.datetime) -> None:
    completed_signal_ids: list[str] = []

    for signal_id, signal in list(active_signals.items()):
        due_labels = [
            label
            for label, due_timestamp in signal["remaining_checkpoints"].items()
            if due_timestamp <= now
        ]
        if not due_labels:
            continue

        underlying_price, underlying_timestamp = get_underlying_price(signal["parent_symbol"], now)

        for label in due_labels:
            due_timestamp = signal["remaining_checkpoints"].pop(label)
            option_return_pct = insert_checkpoint(
                con,
                signal=signal,
                checkpoint_label=label,
                due_timestamp=due_timestamp,
                captured_timestamp=now,
                underlying_price=underlying_price,
                underlying_price_timestamp=underlying_timestamp,
            )
            signal["checkpoint_option_returns"][label] = option_return_pct

        if not signal["remaining_checkpoints"]:
            completed_signal_ids.append(signal_id)

    for signal_id in completed_signal_ids:
        signal = active_signals.pop(signal_id, None)
        if signal is None:
            continue
        insert_signal_outcome(con, signal=signal, finalized_timestamp=now)
        raw_symbol = signal["raw_option_symbol"]
        signals_by_raw_symbol[raw_symbol].discard(signal_id)
        if not signals_by_raw_symbol[raw_symbol]:
            signals_by_raw_symbol.pop(raw_symbol, None)


def main() -> None:
    con = duckdb.connect(SIGNAL_DB_PATH)
    ensure_tables(con)
    debug("db ready path=signal_tracking.duckdb")
    debug("consuming signal-events + market-records")
    events_seen = 0

    try:
        while True:
            msg = consumer.poll(1.0)
            now_utc = dt.datetime.now(dt.timezone.utc)

            if msg is None:
                capture_due_checkpoints(con, now_utc)
                continue

            if msg.error():
                debug(f"kafka error {msg.error()}")
                capture_due_checkpoints(con, now_utc)
                continue

            event = json.loads(msg.value().decode("utf-8"))
            event_type = event.get("type")
            events_seen += 1

            if event_type == "signal_event":
                track_signal_event(con, event)
            elif event_type == "quote_record":
                update_signal_quote_state(event)

            capture_due_checkpoints(con, now_utc)
            if events_seen == 1 or events_seen % EVENT_LOG_INTERVAL == 0:
                debug(f"events seen={events_seen} active_signals={len(active_signals)}")
    except KeyboardInterrupt:
        debug("shutdown requested")
    finally:
        consumer.close()
        con.close()
        debug("shutdown complete")


if __name__ == "__main__":
    main()
