"""Consume live Kafka market records and emit baseline-based alerts."""

from confluent_kafka import Consumer
import json
import datetime as dt
import math
from statistics import mean
from statistics import stdev
import yfinance as yf
from historical_baselines import load_historical_baselines
from message import send_text

def parse_event_timestamp(timestamp):
    return dt.datetime.fromisoformat(timestamp)

consumer = Consumer({
    "bootstrap.servers": "localhost:9092",
    "group.id": "calc-group",
    "auto.offset.reset": "earliest",
})

consumer.subscribe(["market-records"])

raw_symbols_data = {}
raw_symbols_rolling_volume_addition = {}
rolling_volume = {}
rolling_volume_history = []
last_select_time = None
last_insert_time = None
ten_minutes = dt.timedelta(minutes=10)
subscribe_symbols_list = []

symbol_mapper = {}
underlying_price_cache = {}

baselines = load_historical_baselines()
missing_baseline_keys = set()
invalid_baseline_stats = set()


def parse_strike_from_raw_symbol(raw_symbol):
    return int(str(raw_symbol)[-8:]) / 1000.0


def parse_expiration_date_from_raw_symbol(raw_symbol):
    return dt.datetime.strptime(str(raw_symbol)[-15:-9], "%y%m%d").date()


def days_to_expiry_from_raw_symbol(raw_symbol, now):
    expiration_date = parse_expiration_date_from_raw_symbol(raw_symbol)
    return (expiration_date - now.date()).days


def get_underlying_price(parent_symbol, now):
    cached = underlying_price_cache.get(parent_symbol)
    if cached is not None:
        cache_time = cached["timestamp"]
        if now - cache_time < dt.timedelta(minutes=1):
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


def bs_iv_bisect(mid, underlying_price, strike, days_to_expiry, call_put):
    if mid is None or mid <= 0 or underlying_price is None or underlying_price <= 0:
        return None

    time_to_expiry = float(days_to_expiry) / 365.0
    if time_to_expiry <= 0:
        return None

    r = 0.01
    lo, hi = 1e-6, 5.0

    def normal_cdf(x):
        return 0.5 * (1.0 + math.erf(x / math.sqrt(2.0)))

    def bs_price(sigma):
        d1 = (math.log(underlying_price / strike) + (r + 0.5 * sigma * sigma) * time_to_expiry) / (sigma * math.sqrt(time_to_expiry))
        d2 = d1 - sigma * math.sqrt(time_to_expiry)
        if call_put == "C":
            return underlying_price * normal_cdf(d1) - strike * math.exp(-r * time_to_expiry) * normal_cdf(d2)
        return strike * math.exp(-r * time_to_expiry) * normal_cdf(-d2) - underlying_price * normal_cdf(-d1)

    for _ in range(60):
        sigma = 0.5 * (lo + hi)
        if bs_price(sigma) > mid:
            hi = sigma
        else:
            lo = sigma

    return 0.5 * (lo + hi)


def get_baseline(parent_symbol, side, grouping, decay_bucket):
    key = (parent_symbol, side, grouping, decay_bucket)
    baseline = baselines.get(key)
    if baseline is None and key not in missing_baseline_keys:
        print(f"[WARN] missing baseline for {key}")
        missing_baseline_keys.add(key)
    return baseline


def safe_zscore(value, mean_value, std_value, *, stat_name: str, baseline_key):
    if value is None or mean_value is None or std_value is None:
        bad_key = (*baseline_key, stat_name)
        if bad_key not in invalid_baseline_stats:
            print(f"[WARN] unusable baseline stats for {bad_key}")
            invalid_baseline_stats.add(bad_key)
        return None

    try:
        value_f = float(value)
        mean_f = float(mean_value)
        std_f = float(std_value)
    except Exception:
        bad_key = (*baseline_key, stat_name)
        if bad_key not in invalid_baseline_stats:
            print(f"[WARN] non-numeric baseline stats for {bad_key}")
            invalid_baseline_stats.add(bad_key)
        return None

    if not math.isfinite(value_f) or not math.isfinite(mean_f) or not math.isfinite(std_f) or std_f <= 0:
        bad_key = (*baseline_key, stat_name)
        if bad_key not in invalid_baseline_stats:
            print(f"[WARN] invalid baseline std/value for {bad_key}")
            invalid_baseline_stats.add(bad_key)
        return None

    return (value_f - mean_f) / std_f


def update_rolling_volume(raw_symbol, now):
    old_rolling_volume = rolling_volume.get(raw_symbol, 0)
    raw_symbols_rolling_volume_addition[raw_symbol] = [
        (entry_ts, entry_vol)
        for entry_ts, entry_vol in raw_symbols_rolling_volume_addition[raw_symbol]
        if now - entry_ts <= ten_minutes
    ]

    rolling_volume[raw_symbol] = sum(
        entry_vol
        for entry_ts, entry_vol in raw_symbols_rolling_volume_addition[raw_symbol]
    )

    if rolling_volume[raw_symbol] == old_rolling_volume:
        return

    if raw_symbol not in symbol_mapper:
        return

    for map in symbol_mapper[raw_symbol]:
        side = map["side"]
        grouping = map["grouping"]
        parent_symbol = map["parent_symbol"]
        decay_bucket = map["decay_bucket"]
        baseline_key = (parent_symbol, side, grouping, decay_bucket)
        average = get_baseline(*baseline_key)
        if average is None:
            continue

        current_volume = rolling_volume.get(raw_symbol, 0)
        z_vol_35d = safe_zscore(
            current_volume,
            average.get("mean_vol_35d"),
            average.get("std_vol_35d"),
            stat_name="vol_35d",
            baseline_key=baseline_key,
        )
        z_vol_3d = safe_zscore(
            current_volume,
            average.get("mean_vol_3d"),
            average.get("std_vol_3d"),
            stat_name="vol_3d",
            baseline_key=baseline_key,
        )

        if z_vol_35d is None or z_vol_3d is None:
            continue

        if z_vol_35d >= 1.5 and z_vol_3d >= 1.5:
            send_text(f"Volume alert for {raw_symbol}: parent = {parent_symbol} side = {side},grouping={grouping},z35={z_vol_35d}, z3={z_vol_3d}")

while True:
    msg = consumer.poll(1.0)
    if msg is None:
        now = dt.datetime.now(dt.timezone.utc)
        for raw_symbol in list(raw_symbols_rolling_volume_addition):
            update_rolling_volume(raw_symbol, now)
        continue
    if msg.error():
        continue

    event = json.loads(msg.value().decode("utf-8"))
    print(event)

    if event["type"] == "symbol_mapper":
        symbol_mapper = event["symbol_mapper"]
    if event["type"] == "volume_record":
        trade_volume = event["volume"]
        raw_symbol = event["raw_symbol"]
        now = parse_event_timestamp(event["timestamp"])
        if raw_symbol not in raw_symbols_rolling_volume_addition:
            raw_symbols_rolling_volume_addition[raw_symbol] = []

        raw_symbols_rolling_volume_addition[raw_symbol].append((now, trade_volume))
        update_rolling_volume(raw_symbol, now)

        







    if event["type"] == "quote_record":
        raw_symbol = event["raw_symbol"]
        bid = event["bid"]
        ask = event["ask"]
        mid = event["mid"]
        spread = event["spread"]
        spread_pct = event["spread_pct"]
        now = parse_event_timestamp(event["timestamp"])
        if raw_symbol not in symbol_mapper:
            continue

        metadata_rows = symbol_mapper[raw_symbol]
        strike = parse_strike_from_raw_symbol(raw_symbol)
        days_to_expiry = days_to_expiry_from_raw_symbol(raw_symbol, now)
        parent_symbol = metadata_rows[0]["parent_symbol"]
        underlying_price = get_underlying_price(parent_symbol, now)

        for map in metadata_rows:
            side = map["side"]
            grouping = map["grouping"]
            parent_symbol = map["parent_symbol"]
            decay_bucket = map["decay_bucket"]
            baseline_key = (parent_symbol, side, grouping, decay_bucket)
            average = get_baseline(*baseline_key)
            if average is None:
                continue

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
            current_iv = bs_iv_bisect(mid, underlying_price, strike, days_to_expiry, side)

            if z_mid_35d is not None and z_mid_3d is not None and z_mid_35d >= 1.5 and z_mid_3d >= 1.5:
                send_text(f"Mid alert for {raw_symbol}: parent = {parent_symbol} side = {side},grouping={grouping},z35={z_mid_35d}, z3={z_mid_3d}")
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
                if z_iv_35d is not None and z_iv_3d is not None and z_iv_35d >= 1.5 and z_iv_3d >= 1.5:
                    send_text(f"IV alert for {raw_symbol}: parent = {parent_symbol} side = {side},grouping={grouping},z35={z_iv_35d}, z3={z_iv_3d}")
       
