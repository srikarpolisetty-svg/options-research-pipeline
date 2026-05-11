"""Subscribe to Databento live option streams and publish Kafka market records."""

import databento as db
import datetime as dt
from config import DATABENTO_API_KEY
import duckdb
from confluent_kafka import Producer
import json

producer = Producer({"bootstrap.servers": "localhost:9092"})
EVENT_LOG_INTERVAL = 5_000


def debug(message: str) -> None:
    print(f"[LIVE PRODUCER] {message}", flush=True)


def to_event_timestamp(ts_event):
    return dt.datetime.fromtimestamp(
        ts_event / 1_000_000_000,
        tz=dt.timezone.utc,
    ).replace(tzinfo=None).isoformat()

con1 = duckdb.connect("rawsymbols.db", read_only=True)
debug("opened rawsymbols.db read_only")


symbols = con1.execute("""
    SELECT DISTINCT parent_symbol
    FROM raw_symbols
    ORDER BY parent_symbol
""").fetchdf()["parent_symbol"].tolist()

symbol_mapper = {}
subscribe_symbols_list = []

for symbol in symbols:
    df = con1.execute("""
        SELECT raw_option_symbol, side, grouping,decay_bucket
        FROM raw_symbols
        WHERE parent_symbol = ?
    """, [symbol]).fetchdf()

    subscribe_symbols_list.extend(df["raw_option_symbol"].tolist())


    for _, row in df.iterrows():
        raw_option_symbol = row["raw_option_symbol"]

        symbol_mapper.setdefault(raw_option_symbol, []).append({
            "side": row["side"],
            "grouping": row["grouping"],
            "parent_symbol": symbol,
            "decay_bucket": row["decay_bucket"],
        })

subscribe_symbols_list = list(dict.fromkeys(subscribe_symbols_list))
debug(f"raw universe loaded parents={len(symbols)} raws={len(subscribe_symbols_list)}")


con1.close()
symbol_list_event = {
    "type": "symbols_list",
    "symbols_list": subscribe_symbols_list,
}

producer.produce("market-records", json.dumps(symbol_list_event).encode("utf-8"))
producer.poll(0)
debug("published symbols_list")



mapper_event = {
    "type": "symbol_mapper",
    "symbol_mapper": symbol_mapper,
}

producer.produce("market-records", json.dumps(mapper_event).encode("utf-8"))
producer.poll(0)
debug(f"published symbol_mapper raws={len(symbol_mapper)}")



live = db.Live(key=DATABENTO_API_KEY)
debug("databento live client ready")




live.subscribe(
    dataset="OPRA.PILLAR",
    schema="cbbo-1m",
    symbols=subscribe_symbols_list,
    stype_in="raw_symbol",
)
debug("subscribed quotes schema=cbbo-1m")

live.subscribe(
    dataset="OPRA.PILLAR",
    schema="trades",
    symbols=subscribe_symbols_list,
    stype_in="raw_symbol",
)
debug("subscribed trades schema=trades")

inst_to_raw = {}
symbol_mappings = 0
quote_events = 0
volume_events = 0

for rec in live:
    if rec.rtype == db.RType.SYMBOL_MAPPING:
        inst_to_raw[int(rec.instrument_id)] = str(rec.stype_in_symbol)
        symbol_mappings += 1
        if symbol_mappings == 1 or symbol_mappings % EVENT_LOG_INTERVAL == 0:
            debug(f"symbol mappings={symbol_mappings}")
        continue

    if rec.rtype == db.RType.MBP_0:
        trade_volume = rec.size
        now = to_event_timestamp(rec.ts_event)
        raw_symbol = inst_to_raw.get(int(rec.instrument_id))
        if raw_symbol is None:
            continue

        volume_event = {
            "type": "volume_record",
            "raw_symbol": raw_symbol,
            "volume": trade_volume,
            "timestamp": now,
        }
        producer.produce("market-records", json.dumps(volume_event).encode("utf-8"))
        producer.poll(0)
        volume_events += 1
        if volume_events == 1 or volume_events % EVENT_LOG_INTERVAL == 0:
            debug(f"volume events={volume_events} latest={raw_symbol} vol={trade_volume}")





    if rec.rtype != db.RType.CBBO_1M:
        continue

    raw_symbol = inst_to_raw.get(int(rec.instrument_id))
    if raw_symbol is None:
        continue

    bid = rec.levels[0].pretty_bid_px
    ask = rec.levels[0].pretty_ask_px
    mid = (bid + ask) / 2
    if mid == 0:
        continue

    spread = ask - bid
    spread_pct = spread / mid
    now = to_event_timestamp(rec.ts_event)


    quote_event = {
        "type": "quote_record",
        "raw_symbol": raw_symbol,
        "bid": bid,
        "ask": ask,
        "mid": mid,
        "spread": spread,
        "spread_pct": spread_pct,
        "timestamp": now,
    }

    producer.produce("market-records", json.dumps(quote_event).encode("utf-8"))
    producer.poll(0)
    quote_events += 1
    if quote_events == 1 or quote_events % EVENT_LOG_INTERVAL == 0:
        debug(f"quote events={quote_events} latest={raw_symbol} mid={mid:.3f}")
