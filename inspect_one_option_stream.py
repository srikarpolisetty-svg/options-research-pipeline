import databento as db
import datetime as dt
from config import DATABENTO_API_KEY
from statistics import mean
from statistics import stdev
import duckdb
from confluent_kafka import Producer
import json

producer = Producer({"bootstrap.servers": "localhost:9092"})


def to_event_timestamp(ts_event):
    return dt.datetime.fromtimestamp(
        ts_event / 1_000_000_000,
        tz=dt.timezone.utc,
    ).replace(tzinfo=None).isoformat()




import duckdb

con1 = duckdb.connect("rawsymbols.db", read_only=True)


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

    print(symbol)
    print(df)

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


con1.close()
symbol_list_event = {
    "type": "symbols_list",
    "symbols_list": subscribe_symbols_list,
}

producer.produce("market-records", json.dumps(symbol_list_event).encode("utf-8"))
producer.poll(0)



mapper_event = {
    "type": "symbol_mapper",
    "symbol_mapper": symbol_mapper,
}

producer.produce("market-records", json.dumps(mapper_event).encode("utf-8"))
producer.poll(0)



live = db.Live(key=DATABENTO_API_KEY)




live.subscribe(
    dataset="OPRA.PILLAR",
    schema="cbbo-1m",
    symbols=subscribe_symbols_list,
    stype_in="raw_symbol",
)

live.subscribe(
    dataset="OPRA.PILLAR",
    schema="trades",
    symbols=subscribe_symbols_list,
    stype_in="raw_symbol",
)

inst_to_raw = {}

for rec in live:
    if rec.rtype == db.RType.SYMBOL_MAPPING:
        inst_to_raw[int(rec.instrument_id)] = str(rec.stype_in_symbol)
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
