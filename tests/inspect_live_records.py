from __future__ import annotations

import argparse
import time

import _path_setup  # noqa: F401
import databento as db
import duckdb

from config import DATABENTO_API_KEY


def load_raw_symbols(db_path: str, limit: int | None) -> list[str]:
    con = duckdb.connect(db_path, read_only=True)
    try:
        query = """
            SELECT DISTINCT raw_option_symbol
            FROM raw_symbols
            WHERE raw_option_symbol IS NOT NULL
            ORDER BY raw_option_symbol
        """
        rows = con.execute(query).fetchall()
    finally:
        con.close()

    raws = [str(row[0]) for row in rows if row and row[0]]
    if limit is not None:
        return raws[:limit]
    return raws


def is_symbol_mapping_msg(rec) -> bool:
    if type(rec).__name__ == "SymbolMappingMsg":
        return True
    rtype = getattr(rec, "rtype", None)
    return bool(rtype is not None and str(rtype).endswith("SYMBOL_MAPPING"))


def main() -> int:
    parser = argparse.ArgumentParser(description="Inspect live Databento records for raw option symbols.")
    parser.add_argument("--db-path", default="rawsymbols.db")
    parser.add_argument("--limit-symbols", type=int, default=5)
    parser.add_argument("--max-records", type=int, default=25)
    parser.add_argument("--schema", choices=["trades", "cbbo-1m", "cbbo-1s"], default="trades")
    parser.add_argument("--batch-size", type=int, default=200)
    parser.add_argument("--sleep-sec", type=float, default=0.25)
    args = parser.parse_args()

    if not DATABENTO_API_KEY:
        raise RuntimeError("DATABENTO_API_KEY is not set.")

    raw_symbols = load_raw_symbols(args.db_path, args.limit_symbols)
    if not raw_symbols:
        raise RuntimeError(f"No raw symbols found in {args.db_path}.")

    print(f"Loaded {len(raw_symbols)} raw symbols from {args.db_path}")

    live = db.Live(key=DATABENTO_API_KEY)
    inst_to_raw: dict[int, str] = {}

    for i in range(0, len(raw_symbols), args.batch_size):
        batch = raw_symbols[i : i + args.batch_size]
        live.subscribe(
            dataset="OPRA.PILLAR",
            schema=args.schema,
            symbols=batch,
            stype_in="raw_symbol",
        )
        time.sleep(args.sleep_sec)

    live.start()
    print(f"Streaming schema={args.schema}. Waiting for up to {args.max_records} records...")

    seen = 0
    try:
        for rec in live:
            if is_symbol_mapping_msg(rec):
                inst = getattr(rec, "instrument_id", None)
                raw = getattr(rec, "stype_in_symbol", None) or getattr(rec, "stype_out_symbol", None)
                if inst is not None and raw:
                    inst_to_raw[int(inst)] = str(raw)
                print(
                    "MAPPING",
                    {
                        "instrument_id": inst,
                        "raw_symbol": raw,
                    },
                )
                continue

            inst = getattr(rec, "instrument_id", None)
            raw = inst_to_raw.get(int(inst)) if inst is not None and int(inst) in inst_to_raw else None
            if raw is None:
                raw = getattr(rec, "symbol", None) or getattr(rec, "raw_symbol", None)

            print(
                "RECORD",
                {
                    "type": type(rec).__name__,
                    "raw_symbol": raw,
                    "instrument_id": inst,
                    "ts_event": getattr(rec, "ts_event", None),
                    "repr": repr(rec),
                },
            )

            seen += 1
            if seen >= args.max_records:
                break
    finally:
        try:
            live.stop()
        except Exception:
            pass

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
