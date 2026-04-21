from __future__ import annotations

"""Shared Databento loading and cache helpers for local backtests."""

import hashlib
import json
import time
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import _path_setup  # noqa: F401
import databento as db
import pandas as pd

BATCH_CACHE_DIR = Path(__file__).resolve().parent / "batch_downloads" / "configurable_backtest"
MAX_REQUEST_LOOKBACK_DAYS = 364
MAX_BATCH_SYMBOLS = 2_000


def _pick_col(df: pd.DataFrame, candidates: list[str]) -> str | None:
    for candidate in candidates:
        if candidate in df.columns:
            return candidate
    return None


def _normalize_price(series: pd.Series) -> pd.Series:
    normalized = pd.to_numeric(series, errors="coerce")
    median_abs = normalized.dropna().abs().median()
    if pd.notna(median_abs) and median_abs > 1_000_000:
        return normalized / 1e9
    return normalized


def _dedupe_symbols(symbols: list[str]) -> list[str]:
    return list(dict.fromkeys(str(symbol) for symbol in symbols))


def _split_symbol_batches(symbols: list[str], max_batch_symbols: int = MAX_BATCH_SYMBOLS) -> list[list[str]]:
    deduped = _dedupe_symbols(symbols)
    if max_batch_symbols <= 0:
        raise ValueError("max_batch_symbols must be positive")
    return [
        deduped[idx : idx + max_batch_symbols]
        for idx in range(0, len(deduped), max_batch_symbols)
    ]


def _load_option_definitions(
    hist: db.Historical,
    symbol: str,
    start_d: date,
    end_d: date,
) -> pd.DataFrame:
    defs = _load_batch_df(
        hist=hist,
        dataset="OPRA.PILLAR",
        schema="definition",
        symbols=[f"{symbol}.OPT"],
        stype_in="parent",
        start_d=start_d,
        end_d=end_d,
        split_duration="month",
        request_name="definitions",
    )
    if defs is None or defs.empty:
        raise RuntimeError(f"No OPRA definition rows for {symbol}.OPT")

    defs = defs.copy()
    defs["exp_date"] = pd.to_datetime(defs["expiration"], utc=True, errors="coerce").dt.date
    defs["strike_f"] = pd.to_numeric(defs["strike_price"], errors="coerce")
    defs["instrument_class"] = defs["instrument_class"].astype(str)
    defs["raw_symbol"] = defs["raw_symbol"].astype(str)
    return defs.dropna(subset=["exp_date", "strike_f", "raw_symbol"])


def _request_floor(end_d: date) -> date:
    return end_d - timedelta(days=MAX_REQUEST_LOOKBACK_DAYS)


def _cap_request_start(start_d: date, end_d: date) -> date:
    return max(start_d, _request_floor(end_d))


def _batch_request_dir(
    dataset: str,
    schema: str,
    symbols: list[str],
    stype_in: str,
    start_d: date,
    end_d: date,
    split_duration: str,
) -> Path:
    payload = {
        "dataset": dataset,
        "schema": schema,
        "symbols": list(symbols),
        "stype_in": stype_in,
        "start": start_d.isoformat(),
        "end": end_d.isoformat(),
        "split_duration": split_duration,
    }
    key = hashlib.sha256(json.dumps(payload, sort_keys=True).encode("utf-8")).hexdigest()[:16]
    return BATCH_CACHE_DIR / f"{schema}_{key}"


def _request_date_floor(value: str) -> date:
    return date.fromisoformat(str(value)[:10])


def _cached_data_paths(request_dir: Path) -> list[Path]:
    return [
        path
        for path in request_dir.rglob("*")
        if path.is_file() and path.suffix in {".dbn", ".zst"} and not path.name.endswith(".json")
    ]


def _filter_frame_to_symbols(df: pd.DataFrame, symbols: list[str], stype_in: str) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame() if df is None else df

    if stype_in == "parent":
        return df

    if stype_in == "raw_symbol":
        symbol_col = _pick_col(df, ["raw_symbol", "symbol"])
    else:
        symbol_col = _pick_col(df, ["symbol", "raw_symbol"])

    if symbol_col is None:
        return df

    wanted = {str(symbol) for symbol in symbols}
    return df[df[symbol_col].astype(str).isin(wanted)].copy()


def _find_reusable_cached_request(
    *,
    dataset: str,
    schema: str,
    symbols: list[str],
    stype_in: str,
    start_d: date,
    end_d: date,
    split_duration: str,
) -> Path | None:
    requested_symbols = set(symbols)
    schema_prefix = f"{schema}_"
    if not BATCH_CACHE_DIR.exists():
        return None

    for request_dir in sorted(BATCH_CACHE_DIR.iterdir()):
        if not request_dir.is_dir() or not request_dir.name.startswith(schema_prefix):
            continue

        request_meta_path = request_dir / "request.json"
        if not request_meta_path.exists():
            continue

        try:
            meta = json.loads(request_meta_path.read_text())
        except Exception:
            continue

        if meta.get("dataset") != dataset:
            continue
        if meta.get("schema") != schema:
            continue
        if meta.get("stype_in") != stype_in:
            continue
        if meta.get("split_duration") != split_duration:
            continue

        cached_symbols = set(meta.get("symbols", []))
        if not requested_symbols.issubset(cached_symbols):
            continue

        cached_start_d = _request_date_floor(meta.get("start"))
        cached_end_d = _request_date_floor(meta.get("end"))
        if cached_start_d > start_d or cached_end_d < end_d:
            continue

        if _cached_data_paths(request_dir):
            return request_dir

    return None


def _find_partial_cached_requests(
    *,
    dataset: str,
    schema: str,
    symbols: list[str],
    stype_in: str,
    start_d: date,
    end_d: date,
    split_duration: str,
) -> list[tuple[Path, set[str]]]:
    requested_symbols = set(symbols)
    schema_prefix = f"{schema}_"
    if not BATCH_CACHE_DIR.exists():
        return []

    candidates: list[tuple[Path, set[str]]] = []
    for request_dir in sorted(BATCH_CACHE_DIR.iterdir()):
        if not request_dir.is_dir() or not request_dir.name.startswith(schema_prefix):
            continue

        request_meta_path = request_dir / "request.json"
        if not request_meta_path.exists():
            continue

        try:
            meta = json.loads(request_meta_path.read_text())
        except Exception:
            continue

        if meta.get("dataset") != dataset:
            continue
        if meta.get("schema") != schema:
            continue
        if meta.get("stype_in") != stype_in:
            continue
        if meta.get("split_duration") != split_duration:
            continue

        cached_start_d = _request_date_floor(meta.get("start"))
        cached_end_d = _request_date_floor(meta.get("end"))
        if cached_start_d > start_d or cached_end_d < end_d:
            continue

        cached_paths = _cached_data_paths(request_dir)
        if not cached_paths:
            continue

        cached_symbols = {str(symbol) for symbol in meta.get("symbols", [])}
        covered_symbols = requested_symbols & cached_symbols
        if covered_symbols:
            candidates.append((request_dir, covered_symbols))

    selected: list[tuple[Path, set[str]]] = []
    covered_total: set[str] = set()
    for request_dir, covered_symbols in sorted(candidates, key=lambda item: len(item[1]), reverse=True):
        incremental = covered_symbols - covered_total
        if not incremental:
            continue
        selected.append((request_dir, incremental))
        covered_total |= incremental
        if covered_total == requested_symbols:
            break

    return selected


def _wait_for_batch_job(hist: db.Historical, job_id: str, *, timeout_seconds: int = 3600) -> dict:
    deadline = time.time() + timeout_seconds
    while time.time() < deadline:
        jobs = hist.batch.list_jobs(states="queued,processing,done,expired")
        for job in jobs:
            if str(job.get("id")) != job_id:
                continue
            state = str(job.get("state", "")).lower()
            if state == "done":
                return job
            if state == "expired":
                raise RuntimeError(f"Batch job expired before download: {job_id}")
            break
        time.sleep(5.0)
    raise TimeoutError(f"Timed out waiting for Databento batch job {job_id}")


def _read_batch_data_files(paths: list[Path]) -> pd.DataFrame:
    data_paths = [
        path
        for path in paths
        if path.is_file() and path.suffix in {".dbn", ".zst"} and not path.name.endswith(".json")
    ]
    if not data_paths:
        return pd.DataFrame()

    frames: list[pd.DataFrame] = []
    for path in sorted(data_paths):
        store = db.read_dbn(path)
        frame = store.to_df(pretty_ts=False, map_symbols=True)
        if frame is not None and not frame.empty:
            frames.append(frame)
    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True)


def _load_batch_df(
    *,
    hist: db.Historical,
    dataset: str,
    schema: str,
    symbols: list[str],
    stype_in: str,
    start_d: date,
    end_d: date,
    split_duration: str,
    request_name: str,
) -> pd.DataFrame:
    symbols = _dedupe_symbols(symbols)
    if len(symbols) > MAX_BATCH_SYMBOLS:
        symbol_batches = _split_symbol_batches(symbols)
        print(
            f"{request_name}: splitting {len(symbols)} symbols into "
            f"{len(symbol_batches)} batches of <= {MAX_BATCH_SYMBOLS}"
        )
        frames: list[pd.DataFrame] = []
        for batch_idx, symbol_batch in enumerate(symbol_batches, start=1):
            batch_frame = _load_batch_df(
                hist=hist,
                dataset=dataset,
                schema=schema,
                symbols=symbol_batch,
                stype_in=stype_in,
                start_d=start_d,
                end_d=end_d,
                split_duration=split_duration,
                request_name=f"{request_name} [{batch_idx}/{len(symbol_batches)}]",
            )
            if batch_frame is not None and not batch_frame.empty:
                frames.append(batch_frame)
        if not frames:
            return pd.DataFrame()
        return pd.concat(frames, ignore_index=True).drop_duplicates().reset_index(drop=True)

    start_d = _cap_request_start(start_d, end_d)
    requested_end_dt = datetime.combine(end_d + timedelta(days=1), datetime.min.time(), tzinfo=timezone.utc)
    available_end_dt = datetime.combine(
        datetime.now(timezone.utc).date(),
        datetime.min.time(),
        tzinfo=timezone.utc,
    ) - timedelta(minutes=15)
    batch_end_dt = min(requested_end_dt, available_end_dt)
    if batch_end_dt <= datetime.combine(start_d, datetime.min.time(), tzinfo=timezone.utc):
        raise RuntimeError(f"{request_name}: batch end must be after start")

    request_dir = _batch_request_dir(
        dataset=dataset,
        schema=schema,
        symbols=symbols,
        stype_in=stype_in,
        start_d=start_d,
        end_d=end_d,
        split_duration=split_duration,
    )
    request_dir.mkdir(parents=True, exist_ok=True)
    request_meta_path = request_dir / "request.json"
    job_id_path = request_dir / "job_id.txt"

    cached_paths = _cached_data_paths(request_dir)
    if cached_paths:
        print(f"{request_name}: using cached batch files from {request_dir}")
        return _filter_frame_to_symbols(_read_batch_data_files(cached_paths), symbols, stype_in)

    reusable_request_dir = _find_reusable_cached_request(
        dataset=dataset,
        schema=schema,
        symbols=symbols,
        stype_in=stype_in,
        start_d=start_d,
        end_d=end_d,
        split_duration=split_duration,
    )
    if reusable_request_dir is not None:
        reusable_paths = _cached_data_paths(reusable_request_dir)
        if reusable_paths:
            print(f"{request_name}: reusing broader cached batch files from {reusable_request_dir}")
            return _filter_frame_to_symbols(_read_batch_data_files(reusable_paths), symbols, stype_in)

    partial_cached_requests = _find_partial_cached_requests(
        dataset=dataset,
        schema=schema,
        symbols=symbols,
        stype_in=stype_in,
        start_d=start_d,
        end_d=end_d,
        split_duration=split_duration,
    )
    if partial_cached_requests:
        covered_symbols: set[str] = set()
        frames: list[pd.DataFrame] = []
        for cached_request_dir, incremental_symbols in partial_cached_requests:
            covered_symbols |= incremental_symbols
            cached_frame = _filter_frame_to_symbols(
                _read_batch_data_files(_cached_data_paths(cached_request_dir)),
                sorted(incremental_symbols),
                stype_in,
            )
            if not cached_frame.empty:
                frames.append(cached_frame)

        missing_symbols = [symbol for symbol in symbols if symbol not in covered_symbols]
        print(
            f"{request_name}: reusing cached data for {len(covered_symbols)}/{len(symbols)} symbols; "
            f"requesting {len(missing_symbols)} missing symbols"
        )
        if missing_symbols:
            missing_frame = _load_batch_df(
                hist=hist,
                dataset=dataset,
                schema=schema,
                symbols=missing_symbols,
                stype_in=stype_in,
                start_d=start_d,
                end_d=end_d,
                split_duration=split_duration,
                request_name=f"{request_name} missing",
            )
            if missing_frame is not None and not missing_frame.empty:
                frames.append(missing_frame)

        if frames:
            return pd.concat(frames, ignore_index=True).drop_duplicates().reset_index(drop=True)
        return pd.DataFrame()

    if job_id_path.exists():
        job_id = job_id_path.read_text().strip()
        if job_id:
            print(f"{request_name}: resuming existing batch job {job_id}")
            _wait_for_batch_job(hist, job_id)
            print(f"{request_name}: downloading batch job {job_id}")
            downloaded_paths = hist.batch.download(job_id=job_id, output_dir=request_dir)
            return _filter_frame_to_symbols(_read_batch_data_files(downloaded_paths), symbols, stype_in)

    print(f"{request_name}: submitting Databento batch job")
    job = hist.batch.submit_job(
        dataset=dataset,
        symbols=symbols,
        schema=schema,
        start=start_d.isoformat(),
        end=batch_end_dt.isoformat(),
        stype_in=stype_in,
        split_duration=split_duration,
    )
    job_id = str(job["id"])
    request_meta_path.write_text(
        json.dumps(
            {
                "dataset": dataset,
                "schema": schema,
                "symbols": symbols,
                "stype_in": stype_in,
                "start": start_d.isoformat(),
                "end": batch_end_dt.isoformat(),
                "job_id": job_id,
                "split_duration": split_duration,
            },
            indent=2,
        )
    )
    job_id_path.write_text(job_id)
    print(f"{request_name}: waiting for batch job {job_id}")
    _wait_for_batch_job(hist, job_id)
    print(f"{request_name}: downloading batch job {job_id}")
    downloaded_paths = hist.batch.download(job_id=job_id, output_dir=request_dir)
    return _filter_frame_to_symbols(_read_batch_data_files(downloaded_paths), symbols, stype_in)


def _load_quotes_for_raws(
    hist: db.Historical,
    raws: list[str],
    start_d: date,
    end_d: date,
) -> pd.DataFrame:
    quotes = _load_batch_df(
        hist=hist,
        dataset="OPRA.PILLAR",
        schema="cbbo-1m",
        symbols=raws,
        stype_in="raw_symbol",
        start_d=start_d,
        end_d=end_d,
        split_duration="week",
        request_name="quotes",
    )
    if quotes is None or quotes.empty:
        return pd.DataFrame(columns=["raw_symbol", "minute", "bid", "ask", "mid"])

    q_symbol_col = _pick_col(quotes, ["symbol", "raw_symbol"])
    bid_col = _pick_col(quotes, ["bid_px_00", "bid_px", "bid"])
    ask_col = _pick_col(quotes, ["ask_px_00", "ask_px", "ask"])
    if q_symbol_col is None or bid_col is None or ask_col is None:
        raise RuntimeError(f"Could not locate quote symbol/bid/ask columns. quote_cols={list(quotes.columns)}")

    quotes = quotes.copy()
    quotes["raw_symbol"] = quotes[q_symbol_col].astype(str)
    quotes["minute"] = pd.to_datetime(quotes["ts_event"], utc=True, errors="coerce").dt.floor("1min")
    quotes["bid"] = _normalize_price(quotes[bid_col])
    quotes["ask"] = _normalize_price(quotes[ask_col])
    quotes = quotes.dropna(subset=["raw_symbol", "minute", "bid", "ask"])
    quotes = quotes.sort_values(["raw_symbol", "minute", "ts_event"]).drop_duplicates(
        ["raw_symbol", "minute"],
        keep="last",
    )
    quotes["mid"] = (quotes["bid"] + quotes["ask"]) / 2.0
    return quotes.reset_index(drop=True)


def _load_trades_for_raws(
    hist: db.Historical,
    raws: list[str],
    start_d: date,
    end_d: date,
) -> pd.DataFrame:
    trades = _load_batch_df(
        hist=hist,
        dataset="OPRA.PILLAR",
        schema="trades",
        symbols=raws,
        stype_in="raw_symbol",
        start_d=start_d,
        end_d=end_d,
        split_duration="week",
        request_name="trades",
    )
    if trades is None or trades.empty:
        return pd.DataFrame(columns=["raw_symbol", "minute", "volume"])

    t_symbol_col = _pick_col(trades, ["symbol", "raw_symbol"])
    size_col = _pick_col(trades, ["size", "quantity"])
    if t_symbol_col is None or size_col is None:
        raise RuntimeError(f"Could not locate trades symbol/size columns. trade_cols={list(trades.columns)}")

    trades = trades.copy()
    trades["raw_symbol"] = trades[t_symbol_col].astype(str)
    trades["minute"] = pd.to_datetime(trades["ts_event"], utc=True, errors="coerce").dt.floor("1min")
    trades["size_num"] = pd.to_numeric(trades[size_col], errors="coerce").fillna(0.0)
    return (
        trades.groupby(["raw_symbol", "minute"], as_index=False)["size_num"]
        .sum()
        .rename(columns={"size_num": "volume"})
    )


__all__ = [
    "MAX_REQUEST_LOOKBACK_DAYS",
    "_load_option_definitions",
    "_load_quotes_for_raws",
    "_load_trades_for_raws",
    "_request_floor",
]
