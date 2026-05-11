"""Build a max-one-year historical dataset, then replay the current alert logic."""

from __future__ import annotations

import argparse
import datetime as dt
import time
from pathlib import Path

import duckdb

import backtest_combined_alerts as backtest
import databentodatabasebackfillworkingversion as backfill
from databasefunctions import get_sp500_symbols


MAX_DATABENTO_LOOKBACK_DAYS = 365
DEFAULT_DAYS_BACK = 365


def debug(message: str) -> None:
    print(f"[ONE_YEAR] {message}", flush=True)


def parse_csv_symbols(value: str | None) -> list[str] | None:
    if value is None:
        return None
    symbols = [token.strip().upper() for token in value.split(",") if token.strip()]
    return symbols or None


def one_year_request_floor_utc() -> dt.datetime:
    return backfill.db_end_utc_day() - dt.timedelta(days=MAX_DATABENTO_LOOKBACK_DAYS)


def validate_days_back(days_back: int) -> None:
    if days_back < 1:
        raise SystemExit("--days-back must be at least 1")
    if days_back > MAX_DATABENTO_LOOKBACK_DAYS:
        raise SystemExit(
            f"--days-back={days_back} is not allowed. "
            f"Max is {MAX_DATABENTO_LOOKBACK_DAYS} so Databento requests stay within one year."
        )


def resolve_symbols(symbols_arg: list[str] | None) -> list[str]:
    raw_symbols = symbols_arg or get_sp500_symbols()
    symbols = backfill.filter_supported_option_chain_symbols(raw_symbols)
    if not symbols:
        raise SystemExit("No supported symbols selected.")
    return symbols


def run_safe_backfill(
    *,
    symbols: list[str],
    days_back: int,
    options_db: str,
    request_floor: dt.datetime,
) -> None:
    backfill.DB_PATH = options_db
    backfill.set_databento_request_floor(request_floor)
    backfill.set_definition_cache_enabled(True)

    start_time = time.time()
    try:
        backfill.wipe_batch_downloads()
        backfill.ensure_table()
        backfill.delete_old_rows(days_back)

        debug(
            f"backfill start symbols={len(symbols)} days_back={days_back} "
            f"request_floor={request_floor.isoformat()}"
        )
        raw_symbols_list, plans = backfill.create_raw_symbols_list(symbols, days_back)
        backfill.get_data(raw_symbols_list, plans, days_back)
    finally:
        backfill.wipe_batch_downloads()
        debug(f"backfill finished seconds={time.time() - start_time:.2f}")


def run_safe_backtest(
    *,
    options_db: str,
    output_db: str,
    report_dir: str,
    parents: list[str] | None,
    start_date: dt.date | None,
    end_date: dt.date | None,
    request_floor: dt.datetime,
) -> Path:
    floor_date = request_floor.date()
    if start_date is not None and start_date < floor_date:
        raise SystemExit(
            f"--start-date={start_date.isoformat()} is older than the one-year floor "
            f"{floor_date.isoformat()}"
        )

    safe_start_date = start_date or floor_date
    if end_date is not None and end_date < safe_start_date:
        raise SystemExit("--end-date must be on or after the resolved start date")

    started_at = dt.datetime.now(dt.timezone.utc)
    run_id = backtest.readable_utc_run_id("one_year_backtest", started_at)

    hist_con = duckdb.connect(options_db, read_only=True)
    out_con = duckdb.connect(output_db)
    try:
        resolved_start, resolved_end = backtest.resolve_date_range(
            hist_con,
            start_date=safe_start_date,
            end_date=end_date,
            parents=parents,
        )
        resolved_start, resolved_end = backtest.apply_underlying_confirmation_date_limit(
            resolved_start,
            resolved_end,
        )
        if resolved_start < floor_date:
            raise RuntimeError(
                f"Resolved start date {resolved_start.isoformat()} crossed one-year floor "
                f"{floor_date.isoformat()}"
            )

        backtest.ensure_output_tables(out_con)
        backtest.insert_run_started(
            out_con,
            run_id=run_id,
            started_at=started_at,
            start_date=resolved_start,
            end_date=resolved_end,
            parents=parents,
        )

        trade_dates = backtest.fetch_trade_dates(
            hist_con,
            start_date=resolved_start,
            end_date=resolved_end,
            parents=parents,
        )
        debug(
            f"backtest start run_id={run_id} dates={len(trade_dates)} "
            f"range={resolved_start}..{resolved_end} "
            f"parents={','.join(parents) if parents else 'ALL'}"
        )

        total_alerts = 0
        total_outcomes = 0
        for idx, trade_date in enumerate(trade_dates, start=1):
            day_stats = backtest.replay_day(
                hist_con,
                out_con,
                run_id=run_id,
                trade_date=trade_date,
                parents=parents,
            )
            total_alerts += day_stats["alerts"]
            total_outcomes += day_stats["outcomes"]
            debug(
                f"day {idx}/{len(trade_dates)} {trade_date} "
                f"contracts={day_stats['contracts']} events={day_stats['events']} "
                f"alerts={day_stats['alerts']} outcomes={day_stats['outcomes']}"
            )

        completed_at = dt.datetime.now(dt.timezone.utc)
        backtest.finalize_run(
            out_con,
            run_id=run_id,
            completed_at=completed_at,
            trade_days=len(trade_dates),
            alerts_total=total_alerts,
            outcomes_total=total_outcomes,
        )
        report_path = backtest.generate_run_report(
            out_con,
            run_id=run_id,
            report_dir=Path(report_dir),
        )
        debug(
            f"backtest done run_id={run_id} alerts={total_alerts} "
            f"outcomes={total_outcomes} report={report_path}"
        )
        return report_path
    finally:
        hist_con.close()
        out_con.close()


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--days-back", type=int, default=DEFAULT_DAYS_BACK)
    parser.add_argument("--symbols", type=parse_csv_symbols, default=None)
    parser.add_argument("--parents", type=parse_csv_symbols, default=None)
    parser.add_argument("--start-date", type=backtest.parse_date, default=None)
    parser.add_argument("--end-date", type=backtest.parse_date, default=None)
    parser.add_argument("--options-db", default=backtest.OPTIONS_DB_PATH)
    parser.add_argument("--output-db", default=backtest.OUTPUT_DB_PATH)
    parser.add_argument("--report-dir", default=str(backtest.REPORT_DIR))
    parser.add_argument("--skip-backfill", action="store_true")
    parser.add_argument("--skip-backtest", action="store_true")
    return parser


def main() -> None:
    args = build_arg_parser().parse_args()
    validate_days_back(args.days_back)

    request_floor = one_year_request_floor_utc()
    symbols = resolve_symbols(args.symbols)
    parents = args.parents if args.parents is not None else args.symbols

    debug(
        f"request guard active: no Databento request starts before "
        f"{request_floor.isoformat()}"
    )

    if not args.skip_backfill:
        run_safe_backfill(
            symbols=symbols,
            days_back=args.days_back,
            options_db=args.options_db,
            request_floor=request_floor,
        )

    if not args.skip_backtest:
        run_safe_backtest(
            options_db=args.options_db,
            output_db=args.output_db,
            report_dir=args.report_dir,
            parents=parents,
            start_date=args.start_date,
            end_date=args.end_date,
            request_floor=request_floor,
        )


if __name__ == "__main__":
    main()
