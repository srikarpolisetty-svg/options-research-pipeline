# Runbook

This runbook is for local development and demonstration. It assumes the virtual environment is already installed and `.env` contains a valid `DATABENTO_API_KEY`.

## Build Historical Data

```bash
.venv/bin/python databentodatabasebackfillworkingversion.py
```

For the max-one-year workflow:

```bash
.venv/bin/python one_year_backfill_and_backtest.py
```

To replay against existing data only:

```bash
.venv/bin/python one_year_backfill_and_backtest.py --skip-backfill
```

## Build Live Universe

```bash
.venv/bin/python definitions_cache_builder.py
.venv/bin/python universe_builder.py
```

Outputs:

```text
definitioncache.duckdb
rawsymbols.db
```

## Run Live Pipeline

Start Kafka first. Then run each process separately:

```bash
.venv/bin/python databento_live_producer.py
.venv/bin/python live_alert_consumer.py
.venv/bin/python signal_tracker.py
```

Dashboard:

```text
http://127.0.0.1:8765
```

## Run Tests

```bash
.venv/bin/python tests/test_underlying_confirmation_backtest.py
.venv/bin/python -m py_compile backtest_combined_alerts.py one_year_backfill_and_backtest.py
```

## Common Notes

- DuckDB supports multiple readers, but write access should be isolated. The live consumer and signal tracker intentionally write to different state stores.
- Backtest reports are generated artifacts and are ignored by git.
- Hard Databento request failures such as `422 symbology_invalid_request` are not retryable. Temporary gateway errors such as `504` are retryable.
- The live dashboard is local-only by default on `127.0.0.1:8765`.
