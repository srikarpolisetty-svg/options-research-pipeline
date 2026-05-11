# Project Summary

## Problem

The goal was to test whether unusual option activity could be detected early enough to create a usable short-term trading signal. The original thesis was that volume, option mid-price, and implied-volatility z-scores could identify contracts that were about to expand quickly.

## System Built

This project evolved into a full research platform:

- Databento historical OPRA backfill with batching, retries, request guards, and definition caching.
- DuckDB storage for quote snapshots, rolling volume, option definitions, live universe data, signal outcomes, and backtest results.
- Live Databento to Kafka producer.
- Stateful Kafka consumer that maintains live quote, rolling volume, IV, and z-score state.
- Local dashboard for subscribed contracts and alert history.
- Separate signal tracker that consumes the same Kafka events and records post-signal outcomes.
- Live-style historical replay engine that tests the same signal logic against historical data.

## Final Strategy Tested

The final strict version required:

```text
volume anomaly leads
quote/mid confirms within 3 minutes
underlying confirms with a 15-minute breakout
first alert only
OTM1/OTM2 contracts
HIGH/EXTREME decay bucket
exit on underlying failure or 15-minute timeout
```

## Result

The signal did identify real short-term option expansions, but the strategy was not strong enough at portfolio level after realistic exits and misses. The best tested exit produced roughly low-single-digit returns on deployed option premium over the measured window.

## Conclusion

The z-score anomaly idea was fully explored and responsibly closed. The research outcome is useful: z-score anomaly detection can find movement, but it is not a complete trading edge by itself. The reusable value is the pipeline, dashboard, backtester, and research process.

## What This Demonstrates

- Ability to build an end-to-end market-data system.
- Ability to handle live and historical data consistently.
- Ability to design experiments, measure outcomes, and stop when data does not justify more complexity.
- Ability to turn a trading hypothesis into a testable engineering system.
