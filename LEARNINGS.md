# Strategy Learnings

## 2026-04-24

Source:
- Full-universe backtest report `run_20260424T130016`

High-confidence learnings:
- Exits should be fast and active. This behaves like a quick-movement strategy, not a hold-and-wait strategy.
- The first minutes matter a lot. A large share of best moves happen early after the signal.
- Chance/opportunity matters more than fixed 30m hold PnL. The signal can be useful even when blind time-based holding is mediocre.
- Giant z-scores should not automatically be treated as stronger. Bigger anomaly readings can mean overextension, not better opportunity.
- Volume-triggered alerts are healthier than quote-triggered alerts. Volume confirmation looks more reliable than quote-only price movement.

## 2026-04-27

Source:
- Merged from `FINDINGS.txt`

Current thesis:
- First alert matters most. Repeated alerts on the same contract are usually less clean because the move may already be crowded or late.
- Volume spike is the top priority. The best signal context is volume leading the anomaly, not IV or mid already running ahead of volume.
- Far OTM plus short DTE can be the best convexity zone. These contracts can expand fast when the move is real.
- Exit fast when it pops. This strategy is built to catch quick expansion, not to hope through decay, spread, and mean reversion.

Backtest signal filter to test next:
- z_vol_35d >= 6
- z_vol_3d >= 6
- z_vol_35d > z_iv_35d
- z_vol_35d > z_mid_35d
- z_vol_3d > z_iv_3d
- z_vol_3d > z_mid_3d

Plain-English rule:
- Only test signals where volume is both extreme and leading the move.

high/extreme decay is good

## 2026-04-30

Source:
- Next backtest experiment after the 60-day underlying-confirmed report.

New stricter signal thesis:
- Volume still leads. The volume-dominance rule must pass first.
- Underlying must confirm with a breakout. Calls need upside confirmation; puts need downside confirmation.
- Quote confirmation is now required after the volume spike. The signal should fire only from a quote update, not directly from the volume event.
- Quote confirmation must happen fast: within 3 minutes after the volume-dominance trigger.
- Quote confirmation should be small but real: option mid must move at least 3% and at least 0.01 from the reference mid before the volume spike.
- First alert only. Repeated alerts on the same option contract are treated as late/chasing for this experiment.
- Convexity filter is stricter: only OTM1/OTM2 contracts with HIGH/EXTREME decay are allowed.
- Spread filter intentionally skipped for now. The goal is to test quote/underlying/volume alignment first without adding another liquidity gate.

Plain-English rule:
- Volume says something started.
- Quote says the option market is actually repricing it.
- Underlying breakout says the stock confirms it.
- First alert keeps us early instead of chasing.
