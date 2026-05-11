from __future__ import annotations

import datetime as dt
import unittest

import _path_setup  # noqa: F401

from backtest_combined_alerts import (
    STRATEGY_EXIT_MINUTES,
    UnderlyingBar,
    evaluate_underlying_confirmation,
    mark_due_strategy_time_exits,
    underlying_fail_triggered,
)


UTC = dt.timezone.utc


def bar(minute: int, high: float, low: float, close: float) -> UnderlyingBar:
    return UnderlyingBar(
        timestamp=dt.datetime(2026, 4, 24, 13, 30 + minute, tzinfo=UTC),
        open=close,
        high=high,
        low=low,
        close=close,
        volume=100,
    )


class UnderlyingConfirmationBacktestTests(unittest.TestCase):
    def test_call_confirmation_requires_close_above_prior_high(self) -> None:
        bars = [
            bar(0, high=100.2, low=99.8, close=100.0),
            bar(2, high=100.4, low=100.0, close=100.2),
            bar(4, high=100.5, low=100.1, close=100.6),
        ]

        confirmation = evaluate_underlying_confirmation(
            bars,
            side="C",
            alert_timestamp=bars[-1].timestamp,
        )

        self.assertEqual(confirmation.status, "passed")
        self.assertEqual(confirmation.direction, "up")
        self.assertAlmostEqual(confirmation.breakout_level or 0.0, 100.4)
        self.assertAlmostEqual(confirmation.underlying_entry_price or 0.0, 100.6)

    def test_put_confirmation_requires_close_below_prior_low(self) -> None:
        bars = [
            bar(0, high=101.0, low=100.2, close=100.7),
            bar(2, high=100.8, low=100.0, close=100.4),
            bar(4, high=100.5, low=99.8, close=99.7),
        ]

        confirmation = evaluate_underlying_confirmation(
            bars,
            side="P",
            alert_timestamp=bars[-1].timestamp,
        )

        self.assertEqual(confirmation.status, "passed")
        self.assertEqual(confirmation.direction, "down")
        self.assertAlmostEqual(confirmation.breakout_level or 0.0, 100.0)
        self.assertAlmostEqual(confirmation.underlying_entry_price or 0.0, 99.7)

    def test_confirmation_does_not_use_future_bar(self) -> None:
        bars = [
            bar(0, high=100.2, low=99.8, close=100.0),
            bar(2, high=100.4, low=100.0, close=100.2),
            bar(4, high=101.0, low=100.5, close=100.9),
        ]

        confirmation = evaluate_underlying_confirmation(
            bars,
            side="C",
            alert_timestamp=bars[1].timestamp,
        )

        self.assertEqual(confirmation.status, "no_breakout")
        self.assertAlmostEqual(confirmation.breakout_level or 0.0, 100.2)

    def test_underlying_fail_trigger_is_side_based_close_fail(self) -> None:
        self.assertTrue(underlying_fail_triggered(side="C", close_price=100.1, breakout_level=100.2))
        self.assertFalse(underlying_fail_triggered(side="C", close_price=100.3, breakout_level=100.2))
        self.assertTrue(underlying_fail_triggered(side="P", close_price=100.3, breakout_level=100.2))
        self.assertFalse(underlying_fail_triggered(side="P", close_price=100.1, breakout_level=100.2))

    def test_time_exit_marks_signal_at_15_minutes(self) -> None:
        alert_ts = dt.datetime(2026, 4, 24, 14, 0, tzinfo=UTC)
        signal = {
            "strategy_exit_due_ts": alert_ts + dt.timedelta(minutes=STRATEGY_EXIT_MINUTES),
            "strategy_exit_ts": None,
            "latest_option_mid": 1.25,
            "latest_underlying_price": 101.5,
        }

        mark_due_strategy_time_exits({"sig": signal}, alert_ts + dt.timedelta(minutes=15))

        self.assertEqual(signal["strategy_exit_reason"], "time_15m")
        self.assertEqual(signal["strategy_exit_ts"], alert_ts + dt.timedelta(minutes=15))
        self.assertEqual(signal["strategy_exit_option_mid"], 1.25)
        self.assertEqual(signal["strategy_exit_underlying_price"], 101.5)

    def test_missing_underlying_returns_missing_status(self) -> None:
        confirmation = evaluate_underlying_confirmation(
            [],
            side="C",
            alert_timestamp=dt.datetime(2026, 4, 24, 14, 0, tzinfo=UTC),
        )

        self.assertEqual(confirmation.status, "missing_underlying")


if __name__ == "__main__":
    unittest.main()
