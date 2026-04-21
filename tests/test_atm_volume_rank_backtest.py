from __future__ import annotations

import unittest
from datetime import date

import _path_setup  # noqa: F401
import pandas as pd

from atm_volume_rank_backtest import (
    CONFIG,
    _build_active_expiration_map,
    _build_summary,
    _generate_entries,
    _select_best_contract,
    _simulate_exit_path,
)


def _snapshot_rows(
    minute: str,
    trade_date: date,
    strikes: list[float],
    price_moves: list[float],
    volume_zs: list[float],
    symbol: str = "SPY",
) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    for idx, strike in enumerate(strikes):
        rows.append(
            {
                "symbol": symbol,
                "trade_date": trade_date,
                "minute": pd.Timestamp(minute),
                "raw_symbol": f"{symbol}_C_{int(strike)}",
                "strike": float(strike),
                "bid": 0.95 + (idx * 0.01),
                "ask": 1.05 + (idx * 0.01),
                "mid": 1.00 + (idx * 0.01),
                "volume_z": float(volume_zs[idx]),
                "price_move": float(price_moves[idx]),
                "underlying_price": 101.0,
                "underlying_from_open": 0.015,
            }
        )
    return pd.DataFrame(rows)


class AtmVolumeRankBacktestTests(unittest.TestCase):
    def test_active_expiration_map_uses_weekly_fridays_and_skips_third_friday(self) -> None:
        defs = pd.DataFrame(
            {
                "instrument_class": ["C"] * 15,
                "exp_date": (
                    [date(2025, 3, 21)] * 5
                    + [date(2025, 3, 28)] * 5
                    + [date(2025, 3, 26)] * 5
                ),
                "strike_f": [99, 100, 101, 102, 103] * 3,
                "raw_symbol": [f"SYM{i}" for i in range(15)],
            }
        )

        active = _build_active_expiration_map(
            defs=defs,
            trading_dates=[date(2025, 3, 20)],
            config={
                **CONFIG,
                "friday_only": True,
                "exclude_third_friday": True,
            },
        )

        self.assertEqual(len(active), 1)
        self.assertEqual(active.iloc[0]["expiration_date"], date(2025, 3, 28))

    def test_select_best_contract_uses_atm_plus_minus_two_rank_window(self) -> None:
        snapshot = _snapshot_rows(
            minute="2025-03-21 14:40:00+00:00",
            trade_date=date(2025, 3, 21),
            strikes=[98, 99, 100, 101, 102, 103, 104],
            price_moves=[0.01, 0.02, 0.03, 0.01, 0.05, 0.04, 0.02],
            volume_zs=[1.0, 1.1, 1.0, 2.5, 2.0, 3.0, 1.5],
        )

        selected = _select_best_contract(snapshot, strike_span=2)

        self.assertIsNotNone(selected)
        self.assertEqual(float(selected["atm_strike"]), 101.0)
        self.assertEqual(float(selected["strike"]), 103.0)
        self.assertEqual(float(selected["score"]), 9.0)

    def test_generate_entries_allows_only_one_trade_per_symbol_per_day(self) -> None:
        first_snapshot = _snapshot_rows(
            minute="2025-03-21 14:40:00+00:00",
            trade_date=date(2025, 3, 21),
            strikes=[99, 100, 101, 102, 103],
            price_moves=[0.01, 0.02, 0.03, 0.05, 0.04],
            volume_zs=[1.0, 1.2, 2.5, 2.8, 3.0],
        )
        second_snapshot = _snapshot_rows(
            minute="2025-03-21 14:50:00+00:00",
            trade_date=date(2025, 3, 21),
            strikes=[99, 100, 101, 102, 103],
            price_moves=[0.06, 0.05, 0.04, 0.03, 0.02],
            volume_zs=[3.5, 3.4, 3.3, 3.2, 3.1],
        )
        checkpoints = pd.concat([first_snapshot, second_snapshot], ignore_index=True)

        entries = _generate_entries(
            checkpoints,
            {
                **CONFIG,
                "symbols": ["SPY"],
                "strike_span": 2,
                "underlying_day_move_min": 0.01,
                "volume_z_min": 2.0,
                "entry_price_field": "mid",
            },
        )

        self.assertEqual(len(entries), 1)
        self.assertEqual(entries.iloc[0]["minute"], pd.Timestamp("2025-03-21 14:40:00+00:00"))

    def test_generate_entries_filters_local_config_but_keeps_utc_output(self) -> None:
        first_snapshot = _snapshot_rows(
            minute="2025-03-21 14:40:00+00:00",
            trade_date=date(2025, 3, 21),
            strikes=[99, 100, 101, 102, 103],
            price_moves=[0.01, 0.02, 0.03, 0.05, 0.04],
            volume_zs=[1.0, 1.2, 2.5, 2.8, 3.0],
        )
        second_snapshot = _snapshot_rows(
            minute="2025-03-21 14:50:00+00:00",
            trade_date=date(2025, 3, 21),
            strikes=[99, 100, 101, 102, 103],
            price_moves=[0.01, 0.02, 0.04, 0.05, 0.03],
            volume_zs=[1.0, 1.2, 2.6, 2.9, 3.1],
        )
        checkpoints = pd.concat([first_snapshot, second_snapshot], ignore_index=True)

        entries = _generate_entries(
            checkpoints,
            {
                **CONFIG,
                "symbols": ["SPY"],
                "strike_span": 2,
                "entry_times_local": ["10:50"],
                "max_trades_per_symbol_per_day": 2,
                "underlying_day_move_min": 0.01,
                "volume_z_min": 2.0,
                "entry_price_field": "mid",
            },
        )

        self.assertEqual(len(entries), 1)
        self.assertEqual(entries.iloc[0]["time_utc"], "14:50")

    def test_generate_entries_applies_global_day_trade_cap(self) -> None:
        spy_snapshot = _snapshot_rows(
            minute="2025-03-21 14:40:00+00:00",
            trade_date=date(2025, 3, 21),
            strikes=[99, 100, 101, 102, 103],
            price_moves=[0.01, 0.02, 0.03, 0.05, 0.04],
            volume_zs=[1.0, 1.2, 2.5, 2.8, 3.0],
            symbol="SPY",
        )
        qqq_snapshot = _snapshot_rows(
            minute="2025-03-21 14:50:00+00:00",
            trade_date=date(2025, 3, 21),
            strikes=[99, 100, 101, 102, 103],
            price_moves=[0.01, 0.02, 0.03, 0.05, 0.04],
            volume_zs=[1.0, 1.2, 2.5, 2.8, 3.0],
            symbol="QQQ",
        )
        checkpoints = pd.concat([spy_snapshot, qqq_snapshot], ignore_index=True)

        entries = _generate_entries(
            checkpoints,
            {
                **CONFIG,
                "symbols": ["SPY", "QQQ"],
                "strike_span": 2,
                "max_trades_per_symbol_per_day": 2,
                "max_trades_per_day": 1,
                "underlying_day_move_min": 0.01,
                "volume_z_min": 2.0,
                "entry_price_field": "mid",
            },
        )

        self.assertEqual(len(entries), 1)
        self.assertEqual(entries.iloc[0]["symbol"], "SPY")

    def test_generate_entries_can_use_ask_for_entry_price(self) -> None:
        checkpoints = _snapshot_rows(
            minute="2025-03-21 14:40:00+00:00",
            trade_date=date(2025, 3, 21),
            strikes=[99, 100, 101, 102, 103],
            price_moves=[0.01, 0.02, 0.03, 0.05, 0.04],
            volume_zs=[1.0, 1.2, 2.5, 2.8, 3.0],
        )

        entries = _generate_entries(
            checkpoints,
            {
                **CONFIG,
                "symbols": ["SPY"],
                "entry_price_field": "ask",
                "strike_span": 2,
                "underlying_day_move_min": 0.01,
                "volume_z_min": 2.0,
            },
        )

        self.assertEqual(len(entries), 1)
        self.assertEqual(float(entries.iloc[0]["entry_price"]), float(entries.iloc[0]["ask"]))

    def test_simulate_exit_path_handles_take_profit_and_eod_fallback(self) -> None:
        take_profit_path = pd.DataFrame(
            {
                "minute": pd.to_datetime(
                    [
                        "2025-03-21 14:40:00+00:00",
                        "2025-03-21 14:41:00+00:00",
                        "2025-03-21 14:42:00+00:00",
                    ],
                    utc=True,
                ),
                "mid": [1.00, 1.10, 1.20],
            }
        )
        take_profit_result = _simulate_exit_path(
            path=take_profit_path,
            entry_time=pd.Timestamp("2025-03-21 14:40:00+00:00"),
            entry_price=1.00,
            take_profit_pct=0.20,
            stop_loss_pct=0.10,
            price_field="mid",
        )

        self.assertIsNotNone(take_profit_result)
        self.assertEqual(take_profit_result["exit_reason"], "TAKE_PROFIT")
        self.assertEqual(round(float(take_profit_result["return_pct"]), 4), 20.0)

        eod_path = pd.DataFrame(
            {
                "minute": pd.to_datetime(
                    [
                        "2025-03-21 14:40:00+00:00",
                        "2025-03-21 14:41:00+00:00",
                        "2025-03-21 14:42:00+00:00",
                    ],
                    utc=True,
                ),
                "mid": [1.00, 1.06, 1.07],
            }
        )
        eod_result = _simulate_exit_path(
            path=eod_path,
            entry_time=pd.Timestamp("2025-03-21 14:40:00+00:00"),
            entry_price=1.00,
            take_profit_pct=0.20,
            stop_loss_pct=0.10,
            price_field="mid",
        )

        self.assertIsNotNone(eod_result)
        self.assertEqual(eod_result["exit_reason"], "EOD_EXIT")
        self.assertEqual(round(float(eod_result["return_pct"]), 4), 7.0)

    def test_build_summary_matches_requested_metrics(self) -> None:
        trades = pd.DataFrame(
            {
                "return_pct": [20.0, -10.0, 5.0],
                "drawdown": [0.0, 10.0, 5.0],
            }
        )

        summary = _build_summary(trades).iloc[0]

        self.assertEqual(int(summary["total_trades"]), 3)
        self.assertEqual(round(float(summary["win_rate"]), 4), round(2 / 3, 4))
        self.assertEqual(round(float(summary["avg_win"]), 4), 12.5)
        self.assertEqual(round(float(summary["avg_loss"]), 4), -10.0)
        self.assertEqual(
            round(float(summary["expected_value"]), 4),
            round(((2 / 3) * 12.5) + ((1 / 3) * -10.0), 4),
        )
        self.assertEqual(round(float(summary["profit_factor"]), 4), 2.5)
        self.assertEqual(round(float(summary["avg_return_per_trade"]), 4), 5.0)


if __name__ == "__main__":
    unittest.main()
