from __future__ import annotations

import unittest

from backtest_data_utils import MAX_BATCH_SYMBOLS, _split_symbol_batches


class BacktestDataUtilsTests(unittest.TestCase):
    def test_split_symbol_batches_chunks_at_databento_limit(self) -> None:
        symbols = [f"SYM{i}" for i in range(MAX_BATCH_SYMBOLS + 5)]

        batches = _split_symbol_batches(symbols)

        self.assertEqual(len(batches), 2)
        self.assertEqual(len(batches[0]), MAX_BATCH_SYMBOLS)
        self.assertEqual(len(batches[1]), 5)

    def test_split_symbol_batches_dedupes_before_chunking(self) -> None:
        symbols = ["A", "B", "A", "C", "B"]

        batches = _split_symbol_batches(symbols, max_batch_symbols=2)

        self.assertEqual(batches, [["A", "B"], ["C"]])


if __name__ == "__main__":
    unittest.main()
