import os
import sys
import unittest
from datetime import date, timedelta
from types import SimpleNamespace
from unittest.mock import MagicMock

import pandas as pd

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from strategies.krx_us_core4 import (
    CORE4_CODES,
    CORE4_NAMES,
    CORE4_WEIGHTS,
    INCOMPLETE_STATE_CASH_THRESHOLD,
    ZSCORE_WINDOW,
    build_selection_snapshot,
    build_runtime_selection,
    build_stage_target_weights,
    compute_zscore,
    get_signal_closes,
    get_target_weights,
    map_zscore_to_stage,
    needs_cash_correction,
    resolve_applied_stage,
)


class _FakeBar:
    def __init__(self, day: date, close: float):
        self.time = pd.Timestamp(day).to_pydatetime()
        self.close = close


class TestKrxUsCore4Strategy(unittest.TestCase):
    def setUp(self):
        self.df_codes = pd.DataFrame(
            [
                {"단축코드": "418660", "한글명": "TIGER 미국나스닥100레버리지(합성)"},
                {"단축코드": "390390", "한글명": "KODEX 미국반도체MV"},
                {"단축코드": "476760", "한글명": "ACE 미국30년국채액티브(H)"},
                {"단축코드": "411060", "한글명": "ACE KRX금현물"},
                {"단축코드": "005930", "한글명": "삼성전자"},
            ]
        )

    def test_build_selection_snapshot_contains_exact_four_symbols(self):
        df_selected, df_snapshot = build_selection_snapshot(
            self.df_codes,
            {},
            MagicMock(),
            4,
        )

        self.assertEqual(set(df_selected["단축코드"]), set(CORE4_WEIGHTS))
        self.assertEqual(set(df_snapshot["단축코드"]), set(CORE4_WEIGHTS))
        self.assertEqual(len(df_snapshot), 4)

    def test_get_target_weights_returns_expected_fixed_weights(self):
        _, df_snapshot = build_selection_snapshot(
            self.df_codes,
            {},
            MagicMock(),
            4,
        )

        weights = get_target_weights(df_snapshot)

        self.assertIsNotNone(weights)
        assert weights is not None
        self.assertEqual(weights, CORE4_WEIGHTS)
        self.assertAlmostEqual(sum(weights.values()), 1.0)

    def test_build_stage_target_weights_for_stage_two(self):
        weights = build_stage_target_weights(2)
        self.assertAlmostEqual(weights["418660"], 0.42)
        self.assertAlmostEqual(weights["390390"], 0.28)
        self.assertAlmostEqual(weights["476760"], 0.18)
        self.assertAlmostEqual(weights["411060"], 0.12)
        self.assertAlmostEqual(sum(weights.values()), 1.0)

    def test_build_runtime_selection_uses_core4_constants(self):
        df_selection = build_runtime_selection()
        self.assertEqual(df_selection["단축코드"].tolist(), CORE4_CODES)
        self.assertEqual(df_selection["한글명"].tolist(), [CORE4_NAMES[code] for code in CORE4_CODES])
        self.assertEqual(df_selection["target_weight"].tolist(), [CORE4_WEIGHTS[code] for code in CORE4_CODES])

    def test_needs_cash_correction_detects_excess_cash(self):
        self.assertTrue(needs_cash_correction(1_000_000.0, 850_000.0))

    def test_needs_cash_correction_allows_intended_cash_buffer(self):
        stock_value = 1_000_000.0 - INCOMPLETE_STATE_CASH_THRESHOLD + 1.0
        self.assertFalse(needs_cash_correction(1_000_000.0, stock_value))

    def test_map_zscore_to_stage_boundaries(self):
        self.assertEqual(map_zscore_to_stage(0.0), 0)
        self.assertEqual(map_zscore_to_stage(-0.1), 1)
        self.assertEqual(map_zscore_to_stage(-1.0), 1)
        self.assertEqual(map_zscore_to_stage(-1.1), 2)
        self.assertEqual(map_zscore_to_stage(-3.5), 4)
        self.assertEqual(map_zscore_to_stage(-4.1), 5)

    def test_resolve_applied_stage_tracks_bucket_directly(self):
        self.assertEqual(resolve_applied_stage(0, 3), 3)
        self.assertEqual(resolve_applied_stage(2, 1), 1)
        self.assertEqual(resolve_applied_stage(4, 5), 5)
        self.assertEqual(resolve_applied_stage(4, 0), 0)

    def test_compute_zscore_returns_series(self):
        closes = pd.Series([100 + idx for idx in range(ZSCORE_WINDOW + 20)], dtype=float)
        zscore = compute_zscore(closes)
        self.assertEqual(len(zscore), len(closes))
        self.assertTrue(pd.notna(zscore.iloc[-1]))

    def test_get_signal_closes_uses_completed_bars_only(self):
        bars = [
            _FakeBar(date(2025, 12, 20) + timedelta(days=idx), 100 + idx)
            for idx in range(ZSCORE_WINDOW + 31)
        ]
        bars.append(_FakeBar(date(2026, 3, 21), 999.0))

        chart = SimpleNamespace(bars=bars)
        stock = MagicMock()
        stock.daily_chart.return_value = chart
        kis = MagicMock()
        kis.stock.return_value = stock

        closes = get_signal_closes(kis, base_date=date(2026, 3, 21), lookback_days=120)

        self.assertEqual(str(closes.index[-1]), "2026-03-20")
        self.assertNotEqual(closes.iloc[-1], 999.0)
