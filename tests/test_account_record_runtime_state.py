import os
import sys
import unittest
from pathlib import Path

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from tools.account_record import load_strategy_runtime_state, save_strategy_runtime_state


class TestAccountRecordRuntimeState(unittest.TestCase):
    def setUp(self):
        self.account_id = f"test_runtime_state_{self._testMethodName}"
        self.db_path = Path(__file__).resolve().parent.parent / "db" / "account" / f"daily_assets_{self.account_id}.db"
        if self.db_path.exists():
            self.db_path.unlink()

    def tearDown(self):
        if self.db_path.exists():
            self.db_path.unlink()

    def test_defaults_to_stage_zero(self):
        state = load_strategy_runtime_state("krx_us_core4", account_id=self.account_id)
        self.assertEqual(state["stage"], 0)
        self.assertIsNone(state["last_signal_date"])
        self.assertIsNone(state["last_rsi"])
        self.assertIsNone(state["last_rebalance_date"])

    def test_save_and_load_runtime_state(self):
        save_strategy_runtime_state(
            "krx_us_core4",
            3,
            account_id=self.account_id,
            last_signal_date="2026-03-20",
            last_rsi=28.5,
            last_rebalance_date="2026-03-18",
        )

        state = load_strategy_runtime_state("krx_us_core4", account_id=self.account_id)
        self.assertEqual(state["stage"], 3)
        self.assertEqual(state["last_signal_date"], "2026-03-20")
        self.assertEqual(state["last_rsi"], 28.5)
        self.assertEqual(state["last_rebalance_date"], "2026-03-18")
