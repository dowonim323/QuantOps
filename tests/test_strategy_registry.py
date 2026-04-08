import os
import sys
import unittest

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from strategies.registry import get_strategy_definition


class TestStrategyRegistry(unittest.TestCase):
    def test_returns_krx_vmq_definition(self):
        strategy = get_strategy_definition("krx_vmq")
        self.assertEqual(strategy.strategy_id, "krx_vmq")
        self.assertEqual(strategy.rebalance_mode, "signal_loop")
        self.assertTrue(strategy.requires_selection)

    def test_returns_krx_us_core4_definition(self):
        strategy = get_strategy_definition("krx_us_core4")
        self.assertEqual(strategy.strategy_id, "krx_us_core4")
        self.assertEqual(strategy.rebalance_mode, "scheduled_once")
        self.assertFalse(strategy.requires_selection)

    def test_raises_for_unknown_strategy(self):
        with self.assertRaises(KeyError):
            get_strategy_definition("unknown")
