import os
import sys
import unittest
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from tools.trading_utils import rebalance


class TestRebalance(unittest.TestCase):
    def _make_scope(self, *, halt: bool) -> MagicMock:
        scope = MagicMock()
        scope.quote.return_value = SimpleNamespace(halt=halt)
        return scope

    def _make_failing_scope(self) -> MagicMock:
        scope = MagicMock()
        scope.quote.side_effect = RuntimeError("quote unavailable")
        return scope

    def _make_flaky_scope(self, *results: object) -> MagicMock:
        scope = MagicMock()
        scope.quote.side_effect = list(results)
        return scope

    def _make_holding(
        self,
        symbol: str,
        *,
        amount: float,
        qty: int,
        orderable: int,
    ) -> SimpleNamespace:
        return SimpleNamespace(
            symbol=symbol,
            amount=amount,
            qty=qty,
            orderable=orderable,
        )

    def test_rebalance_skips_halted_non_target_and_uses_remaining_budget(self):
        kis = MagicMock()
        kis.virtual = False

        stocks_selected = {
            "A": self._make_scope(halt=False),
            "B": self._make_scope(halt=False),
            "C": self._make_scope(halt=False),
        }
        halted_non_target = self._make_scope(halt=True)
        kis.stock.side_effect = lambda symbol: {"D": halted_non_target}[symbol]

        balance = SimpleNamespace(
            amount=110.0,
            stocks=[
                self._make_holding("A", amount=45.0, qty=45, orderable=45),
                self._make_holding("B", amount=20.0, qty=20, orderable=20),
                self._make_holding("C", amount=25.0, qty=25, orderable=25),
                self._make_holding("D", amount=20.0, qty=10, orderable=10),
            ],
        )

        with patch(
            "tools.trading_utils.get_balance_safe",
            side_effect=[balance, balance, balance],
        ), patch("tools.trading_utils.sell_qty", return_value=([], [])) as sell_qty_mock, patch(
            "tools.trading_utils.sell_value",
            return_value=([], []),
        ) as sell_value_mock, patch(
            "tools.trading_utils.buy_value",
            return_value=([], []),
        ) as buy_value_mock:
            result = rebalance(
                kis,
                stocks_selected,
                cash_ratio=0.0,
                max_retries=1,
            )

        self.assertEqual(result, {"orders": [], "errors": []})
        sell_qty_mock.assert_not_called()

        sell_candidates = sell_value_mock.call_args.kwargs["stocks"]
        self.assertEqual(set(sell_candidates), {"A"})
        self.assertAlmostEqual(sell_candidates["A"][1], 45.0)
        self.assertAlmostEqual(sell_candidates["A"][2], 30.0)

        buy_candidates = buy_value_mock.call_args.kwargs["stocks"]
        self.assertEqual(set(buy_candidates), {"B", "C"})
        self.assertAlmostEqual(buy_candidates["B"][1], 20.0)
        self.assertAlmostEqual(buy_candidates["B"][2], 30.0)
        self.assertAlmostEqual(buy_candidates["C"][1], 25.0)
        self.assertAlmostEqual(buy_candidates["C"][2], 30.0)

    def test_rebalance_fixes_halted_overweight_target_and_renormalizes_targets(self):
        kis = MagicMock()
        kis.virtual = False

        stocks_selected = {
            "A": self._make_scope(halt=True),
            "B": self._make_scope(halt=False),
            "C": self._make_scope(halt=False),
        }

        balance = SimpleNamespace(
            amount=90.0,
            stocks=[
                self._make_holding("A", amount=45.0, qty=45, orderable=45),
                self._make_holding("B", amount=20.0, qty=20, orderable=20),
                self._make_holding("C", amount=25.0, qty=25, orderable=25),
            ],
        )

        with patch(
            "tools.trading_utils.get_balance_safe",
            side_effect=[balance, balance, balance],
        ), patch("tools.trading_utils.sell_qty", return_value=([], [])), patch(
            "tools.trading_utils.sell_value",
            return_value=([], []),
        ) as sell_value_mock, patch(
            "tools.trading_utils.buy_value",
            return_value=([], []),
        ) as buy_value_mock:
            rebalance(
                kis,
                stocks_selected,
                cash_ratio=0.0,
                max_retries=1,
            )

        sell_candidates = sell_value_mock.call_args.kwargs["stocks"]
        self.assertEqual(set(sell_candidates), {"C"})
        self.assertAlmostEqual(sell_candidates["C"][1], 25.0)
        self.assertAlmostEqual(sell_candidates["C"][2], 22.5)

        buy_candidates = buy_value_mock.call_args.kwargs["stocks"]
        self.assertEqual(set(buy_candidates), {"B"})
        self.assertAlmostEqual(buy_candidates["B"][1], 20.0)
        self.assertAlmostEqual(buy_candidates["B"][2], 22.5)

    def test_rebalance_recomputes_custom_weight_targets_after_locked_capital(self):
        kis = MagicMock()
        kis.virtual = False

        stocks_selected = {
            "A": self._make_scope(halt=False),
            "B": self._make_scope(halt=False),
            "C": self._make_scope(halt=False),
        }
        halted_non_target = self._make_scope(halt=True)
        kis.stock.side_effect = lambda symbol: {"D": halted_non_target}[symbol]

        balance = SimpleNamespace(
            amount=120.0,
            stocks=[
                self._make_holding("A", amount=50.0, qty=50, orderable=50),
                self._make_holding("B", amount=20.0, qty=20, orderable=20),
                self._make_holding("C", amount=10.0, qty=10, orderable=10),
                self._make_holding("D", amount=20.0, qty=10, orderable=10),
            ],
        )

        with patch(
            "tools.trading_utils.get_balance_safe",
            side_effect=[balance, balance, balance],
        ), patch("tools.trading_utils.sell_qty", return_value=([], [])), patch(
            "tools.trading_utils.sell_value",
            return_value=([], []),
        ) as sell_value_mock, patch(
            "tools.trading_utils.buy_value",
            return_value=([], []),
        ) as buy_value_mock:
            rebalance(
                kis,
                stocks_selected,
                cash_ratio=0.0,
                target_weights={"A": 0.5, "B": 0.3, "C": 0.2},
                max_retries=1,
            )

        sell_value_mock.assert_not_called()
        buy_candidates = buy_value_mock.call_args.kwargs["stocks"]
        self.assertEqual(set(buy_candidates), {"B", "C"})
        self.assertAlmostEqual(buy_candidates["B"][1], 20.0)
        self.assertAlmostEqual(buy_candidates["B"][2], 30.0)
        self.assertAlmostEqual(buy_candidates["C"][1], 10.0)
        self.assertAlmostEqual(buy_candidates["C"][2], 20.0)

    def test_rebalance_fixes_unsellable_target_that_becomes_overweight_after_locking(self):
        kis = MagicMock()
        kis.virtual = False

        stocks_selected = {
            "A": self._make_scope(halt=False),
            "B": self._make_scope(halt=False),
        }
        halted_non_target = self._make_scope(halt=True)
        kis.stock.side_effect = lambda symbol: {"D": halted_non_target}[symbol]

        balance = SimpleNamespace(
            amount=100.0,
            stocks=[
                self._make_holding("A", amount=45.0, qty=45, orderable=0),
                self._make_holding("B", amount=35.0, qty=35, orderable=35),
                self._make_holding("D", amount=20.0, qty=10, orderable=10),
            ],
        )

        with patch(
            "tools.trading_utils.get_balance_safe",
            side_effect=[balance, balance, balance],
        ), patch("tools.trading_utils.sell_qty", return_value=([], [])), patch(
            "tools.trading_utils.sell_value",
            return_value=([], []),
        ) as sell_value_mock, patch(
            "tools.trading_utils.buy_value",
            return_value=([], []),
        ) as buy_value_mock:
            rebalance(
                kis,
                stocks_selected,
                cash_ratio=0.0,
                max_retries=1,
            )

        sell_value_mock.assert_not_called()
        buy_value_mock.assert_not_called()

    def test_rebalance_leaves_cash_when_only_zero_weight_targets_remain_adjustable(self):
        kis = MagicMock()
        kis.virtual = False

        stocks_selected = {
            "A": self._make_scope(halt=True),
            "B": self._make_scope(halt=True),
            "C": self._make_scope(halt=False),
            "D": self._make_scope(halt=False),
        }

        balance = SimpleNamespace(
            amount=100.0,
            stocks=[
                self._make_holding("A", amount=60.0, qty=60, orderable=60),
                self._make_holding("B", amount=20.0, qty=20, orderable=20),
            ],
        )

        with patch(
            "tools.trading_utils.get_balance_safe",
            side_effect=[balance, balance, balance],
        ), patch("tools.trading_utils.sell_qty", return_value=([], [])), patch(
            "tools.trading_utils.sell_value",
            return_value=([], []),
        ) as sell_value_mock, patch(
            "tools.trading_utils.buy_value",
            return_value=([], []),
        ) as buy_value_mock:
            rebalance(
                kis,
                stocks_selected,
                cash_ratio=0.0,
                target_weights={"A": 0.6, "B": 0.4, "C": 0.0, "D": 0.0},
                max_retries=1,
            )

        sell_value_mock.assert_not_called()
        buy_value_mock.assert_not_called()

    def test_rebalance_treats_quote_failure_as_non_tradable(self):
        kis = MagicMock()
        kis.virtual = False

        stocks_selected = {
            "A": self._make_failing_scope(),
            "B": self._make_scope(halt=False),
            "C": self._make_scope(halt=False),
        }
        unknown_non_target = self._make_failing_scope()
        kis.stock.side_effect = lambda symbol: {"D": unknown_non_target}[symbol]

        balance = SimpleNamespace(
            amount=110.0,
            stocks=[
                self._make_holding("A", amount=45.0, qty=45, orderable=45),
                self._make_holding("B", amount=20.0, qty=20, orderable=20),
                self._make_holding("C", amount=25.0, qty=25, orderable=25),
                self._make_holding("D", amount=20.0, qty=10, orderable=10),
            ],
        )

        with patch(
            "tools.trading_utils.get_balance_safe",
            side_effect=[balance, balance, balance],
        ), patch("tools.trading_utils.sell_qty", return_value=([], [])) as sell_qty_mock, patch(
            "tools.trading_utils.sell_value",
            return_value=([], []),
        ) as sell_value_mock, patch(
            "tools.trading_utils.buy_value",
            return_value=([], []),
        ) as buy_value_mock:
            rebalance(
                kis,
                stocks_selected,
                cash_ratio=0.0,
                max_retries=1,
            )

        sell_qty_mock.assert_not_called()

        sell_candidates = sell_value_mock.call_args.kwargs["stocks"]
        self.assertEqual(set(sell_candidates), {"C"})
        self.assertAlmostEqual(sell_candidates["C"][2], 22.5)

        buy_candidates = buy_value_mock.call_args.kwargs["stocks"]
        self.assertEqual(set(buy_candidates), {"B"})
        self.assertAlmostEqual(buy_candidates["B"][2], 22.5)

    def test_rebalance_retries_transient_quote_failure_before_buying_target(self):
        kis = MagicMock()
        kis.virtual = False

        flaky_scope = self._make_flaky_scope(
            RuntimeError("quote unavailable"),
            RuntimeError("quote unavailable"),
            SimpleNamespace(halt=False),
        )
        stocks_selected = {
            "A": flaky_scope,
            "B": self._make_scope(halt=False),
            "C": self._make_scope(halt=False),
        }

        balance = SimpleNamespace(
            amount=90.0,
            stocks=[
                self._make_holding("B", amount=30.0, qty=30, orderable=30),
                self._make_holding("C", amount=30.0, qty=30, orderable=30),
            ],
        )

        with patch(
            "tools.trading_utils.get_balance_safe",
            side_effect=[balance, balance, balance],
        ), patch("tools.trading_utils.sell_qty", return_value=([], [])), patch(
            "tools.trading_utils.sell_value",
            return_value=([], []),
        ) as sell_value_mock, patch(
            "tools.trading_utils.buy_value",
            return_value=([], []),
        ) as buy_value_mock:
            rebalance(
                kis,
                stocks_selected,
                cash_ratio=0.0,
                max_retries=1,
            )

        sell_value_mock.assert_not_called()
        self.assertEqual(flaky_scope.quote.call_count, 3)

        buy_candidates = buy_value_mock.call_args.kwargs["stocks"]
        self.assertEqual(set(buy_candidates), {"A"})
        self.assertAlmostEqual(buy_candidates["A"][1], 0.0)
        self.assertAlmostEqual(buy_candidates["A"][2], 30.0)

    def test_rebalance_retries_cold_stock_scope_resolution_for_non_target(self):
        kis = MagicMock()
        kis.virtual = False

        stocks_selected = {
            "A": self._make_scope(halt=False),
            "B": self._make_scope(halt=False),
            "C": self._make_scope(halt=False),
        }
        non_target_scope = self._make_scope(halt=False)

        stock_results = iter(
            [
                RuntimeError("stock unavailable"),
                RuntimeError("stock unavailable"),
                non_target_scope,
            ]
        )

        def _stock_side_effect(symbol: str):
            result = next(stock_results)
            if isinstance(result, Exception):
                raise result
            return result

        kis.stock.side_effect = _stock_side_effect

        balance = SimpleNamespace(
            amount=110.0,
            stocks=[
                self._make_holding("A", amount=30.0, qty=30, orderable=30),
                self._make_holding("B", amount=30.0, qty=30, orderable=30),
                self._make_holding("C", amount=30.0, qty=30, orderable=30),
                self._make_holding("D", amount=20.0, qty=10, orderable=10),
            ],
        )

        with patch(
            "tools.trading_utils.get_balance_safe",
            side_effect=[balance, balance, balance],
        ), patch("tools.trading_utils.sell_qty", return_value=([], [])) as sell_qty_mock, patch(
            "tools.trading_utils.sell_value",
            return_value=([], []),
        ), patch(
            "tools.trading_utils.buy_value",
            return_value=([], []),
        ):
            rebalance(
                kis,
                stocks_selected,
                cash_ratio=0.0,
                max_retries=1,
            )

        self.assertEqual(kis.stock.call_count, 3)
        sell_candidates = sell_qty_mock.call_args.kwargs["stocks"]
        self.assertEqual(set(sell_candidates), {"D"})


if __name__ == "__main__":
    unittest.main()
