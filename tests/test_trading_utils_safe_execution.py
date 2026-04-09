import os
import sys
import unittest
from unittest.mock import MagicMock, patch

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from tools.trading_utils import (
    _execute_with_retry,
    execute_rebalance_safe,
    execute_sell_all_safe,
)


class TestSafeTradeExecution(unittest.TestCase):
    def test_execute_with_retry_clears_recovered_retry_errors(self):
        kis = MagicMock()
        kis.virtual = False
        order = MagicMock()
        attempts = iter([
            ([], [{"type": "unfilled", "symbol": "005930"}]),
            ([order], []),
        ])

        def attempt_func():
            return next(attempts)

        with patch("tools.trading_utils.time.sleep"), patch(
            "tools.trading_utils._wait_until_filled",
            return_value=None,
        ):
            orders, errors = _execute_with_retry(
                kis=kis,
                max_retries=2,
                execution_timeout=10,
                verbose=False,
                dry_run=False,
                step_name="Step 1",
                attempt_func=attempt_func,
            )

        self.assertEqual(orders, [order])
        self.assertEqual(errors, [])

    def test_execute_rebalance_safe_returns_false_when_result_has_errors(self):
        kis = MagicMock()
        stocks_selected = {"005930": MagicMock()}

        with patch(
            "tools.trading_utils.rebalance",
            return_value={
                "orders": [MagicMock()],
                "errors": [{"type": "unfilled", "symbol": "005930"}],
            },
        ), patch("tools.trading_utils.send_notification") as notify_mock, self.assertLogs(
            "tools.trading_utils",
            level="WARNING",
        ) as captured:
            success = execute_rebalance_safe(
                kis,
                stocks_selected,
                check_alive=lambda: True,
                context="Scheduled rebalance",
            )

        self.assertFalse(success)
        self.assertIn("finished with warnings", "\n".join(captured.output))
        self.assertIn("error_types=unfilled=1", notify_mock.call_args.args[1])

    def test_execute_rebalance_safe_returns_true_when_no_orders_are_required(self):
        kis = MagicMock()
        stocks_selected = {"005930": MagicMock()}

        with patch(
            "tools.trading_utils.rebalance",
            return_value={"orders": [], "errors": []},
        ), patch("tools.trading_utils.send_notification") as notify_mock, self.assertLogs(
            "tools.trading_utils",
            level="INFO",
        ) as captured:
            success = execute_rebalance_safe(
                kis,
                stocks_selected,
                check_alive=lambda: True,
                context="Scheduled rebalance",
            )

        self.assertTrue(success)
        self.assertIn("no orders required", "\n".join(captured.output))
        self.assertIn("no orders required", notify_mock.call_args.args[1])

    def test_execute_sell_all_safe_returns_false_when_result_has_errors(self):
        kis = MagicMock()

        with patch(
            "tools.trading_utils.sell_all",
            return_value={
                "orders": [MagicMock()],
                "errors": [{"type": "unfilled", "symbol": "005930"}],
            },
        ), patch("tools.trading_utils.send_notification") as notify_mock, self.assertLogs(
            "tools.trading_utils",
            level="WARNING",
        ) as captured:
            success = execute_sell_all_safe(
                kis,
                check_alive=lambda: True,
                context="Opening sell",
            )

        self.assertFalse(success)
        self.assertIn("finished with warnings", "\n".join(captured.output))
        self.assertIn("error_types=unfilled=1", notify_mock.call_args.args[1])


if __name__ == "__main__":
    unittest.main()
