import os
import sys
import unittest
from types import SimpleNamespace
from unittest.mock import ANY, MagicMock, patch

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pipelines.trading_session import (
    _load_or_capture_initial_asset,
    _load_or_capture_opening_asset,
    finalize_trading_day,
    run_account,
)
from tools.trading_profiles import AccountProfile, StrategyProfile


class TestTradingSessionStartup(unittest.TestCase):
    def setUp(self):
        self.account = AccountProfile(
            account_id="krx_vmq",
            display_name="KRX VMQ Account",
            secret_filename="krx_vmq.json",
            strategy_id="krx_vmq",
        )
        self.strategy_profile = StrategyProfile(
            strategy_id="krx_vmq",
            display_name="KRX VMQ",
            selection_top_n=20,
            cash_ratio=0.03,
        )
        self.account_logger = MagicMock()

    def test_load_or_capture_initial_asset_uses_saved_daily_snapshot(self):
        kis = MagicMock()

        with patch(
            "pipelines.trading_session.get_daily_asset",
            return_value=(1_250_000.0, None, 50_000.0),
        ), patch("pipelines.trading_session.get_balance_safe") as balance_mock:
            initial_asset, transfer_amount = _load_or_capture_initial_asset(
                self.account,
                kis,
                account_logger=self.account_logger,
            )

        self.assertEqual(initial_asset, 1_250_000.0)
        self.assertEqual(transfer_amount, 50_000.0)
        balance_mock.assert_not_called()

    def test_load_or_capture_initial_asset_captures_preopen_balance_and_transfer(self):
        kis = MagicMock()
        balance = SimpleNamespace(
            total=1_200_000.0,
            deposits={"KRW": SimpleNamespace(d2_amount=250_000.0)},
            stocks=[],
        )

        with patch(
            "pipelines.trading_session.get_daily_asset",
            return_value=(None, None, 0.0),
        ), patch(
            "pipelines.trading_session.get_balance_safe",
            return_value=balance,
        ) as balance_mock, patch(
            "pipelines.trading_session.get_previous_final_asset",
            return_value=(1_100_000.0, 180_000.0),
        ), patch("pipelines.trading_session.save_initial_asset") as save_mock:
            initial_asset, transfer_amount = _load_or_capture_initial_asset(
                self.account,
                kis,
                account_logger=self.account_logger,
            )

        self.assertEqual(initial_asset, 1_200_000.0)
        self.assertEqual(transfer_amount, 70_000.0)
        balance_mock.assert_called_once_with(kis.account(), verbose=True)
        save_mock.assert_called_once_with(
            1_200_000.0,
            250_000.0,
            70_000.0,
            account_id=self.account.account_id,
        )

    def test_load_or_capture_opening_asset_uses_saved_market_open_snapshot(self):
        kis = MagicMock()

        with patch(
            "pipelines.trading_session.get_opening_asset",
            return_value=1_240_000.0,
        ), patch("pipelines.trading_session.get_balance_safe") as balance_mock:
            opening_asset = _load_or_capture_opening_asset(
                self.account,
                kis,
                1_200_000.0,
                account_logger=self.account_logger,
            )

        self.assertEqual(opening_asset, 1_240_000.0)
        balance_mock.assert_not_called()

    def test_load_or_capture_opening_asset_captures_after_market_open(self):
        kis = MagicMock()
        balance = SimpleNamespace(total=1_240_000.0, deposits={}, stocks=[])

        with patch(
            "pipelines.trading_session.get_opening_asset",
            return_value=None,
        ), patch(
            "pipelines.trading_session.get_balance_safe",
            return_value=balance,
        ) as balance_mock, patch("pipelines.trading_session.save_opening_asset") as save_mock:
            opening_asset = _load_or_capture_opening_asset(
                self.account,
                kis,
                1_200_000.0,
                account_logger=self.account_logger,
            )

        self.assertEqual(opening_asset, 1_240_000.0)
        balance_mock.assert_called_once_with(kis.account(), verbose=True)
        save_mock.assert_called_once_with(
            1_240_000.0,
            account_id=self.account.account_id,
        )

    def test_run_account_captures_initial_asset_before_market_open_wait(self):
        kis = MagicMock()
        kis.websocket.connected = True
        kis.websocket.on_domestic_index_price.side_effect = [MagicMock(), MagicMock()]
        strategy_def = SimpleNamespace(
            requires_selection=True,
            run_trading_day=MagicMock(side_effect=KeyboardInterrupt),
        )
        monitor_mock = MagicMock()
        call_order: list[str] = []
        market_open_states = iter([True])
        notify_mock = MagicMock()

        def capture_initial_asset(*args, **kwargs):
            del args, kwargs
            call_order.append("capture_initial_asset")
            return 1_000_000.0, 25_000.0

        def capture_opening_asset(*args, **kwargs):
            del args, kwargs
            call_order.append("capture_opening_asset")
            return 1_030_000.0

        def notify(*args, **kwargs):
            call_order.append("notify_market_open")
            return notify_mock(*args, **kwargs)

        def run_trading_day(*args, **kwargs):
            del args, kwargs
            call_order.append("run_strategy")
            raise KeyboardInterrupt

        def monitor_is_active(*args, **kwargs):
            del args, kwargs
            call_order.append("wait_for_market_open")
            return next(market_open_states)

        monitor_mock.is_active.side_effect = monitor_is_active
        strategy_def.run_trading_day.side_effect = run_trading_day

        with patch("pipelines.trading_session.get_strategy_profile", return_value=self.strategy_profile), patch(
            "pipelines.trading_session.get_strategy_definition",
            return_value=strategy_def,
        ), patch("pipelines.trading_session.resolve_secret_path") as secret_path_mock, patch(
            "pipelines.trading_session.KisAuth.load",
            return_value=MagicMock(),
        ), patch("pipelines.trading_session.PyKis", return_value=kis), patch(
            "pipelines.trading_session.is_today_open_day",
            return_value=True,
        ), patch("pipelines.trading_session._load_or_capture_initial_asset", side_effect=capture_initial_asset), patch(
            "pipelines.trading_session._load_or_capture_opening_asset",
            side_effect=capture_opening_asset,
        ), patch(
            "pipelines.trading_session.MarketMonitor",
            return_value=monitor_mock,
        ), patch("pipelines.trading_session._notify", side_effect=notify), patch(
            "pipelines.trading_session._warn_if_today_selection_missing",
        ), patch("pipelines.trading_session.wait_until_market_close"), patch(
            "pipelines.trading_session.finalize_trading_day",
        ), patch("pipelines.trading_session.time.sleep"):
            secret_path = MagicMock()
            secret_path.exists.return_value = True
            secret_path_mock.return_value = secret_path

            run_account(self.account)

        self.assertEqual(
            call_order,
            [
                "capture_initial_asset",
                "wait_for_market_open",
                "capture_opening_asset",
                "notify_market_open",
                "run_strategy",
            ],
        )

    def test_run_account_market_open_notification_includes_opening_asset_and_gap(self):
        kis = MagicMock()
        kis.websocket.connected = True
        kis.websocket.on_domestic_index_price.side_effect = [MagicMock(), MagicMock()]
        strategy_def = SimpleNamespace(
            requires_selection=True,
            run_trading_day=MagicMock(side_effect=KeyboardInterrupt),
        )
        monitor_mock = MagicMock()
        monitor_mock.is_active.side_effect = [True]

        with patch("pipelines.trading_session.get_strategy_profile", return_value=self.strategy_profile), patch(
            "pipelines.trading_session.get_strategy_definition",
            return_value=strategy_def,
        ), patch("pipelines.trading_session.resolve_secret_path") as secret_path_mock, patch(
            "pipelines.trading_session.KisAuth.load",
            return_value=MagicMock(),
        ), patch("pipelines.trading_session.PyKis", return_value=kis), patch(
            "pipelines.trading_session.is_today_open_day",
            return_value=True,
        ), patch(
            "pipelines.trading_session._load_or_capture_initial_asset",
            return_value=(1_000_000.0, 25_000.0),
        ), patch(
            "pipelines.trading_session._load_or_capture_opening_asset",
            return_value=1_030_000.0,
        ), patch("pipelines.trading_session.MarketMonitor", return_value=monitor_mock), patch(
            "pipelines.trading_session._notify",
        ) as notify_mock, patch("pipelines.trading_session._warn_if_today_selection_missing"), patch(
            "pipelines.trading_session.wait_until_market_close",
        ), patch("pipelines.trading_session.finalize_trading_day"), patch(
            "pipelines.trading_session.time.sleep",
        ):
            secret_path = MagicMock()
            secret_path.exists.return_value = True
            secret_path_mock.return_value = secret_path

            run_account(self.account)

        market_open_message = notify_mock.call_args.args[1]
        self.assertIn("Initial Asset: 1,000,000 KRW", market_open_message)
        self.assertIn("Opening Asset: 1,030,000 KRW", market_open_message)
        self.assertIn("Gap at Open: +30,000 (+3.00%)", market_open_message)
        self.assertIn("(Net Transfer: +25,000)", market_open_message)

    def test_run_account_does_not_finalize_before_market_open(self):
        kis = MagicMock()
        kis.websocket.connected = True
        kis.websocket.on_domestic_index_price.side_effect = [MagicMock(), MagicMock()]
        strategy_def = SimpleNamespace(
            requires_selection=True,
            run_trading_day=MagicMock(),
        )
        monitor_mock = MagicMock()
        monitor_mock.is_active.side_effect = KeyboardInterrupt()

        with patch("pipelines.trading_session.get_strategy_profile", return_value=self.strategy_profile), patch(
            "pipelines.trading_session.get_strategy_definition",
            return_value=strategy_def,
        ), patch("pipelines.trading_session.resolve_secret_path") as secret_path_mock, patch(
            "pipelines.trading_session.KisAuth.load",
            return_value=MagicMock(),
        ), patch("pipelines.trading_session.PyKis", return_value=kis), patch(
            "pipelines.trading_session.is_today_open_day",
            return_value=True,
        ), patch(
            "pipelines.trading_session._load_or_capture_initial_asset",
            return_value=(1_000_000.0, 25_000.0),
        ), patch("pipelines.trading_session.MarketMonitor", return_value=monitor_mock), patch(
            "pipelines.trading_session._notify",
        ), patch("pipelines.trading_session._warn_if_today_selection_missing"), patch(
            "pipelines.trading_session.wait_until_market_close",
        ), patch("pipelines.trading_session.finalize_trading_day") as finalize_mock, patch(
            "pipelines.trading_session.time.sleep",
        ):
            secret_path = MagicMock()
            secret_path.exists.return_value = True
            secret_path_mock.return_value = secret_path

            run_account(self.account)

        finalize_mock.assert_not_called()

    def test_run_account_does_not_finalize_when_interrupted_after_open(self):
        kis = MagicMock()
        kis.websocket.connected = True
        kis.websocket.on_domestic_index_price.side_effect = [MagicMock(), MagicMock()]
        strategy_def = SimpleNamespace(
            requires_selection=True,
            run_trading_day=MagicMock(),
        )
        monitor_mock = MagicMock()
        monitor_mock.is_active.side_effect = [True]

        with patch("pipelines.trading_session.get_strategy_profile", return_value=self.strategy_profile), patch(
            "pipelines.trading_session.get_strategy_definition",
            return_value=strategy_def,
        ), patch("pipelines.trading_session.resolve_secret_path") as secret_path_mock, patch(
            "pipelines.trading_session.KisAuth.load",
            return_value=MagicMock(),
        ), patch("pipelines.trading_session.PyKis", return_value=kis), patch(
            "pipelines.trading_session.is_today_open_day",
            return_value=True,
        ), patch(
            "pipelines.trading_session._load_or_capture_initial_asset",
            return_value=(1_000_000.0, 25_000.0),
        ), patch(
            "pipelines.trading_session._load_or_capture_opening_asset",
            return_value=1_030_000.0,
        ), patch("pipelines.trading_session.MarketMonitor", return_value=monitor_mock), patch(
            "pipelines.trading_session._notify",
        ), patch("pipelines.trading_session._warn_if_today_selection_missing"), patch(
            "pipelines.trading_session.wait_until_market_close",
            side_effect=KeyboardInterrupt(),
        ), patch("pipelines.trading_session.finalize_trading_day") as finalize_mock, patch(
            "pipelines.trading_session.time.sleep",
        ):
            secret_path = MagicMock()
            secret_path.exists.return_value = True
            secret_path_mock.return_value = secret_path

            run_account(self.account)

        finalize_mock.assert_not_called()

    def test_run_account_passes_opening_asset_to_finalize(self):
        kis = MagicMock()
        kis.websocket.connected = True
        kis.websocket.on_domestic_index_price.side_effect = [MagicMock(), MagicMock()]
        strategy_def = SimpleNamespace(
            requires_selection=True,
            run_trading_day=MagicMock(),
        )
        monitor_mock = MagicMock()
        monitor_mock.is_active.side_effect = [True]

        with patch("pipelines.trading_session.get_strategy_profile", return_value=self.strategy_profile), patch(
            "pipelines.trading_session.get_strategy_definition",
            return_value=strategy_def,
        ), patch("pipelines.trading_session.resolve_secret_path") as secret_path_mock, patch(
            "pipelines.trading_session.KisAuth.load",
            return_value=MagicMock(),
        ), patch("pipelines.trading_session.PyKis", return_value=kis), patch(
            "pipelines.trading_session.is_today_open_day",
            return_value=True,
        ), patch(
            "pipelines.trading_session._load_or_capture_initial_asset",
            return_value=(1_000_000.0, 25_000.0),
        ), patch(
            "pipelines.trading_session._load_or_capture_opening_asset",
            return_value=1_030_000.0,
        ), patch("pipelines.trading_session.MarketMonitor", return_value=monitor_mock), patch(
            "pipelines.trading_session._notify",
        ), patch("pipelines.trading_session._warn_if_today_selection_missing"), patch(
            "pipelines.trading_session.wait_until_market_close",
        ), patch("pipelines.trading_session.finalize_trading_day") as finalize_mock, patch(
            "pipelines.trading_session.time.sleep",
        ):
            secret_path = MagicMock()
            secret_path.exists.return_value = True
            secret_path_mock.return_value = secret_path

            run_account(self.account)

        finalize_mock.assert_called_once_with(
            kis,
            1_000_000.0,
            1_030_000.0,
            25_000.0,
            account=self.account,
            account_logger=ANY,
        )

    def test_finalize_trading_day_uses_opening_asset_for_vs_open_and_prev_for_vs_prev(self):
        kis = MagicMock()
        balance = SimpleNamespace(
            total=1_080_000.0,
            deposits={"KRW": SimpleNamespace(d2_amount=300_000.0)},
            stocks=[],
        )
        kis.account().daily_orders.return_value = []
        kis.account().profits.return_value = SimpleNamespace(orders=[])

        with patch(
            "pipelines.trading_session.get_balance_safe",
            return_value=balance,
        ), patch(
            "pipelines.trading_session.save_final_asset",
        ), patch(
            "pipelines.trading_session.get_previous_final_asset",
            return_value=(1_000_000.0, 0.0),
        ), patch(
            "pipelines.trading_session.retry_execution",
            return_value=(True, []),
        ), patch(
            "pipelines.trading_session.get_latest_stock_performance",
            return_value={},
        ), patch(
            "pipelines.trading_session.save_stock_performance",
        ), patch(
            "pipelines.trading_session.save_daily_orders",
        ), patch("pipelines.trading_session._notify") as notify_mock:
            finalize_trading_day(
                kis,
                1_000_000.0,
                1_050_000.0,
                20_000.0,
                account=self.account,
                account_logger=self.account_logger,
            )

        report_message = notify_mock.call_args.args[1]
        self.assertIn("Vs Open: +30,000 (+2.86%)", report_message)
        self.assertIn("Vs Prev: +60,000 (+5.88%)", report_message)

    def test_finalize_trading_day_uses_zero_previous_asset_when_present(self):
        kis = MagicMock()
        balance = SimpleNamespace(
            total=1_080_000.0,
            deposits={"KRW": SimpleNamespace(d2_amount=300_000.0)},
            stocks=[],
        )
        kis.account().daily_orders.return_value = []
        kis.account().profits.return_value = SimpleNamespace(orders=[])

        with patch(
            "pipelines.trading_session.get_balance_safe",
            return_value=balance,
        ), patch(
            "pipelines.trading_session.save_final_asset",
        ), patch(
            "pipelines.trading_session.get_previous_final_asset",
            return_value=(0.0, 0.0),
        ), patch(
            "pipelines.trading_session.retry_execution",
            return_value=(True, []),
        ), patch(
            "pipelines.trading_session.get_latest_stock_performance",
            return_value={},
        ), patch(
            "pipelines.trading_session.save_stock_performance",
        ), patch(
            "pipelines.trading_session.save_daily_orders",
        ), patch("pipelines.trading_session._notify") as notify_mock:
            finalize_trading_day(
                kis,
                1_000_000.0,
                1_050_000.0,
                1_000_000.0,
                account=self.account,
                account_logger=self.account_logger,
            )

        report_message = notify_mock.call_args.args[1]
        self.assertIn("Vs Prev: +80,000 (+8.00%)", report_message)


if __name__ == "__main__":
    unittest.main()
