import os
import sys
import unittest
from datetime import date
from unittest.mock import ANY, MagicMock, patch

import pandas as pd

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pipelines.trading_session import _warn_if_today_selection_missing
from strategies.base import StrategyRuntimeContext
from strategies.krx_vmq import _load_saved_selection_or_warn
from tools.trading_profiles import AccountProfile, StrategyProfile

EMPTY_SELECTION_COLUMNS = pd.Index(["단축코드", "한글명"])


class TestStartupSelectionWarnings(unittest.TestCase):
    def setUp(self):
        self.account = AccountProfile(
            account_id="krx_vmq",
            display_name="KRX VMQ Account",
            secret_filename="krx_vmq.json",
            strategy_id="krx_vmq",
        )
        self.account_logger = MagicMock()
        self.kis = MagicMock()

    def test_warn_if_today_selection_missing_sends_notification(self):
        with patch("pipelines.trading_session.today_kst", return_value=date(2026, 4, 8)), patch(
            "pipelines.trading_session.load_stock_selection",
            side_effect=KeyError("missing selection"),
        ), patch("pipelines.trading_session._notify") as notify_mock:
            _warn_if_today_selection_missing(
                self.account,
                requires_selection=True,
                selection_top_n=20,
                kis=self.kis,
                account_logger=self.account_logger,
            )

        notify_mock.assert_called_once()
        _, kwargs = notify_mock.call_args
        self.assertEqual(kwargs["title"], "Today's Selection Missing")
        self.assertEqual(kwargs["tags"], ("warning",))

    def test_warn_if_today_selection_empty_sends_notification(self):
        with patch("pipelines.trading_session.today_kst", return_value=date(2026, 4, 8)), patch(
            "pipelines.trading_session.load_stock_selection",
            return_value=pd.DataFrame(columns=EMPTY_SELECTION_COLUMNS),
        ), patch("pipelines.trading_session._notify") as notify_mock:
            _warn_if_today_selection_missing(
                self.account,
                requires_selection=True,
                selection_top_n=20,
                kis=self.kis,
                account_logger=self.account_logger,
            )

        notify_mock.assert_called_once()
        _, kwargs = notify_mock.call_args
        self.assertEqual(kwargs["title"], "Today's Selection Empty")
        self.assertEqual(kwargs["tags"], ("warning",))

    def test_warn_if_today_selection_missing_skips_non_selection_strategy(self):
        with patch("pipelines.trading_session.load_stock_selection") as load_mock, patch(
            "pipelines.trading_session._notify",
        ) as notify_mock:
            _warn_if_today_selection_missing(
                self.account,
                requires_selection=False,
                selection_top_n=20,
                kis=self.kis,
                account_logger=self.account_logger,
            )

        load_mock.assert_not_called()
        notify_mock.assert_not_called()


class TestRebalanceSelectionWarnings(unittest.TestCase):
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
        self.notify_mock = MagicMock()
        self.context = StrategyRuntimeContext(
            kis=MagicMock(),
            account=self.account,
            strategy_profile=self.strategy_profile,
            account_logger=MagicMock(),
            monitor=MagicMock(),
            initial_asset=1_000_000.0,
            order_timeout=600.0,
            execution_timeout=600.0,
            market_check_timeout=180,
            market_wait_timeout=180,
            max_daily_trades=3,
            trade_interval_seconds=3600,
            notify=self.notify_mock,
        )
        self.selection_date = date(2026, 4, 8)
        self.warned_events: set[tuple[str, str]] = set()

    def test_load_saved_selection_or_warn_notifies_when_selection_missing(self):
        with patch(
            "strategies.krx_vmq.load_stock_selection",
            side_effect=KeyError("missing selection"),
        ):
            result = _load_saved_selection_or_warn(
                self.context,
                trigger="scheduled weekly rebalance",
                selection_date=self.selection_date,
                warned_events=self.warned_events,
            )

        self.assertTrue(result.empty)
        self.notify_mock.assert_called_once_with(
            ANY,
            "Saved Selection Missing",
            ("warning",),
        )

    def test_load_saved_selection_or_warn_notifies_when_selection_empty(self):
        with patch(
            "strategies.krx_vmq.load_stock_selection",
            return_value=pd.DataFrame(columns=EMPTY_SELECTION_COLUMNS),
        ):
            result = _load_saved_selection_or_warn(
                self.context,
                trigger="cash-to-stock rebalance",
                selection_date=self.selection_date,
                warned_events=self.warned_events,
            )

        self.assertTrue(result.empty)
        self.notify_mock.assert_called_once_with(
            ANY,
            "Saved Selection Unavailable",
            ("warning",),
        )

    def test_load_saved_selection_or_warn_uses_today_selection_date(self):
        snapshot = pd.DataFrame([
            {"단축코드": "005930", "한글명": "삼성전자"},
        ])

        with patch(
            "strategies.krx_vmq.load_stock_selection",
            return_value=snapshot,
        ) as load_mock:
            _load_saved_selection_or_warn(
                self.context,
                trigger="cash-to-stock rebalance",
                selection_date=self.selection_date,
                warned_events=self.warned_events,
            )

        _, kwargs = load_mock.call_args
        self.assertEqual(kwargs["table_date"], self.selection_date)

    def test_load_saved_selection_or_warn_deduplicates_same_warning(self):
        with patch(
            "strategies.krx_vmq.load_stock_selection",
            side_effect=KeyError("missing selection"),
        ):
            _load_saved_selection_or_warn(
                self.context,
                trigger="cash-to-stock rebalance",
                selection_date=self.selection_date,
                warned_events=self.warned_events,
            )
            _load_saved_selection_or_warn(
                self.context,
                trigger="cash-to-stock rebalance",
                selection_date=self.selection_date,
                warned_events=self.warned_events,
            )

        self.notify_mock.assert_called_once()

    def test_load_saved_selection_or_warn_returns_selection_without_warning(self):
        snapshot = pd.DataFrame([
            {"단축코드": "005930", "한글명": "삼성전자"},
        ])

        with patch(
            "strategies.krx_vmq.load_stock_selection",
            return_value=snapshot,
        ):
            result = _load_saved_selection_or_warn(
                self.context,
                trigger="cash-to-stock rebalance",
                selection_date=self.selection_date,
                warned_events=self.warned_events,
            )

        self.assertEqual(result["단축코드"].tolist(), ["005930"])
        self.notify_mock.assert_not_called()


if __name__ == "__main__":
    unittest.main()
