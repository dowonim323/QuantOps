import os
import sys
import unittest
from datetime import datetime, time as dt_time
from typing import TYPE_CHECKING, cast
from unittest.mock import patch

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from tools.market_watcher import wait_until_market_close

if TYPE_CHECKING:
    from pykis import PyKis


class TestMarketCloseDetection(unittest.TestCase):
    def test_wait_until_market_close_closes_on_single_clean_failure(self):
        kis = cast("PyKis", object())
        with patch("tools.market_watcher.check_market_open_by_indexes", return_value=False):
            closed_at = wait_until_market_close(
                kis=kis,
                timeout=1.0,
                poll_interval=0.0,
                verbose=False,
            )

        self.assertIsNotNone(closed_at)

    def test_wait_until_market_close_retries_on_exception_before_closing(self):
        kis = cast("PyKis", object())
        with patch(
            "tools.market_watcher.check_market_open_by_indexes",
            side_effect=[RuntimeError("dns"), RuntimeError("dns"), False],
        ):
            with patch("tools.market_watcher.time.sleep") as sleep_mock:
                closed_at = wait_until_market_close(
                    kis=kis,
                    timeout=1.0,
                    poll_interval=0.0,
                    max_error_retries=2,
                    verbose=False,
                )

        self.assertIsNotNone(closed_at)
        sleep_mock.assert_not_called()

    def test_wait_until_market_close_falls_back_to_default_close_time_after_exceeding_error_retries(self):
        kis = cast("PyKis", object())
        with patch(
            "tools.market_watcher.check_market_open_by_indexes",
            side_effect=[
                RuntimeError("dns"),
                RuntimeError("dns"),
                RuntimeError("dns"),
                RuntimeError("dns"),
                RuntimeError("dns"),
                RuntimeError("dns"),
            ],
        ):
            with patch("tools.market_watcher._default_market_close_datetime", return_value=datetime(2026, 3, 24, 15, 30, 0)):
                with patch("tools.market_watcher.time.sleep") as sleep_mock:
                    closed_at = wait_until_market_close(
                        kis=kis,
                        timeout=1.0,
                        poll_interval=0.0,
                        verbose=False,
                    )

        self.assertEqual(closed_at, datetime(2026, 3, 24, 15, 30, 0))
        sleep_mock.assert_not_called()

    def test_wait_until_market_close_uses_custom_default_close_time(self):
        kis = cast("PyKis", object())
        with patch(
            "tools.market_watcher.check_market_open_by_indexes",
            side_effect=[RuntimeError("dns"), RuntimeError("dns")],
        ):
            with patch("tools.market_watcher.datetime") as mock_datetime:
                mock_datetime.now.return_value = datetime(2026, 3, 24, 14, 0, 0)
                mock_datetime.side_effect = datetime
                with patch("tools.market_watcher.time.sleep") as sleep_mock:
                    closed_at = wait_until_market_close(
                        kis=kis,
                        timeout=1.0,
                        poll_interval=0.0,
                        max_error_retries=1,
                        default_close_time=dt_time(15, 30),
                        verbose=False,
                    )

        self.assertEqual(closed_at, datetime(2026, 3, 24, 15, 30, 0))
        sleep_mock.assert_called_once_with(5400.0)

    def test_wait_until_market_close_honors_custom_error_retry_count(self):
        kis = cast("PyKis", object())
        with patch(
            "tools.market_watcher.check_market_open_by_indexes",
            side_effect=[RuntimeError("dns"), False],
        ):
            closed_at = wait_until_market_close(
                kis=kis,
                timeout=1.0,
                poll_interval=0.0,
                max_error_retries=1,
                verbose=False,
            )

        self.assertIsNotNone(closed_at)
