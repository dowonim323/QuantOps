import os
import shutil
import sys
import tempfile
import time
import unittest
from datetime import date, datetime
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import tools.scheduler_state as scheduler_state
from pipelines.trading_day_controller import run_trading_day_once
from tools.trading_profiles import AccountProfile


class TestTradingDayController(unittest.TestCase):
    def setUp(self):
        self.temp_dir = Path(tempfile.mkdtemp(prefix="trading-controller-"))
        scheduler_state.DB_DIR = self.temp_dir
        scheduler_state.DB_PATH = self.temp_dir / "controller_state.db"
        scheduler_state.LOCK_DIR = self.temp_dir / "locks"
        scheduler_state._db_initialized.clear()
        self.current_dt = datetime(2026, 4, 8, 8, 45, 0)
        self.accounts = [
            AccountProfile(
                account_id="krx_vmq",
                display_name="KRX VMQ Account",
                secret_filename="krx_vmq.json",
                strategy_id="krx_vmq",
            ),
        ]

    def tearDown(self):
        scheduler_state._db_initialized.clear()
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def test_outside_window_does_not_start_session(self):
        with self.assertLogs("pipelines.trading_day_controller", level="INFO") as logs, patch(
            "pipelines.trading_day_controller.get_enabled_accounts",
            return_value=self.accounts,
        ), patch(
            "pipelines.trading_day_controller.run_trading_session",
        ) as session_mock:
            result = run_trading_day_once(datetime(2026, 4, 8, 7, 0, 0))

        self.assertEqual(result, "outside_window")
        session_mock.assert_not_called()
        self.assertIn("outside launch window", "\n".join(logs.output))

    def test_launches_trading_session_once_per_day(self):
        with patch("pipelines.trading_day_controller.get_enabled_accounts", return_value=self.accounts), patch(
            "pipelines.trading_day_controller.get_strategy_definition",
            return_value=SimpleNamespace(requires_selection=True),
        ), patch(
            "pipelines.trading_day_controller.get_saved_selection_row_count",
            return_value=5,
        ), patch(
            "pipelines.trading_day_controller.send_notification",
        ), patch(
            "pipelines.trading_day_controller.run_trading_session",
            return_value={"krx_vmq": "completed"},
        ) as session_mock:
            first_result = run_trading_day_once(self.current_dt)
            second_result = run_trading_day_once(self.current_dt)

        state = scheduler_state.load_trading_day_state(self.current_dt.date(), account_id="krx_vmq")
        self.assertEqual(first_result, "completed")
        self.assertEqual(second_result, "completed")
        self.assertEqual(session_mock.call_count, 1)
        self.assertEqual(state["status"], "completed")
        self.assertEqual(state["launch_mode"], "normal")
        self.assertIsNone(state["launch_reason"])
        self.assertIsNotNone(state["last_heartbeat_at"])
        self.assertIsNotNone(state["session_started_at"])
        self.assertIsNotNone(state["session_finished_at"])

    def test_launches_without_saved_selection(self):
        with self.assertLogs("pipelines.trading_day_controller", level="INFO") as logs, patch(
            "pipelines.trading_day_controller.get_enabled_accounts",
            return_value=self.accounts,
        ), patch(
            "pipelines.trading_day_controller.get_strategy_definition",
            return_value=SimpleNamespace(requires_selection=True),
        ), patch(
            "pipelines.trading_day_controller.get_saved_selection_row_count",
            return_value=None,
        ), patch(
            "pipelines.trading_day_controller.run_trading_session",
            return_value={"krx_vmq": "completed"},
        ) as session_mock:
            result = run_trading_day_once(self.current_dt)

        state = scheduler_state.load_trading_day_state(self.current_dt.date(), account_id="krx_vmq")
        self.assertEqual(result, "completed")
        self.assertEqual(state["status"], "completed")
        self.assertEqual(state["launch_mode"], "degraded_sell_only")
        self.assertIn("Buy/rebalance paths may be skipped", state["launch_reason"])
        self.assertIsNotNone(state["last_heartbeat_at"])
        session_mock.assert_called_once()
        output = "\n".join(logs.output)
        self.assertIn("Trading day launch degraded", output)
        self.assertIn("Trading day session completed", output)

    def test_exposes_running_phase_and_heartbeat_during_session(self):
        observed_state: dict[str, object] = {}

        def fake_session() -> dict[str, str]:
            observed_state.update(
                scheduler_state.load_trading_day_state(self.current_dt.date(), account_id="krx_vmq"),
            )
            time.sleep(0.05)
            refreshed_state = scheduler_state.load_trading_day_state(
                self.current_dt.date(),
                account_id="krx_vmq",
            )
            observed_state["refreshed_heartbeat_at"] = refreshed_state["last_heartbeat_at"]
            return {"krx_vmq": "completed"}

        with patch("pipelines.trading_day_controller.HEARTBEAT_INTERVAL_SECONDS", 0.01), patch(
            "pipelines.trading_day_controller.get_enabled_accounts",
            return_value=self.accounts,
        ), patch(
            "pipelines.trading_day_controller.get_strategy_definition",
            return_value=SimpleNamespace(requires_selection=True),
        ), patch(
            "pipelines.trading_day_controller.get_saved_selection_row_count",
            return_value=5,
        ), patch(
            "pipelines.trading_day_controller.run_trading_session",
            side_effect=fake_session,
        ):
            result = run_trading_day_once(self.current_dt)

        final_state = scheduler_state.load_trading_day_state(self.current_dt.date(), account_id="krx_vmq")
        self.assertEqual(result, "completed")
        self.assertEqual(observed_state["status"], "running")
        self.assertEqual(observed_state["phase"], "running")
        self.assertEqual(observed_state["launch_mode"], "normal")
        self.assertIsNotNone(observed_state["last_heartbeat_at"])
        self.assertNotEqual(observed_state["last_heartbeat_at"], observed_state["refreshed_heartbeat_at"])
        self.assertEqual(final_state["status"], "completed")

    def test_blocks_restart_after_incomplete_session(self):
        scheduler_state.save_trading_day_state(
            self.current_dt.date(),
            account_id="krx_vmq",
            status="running",
            session_started_at="2026-04-08T08:30:00+09:00",
            phase="launching",
        )

        with self.assertLogs("pipelines.trading_day_controller", level="WARNING") as logs, patch(
            "pipelines.trading_day_controller.get_enabled_accounts",
            return_value=self.accounts,
        ), patch(
            "pipelines.trading_day_controller.send_notification",
        ) as notify_mock, patch("pipelines.trading_day_controller.run_trading_session") as session_mock:
            result = run_trading_day_once(self.current_dt)

        state = scheduler_state.load_trading_day_state(self.current_dt.date(), account_id="krx_vmq")
        self.assertEqual(result, "blocked")
        self.assertEqual(state["status"], "blocked")
        self.assertTrue(state["manual_review_required"])
        notify_mock.assert_called_once()
        session_mock.assert_not_called()
        self.assertIn("current-day session requires manual review", "\n".join(logs.output))

    def test_abnormal_session_result_requires_manual_review(self):
        with patch("pipelines.trading_day_controller.get_enabled_accounts", return_value=self.accounts), patch(
            "pipelines.trading_day_controller.get_strategy_definition",
            return_value=SimpleNamespace(requires_selection=True),
        ), patch(
            "pipelines.trading_day_controller.get_saved_selection_row_count",
            return_value=5,
        ), patch(
            "pipelines.trading_day_controller.send_notification",
        ) as notify_mock, patch(
            "pipelines.trading_day_controller.run_trading_session",
            return_value={"krx_vmq": "error"},
        ):
            result = run_trading_day_once(self.current_dt)

        state = scheduler_state.load_trading_day_state(self.current_dt.date(), account_id="krx_vmq")
        self.assertEqual(result, "blocked")
        self.assertEqual(state["status"], "blocked")
        self.assertTrue(state["manual_review_required"])
        notify_mock.assert_called_once()

    def test_holiday_session_result_remains_completed(self):
        with patch("pipelines.trading_day_controller.get_enabled_accounts", return_value=self.accounts), patch(
            "pipelines.trading_day_controller.get_strategy_definition",
            return_value=SimpleNamespace(requires_selection=True),
        ), patch(
            "pipelines.trading_day_controller.get_saved_selection_row_count",
            return_value=5,
        ), patch(
            "pipelines.trading_day_controller.send_notification",
        ) as notify_mock, patch(
            "pipelines.trading_day_controller.run_trading_session",
            return_value={"krx_vmq": "holiday"},
        ):
            result = run_trading_day_once(self.current_dt)

        state = scheduler_state.load_trading_day_state(self.current_dt.date(), account_id="krx_vmq")
        self.assertEqual(result, "completed")
        self.assertEqual(state["status"], "completed")
        self.assertFalse(state["manual_review_required"])
        notify_mock.assert_not_called()

    def test_unknown_session_result_requires_manual_review(self):
        with patch("pipelines.trading_day_controller.get_enabled_accounts", return_value=self.accounts), patch(
            "pipelines.trading_day_controller.get_strategy_definition",
            return_value=SimpleNamespace(requires_selection=True),
        ), patch(
            "pipelines.trading_day_controller.get_saved_selection_row_count",
            return_value=5,
        ), patch(
            "pipelines.trading_day_controller.send_notification",
        ) as notify_mock, patch(
            "pipelines.trading_day_controller.run_trading_session",
            return_value={"krx_vmq": "timeout"},
        ):
            result = run_trading_day_once(self.current_dt)

        state = scheduler_state.load_trading_day_state(self.current_dt.date(), account_id="krx_vmq")
        self.assertEqual(result, "blocked")
        self.assertEqual(state["status"], "blocked")
        self.assertTrue(state["manual_review_required"])
        self.assertIn("Unknown results: krx_vmq=timeout", state["error_text"])
        notify_mock.assert_called_once()

    def test_prior_day_manual_review_blocks_new_launch(self):
        scheduler_state.save_trading_day_state(
            datetime(2026, 4, 7, 10, 0, 0).date(),
            account_id="krx_vmq",
            status="blocked",
            phase="manual_review",
            manual_review_required=True,
            error_text="pending review",
        )

        with self.assertLogs("pipelines.trading_day_controller", level="WARNING") as logs, patch(
            "pipelines.trading_day_controller.get_enabled_accounts",
            return_value=self.accounts,
        ), patch(
            "pipelines.trading_day_controller.send_notification",
        ) as notify_mock, patch("pipelines.trading_day_controller.run_trading_session") as session_mock:
            result = run_trading_day_once(self.current_dt)

        state = scheduler_state.load_trading_day_state(self.current_dt.date(), account_id="krx_vmq")
        self.assertEqual(result, "blocked")
        self.assertEqual(state["status"], "waiting_for_review")
        self.assertFalse(state["manual_review_required"])
        self.assertIn("2026-04-07:krx_vmq", state["error_text"])
        notify_mock.assert_called_once()
        session_mock.assert_not_called()
        self.assertIn("unresolved prior manual reviews", "\n".join(logs.output))

    def test_clearing_prior_day_review_allows_new_launch(self):
        scheduler_state.save_trading_day_state(
            datetime(2026, 4, 7, 10, 0, 0).date(),
            account_id="krx_vmq",
            status="blocked",
            phase="manual_review",
            manual_review_required=True,
            error_text="pending review",
        )

        with patch("pipelines.trading_day_controller.get_enabled_accounts", return_value=self.accounts), patch(
            "pipelines.trading_day_controller.send_notification",
        ), patch("pipelines.trading_day_controller.run_trading_session") as first_session_mock:
            first_result = run_trading_day_once(self.current_dt)

        scheduler_state.clear_trading_day_manual_review(date(2026, 4, 7), account_id="krx_vmq")

        with patch("pipelines.trading_day_controller.get_enabled_accounts", return_value=self.accounts), patch(
            "pipelines.trading_day_controller.get_strategy_definition",
            return_value=SimpleNamespace(requires_selection=True),
        ), patch(
            "pipelines.trading_day_controller.get_saved_selection_row_count",
            return_value=5,
        ), patch(
            "pipelines.trading_day_controller.send_notification",
        ), patch(
            "pipelines.trading_day_controller.run_trading_session",
            return_value={"krx_vmq": "completed"},
        ) as second_session_mock:
            second_result = run_trading_day_once(datetime(2026, 4, 8, 8, 46, 0))

        state = scheduler_state.load_trading_day_state(date(2026, 4, 8), account_id="krx_vmq")
        self.assertEqual(first_result, "blocked")
        self.assertEqual(second_result, "completed")
        first_session_mock.assert_not_called()
        second_session_mock.assert_called_once()
        self.assertEqual(state["status"], "completed")


if __name__ == "__main__":
    unittest.main()
