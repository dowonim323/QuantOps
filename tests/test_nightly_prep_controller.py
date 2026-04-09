import os
import shutil
import sys
import tempfile
import unittest
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import tools.scheduler_state as scheduler_state
from pipelines.nightly_prep_controller import run_nightly_prep_once
from tools.trading_profiles import AccountProfile


class TestNightlyPrepController(unittest.TestCase):
    def setUp(self):
        self.temp_dir = Path(tempfile.mkdtemp(prefix="nightly-controller-"))
        scheduler_state.DB_DIR = self.temp_dir
        scheduler_state.DB_PATH = self.temp_dir / "controller_state.db"
        scheduler_state.LOCK_DIR = self.temp_dir / "locks"
        scheduler_state._db_initialized.clear()
        self.current_dt = datetime(2026, 4, 8, 1, 0, 0)
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

    def test_outside_window_does_not_run_jobs(self):
        with self.assertLogs("pipelines.nightly_prep_controller", level="INFO") as logs, patch(
            "pipelines.nightly_prep_controller.run_financial_crawler",
        ) as crawler_mock, patch(
            "pipelines.nightly_prep_controller.run_stock_selection",
        ) as selection_mock:
            result = run_nightly_prep_once(datetime(2026, 4, 8, 12, 0, 0))

        self.assertEqual(result, "outside_window")
        crawler_mock.assert_not_called()
        selection_mock.assert_not_called()
        self.assertIn("outside prep window", "\n".join(logs.output))

    def test_runs_crawler_then_selection_once_per_date(self):
        with self.assertLogs("pipelines.nightly_prep_controller", level="INFO") as logs, patch(
            "pipelines.nightly_prep_controller.get_enabled_accounts",
            return_value=self.accounts,
        ), patch(
            "pipelines.nightly_prep_controller.get_strategy_definition",
            return_value=SimpleNamespace(requires_selection=True),
        ), patch("pipelines.nightly_prep_controller.get_saved_selection_row_count", return_value=5), patch(
            "pipelines.nightly_prep_controller.send_notification",
        ), patch("pipelines.nightly_prep_controller.run_financial_crawler") as crawler_mock, patch(
            "pipelines.nightly_prep_controller.run_stock_selection",
        ) as selection_mock:
            first_result = run_nightly_prep_once(self.current_dt)
            second_result = run_nightly_prep_once(self.current_dt)

        state = scheduler_state.load_nightly_prep_state(self.current_dt.date())
        self.assertEqual(first_result, "completed")
        self.assertEqual(second_result, "completed")
        self.assertEqual(crawler_mock.call_count, 1)
        self.assertEqual(selection_mock.call_count, 1)
        self.assertEqual(state["status"], "completed")
        output = "\n".join(logs.output)
        self.assertIn("Nightly prep starting crawler", output)
        self.assertIn("Nightly prep crawler completed", output)
        self.assertIn("Nightly prep starting selection", output)
        self.assertIn("Nightly prep completed", output)

    def test_resumes_at_selection_after_crawler_completed(self):
        scheduler_state.save_nightly_prep_state(
            self.current_dt.date(),
            status="failed",
            crawler_started_at="2026-04-08T00:00:00+09:00",
            crawler_finished_at="2026-04-08T00:30:00+09:00",
            selection_started_at="2026-04-08T00:45:00+09:00",
            error_text="previous failure",
        )

        with self.assertLogs("pipelines.nightly_prep_controller", level="INFO") as logs, patch(
            "pipelines.nightly_prep_controller.get_enabled_accounts",
            return_value=self.accounts,
        ), patch(
            "pipelines.nightly_prep_controller.get_strategy_definition",
            return_value=SimpleNamespace(requires_selection=True),
        ), patch("pipelines.nightly_prep_controller.get_saved_selection_row_count", return_value=3), patch(
            "pipelines.nightly_prep_controller.send_notification",
        ), patch("pipelines.nightly_prep_controller.run_financial_crawler") as crawler_mock, patch(
            "pipelines.nightly_prep_controller.run_stock_selection",
        ) as selection_mock:
            result = run_nightly_prep_once(self.current_dt)

        state = scheduler_state.load_nightly_prep_state(self.current_dt.date())
        self.assertEqual(result, "completed")
        crawler_mock.assert_not_called()
        selection_mock.assert_called_once()
        self.assertEqual(state["status"], "completed")
        self.assertIn("Nightly prep resuming selection", "\n".join(logs.output))

    def test_validation_failure_marks_run_failed(self):
        with self.assertLogs("pipelines.nightly_prep_controller", level="ERROR") as logs, patch(
            "pipelines.nightly_prep_controller.get_enabled_accounts",
            return_value=self.accounts,
        ), patch(
            "pipelines.nightly_prep_controller.get_strategy_definition",
            return_value=SimpleNamespace(requires_selection=True),
        ), patch("pipelines.nightly_prep_controller.get_saved_selection_row_count", return_value=0), patch(
            "pipelines.nightly_prep_controller.send_notification",
        ) as notify_mock, patch("pipelines.nightly_prep_controller.run_financial_crawler"), patch(
            "pipelines.nightly_prep_controller.run_stock_selection",
        ):
            result = run_nightly_prep_once(self.current_dt)

        state = scheduler_state.load_nightly_prep_state(self.current_dt.date())
        self.assertEqual(result, "failed")
        self.assertEqual(state["status"], "failed")
        notify_mock.assert_called_once()
        self.assertIn("Nightly prep selection failed", "\n".join(logs.output))

    def test_repeated_same_failure_notifies_once(self):
        with patch("pipelines.nightly_prep_controller.get_enabled_accounts", return_value=self.accounts), patch(
            "pipelines.nightly_prep_controller.get_strategy_definition",
            return_value=SimpleNamespace(requires_selection=True),
        ), patch("pipelines.nightly_prep_controller.send_notification") as notify_mock, patch(
            "pipelines.nightly_prep_controller.run_financial_crawler",
            side_effect=RuntimeError("crawler down"),
        ):
            first_result = run_nightly_prep_once(self.current_dt)
            second_result = run_nightly_prep_once(self.current_dt)

        self.assertEqual(first_result, "failed")
        self.assertEqual(second_result, "failed")
        notify_mock.assert_called_once()


if __name__ == "__main__":
    unittest.main()
