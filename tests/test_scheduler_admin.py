import io
import os
import shutil
import sys
import tempfile
import unittest
from contextlib import redirect_stdout
from datetime import date
from pathlib import Path
from unittest.mock import patch

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import tools.scheduler_state as scheduler_state
from pipelines import scheduler_admin
from tools.trading_profiles import AccountProfile


class TestSchedulerAdmin(unittest.TestCase):
    def setUp(self):
        self.temp_dir = Path(tempfile.mkdtemp(prefix="scheduler-admin-"))
        scheduler_state.DB_DIR = self.temp_dir
        scheduler_state.DB_PATH = self.temp_dir / "controller_state.db"
        scheduler_state.LOCK_DIR = self.temp_dir / "locks"
        scheduler_state._db_initialized.clear()
        self.account = AccountProfile(
            account_id="krx_vmq",
            display_name="KRX VMQ Account",
            secret_filename="krx_vmq.json",
            strategy_id="krx_vmq",
        )

    def tearDown(self):
        scheduler_state._db_initialized.clear()
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def test_clear_trading_review_command_outputs_cleared_state(self):
        scheduler_state.save_trading_day_state(
            date(2026, 4, 7),
            account_id="krx_vmq",
            status="blocked",
            phase="manual_review",
            manual_review_required=True,
            error_text="pending review",
        )

        stdout = io.StringIO()
        with patch.object(sys, "argv", [
            "scheduler_admin",
            "clear-trading-review",
            "--run-date",
            "2026-04-07",
            "--account-id",
            "krx_vmq",
        ]), redirect_stdout(stdout):
            scheduler_admin.main()

        output = stdout.getvalue()
        self.assertIn('"manual_review_required": false', output)
        self.assertIn('"status": "reviewed"', output)

    def test_status_command_includes_pending_manual_reviews(self):
        scheduler_state.save_trading_day_state(
            date(2026, 4, 7),
            account_id="krx_vmq",
            status="blocked",
            phase="manual_review",
            manual_review_required=True,
            error_text="pending review",
        )

        stdout = io.StringIO()
        with patch.object(sys, "argv", [
            "scheduler_admin",
            "status",
            "--run-date",
            "2026-04-08",
        ]), patch("pipelines.scheduler_admin.get_enabled_accounts", return_value=[self.account]), redirect_stdout(stdout):
            scheduler_admin.main()

        output = stdout.getvalue()
        self.assertIn('"pending_manual_reviews"', output)
        self.assertIn('"run_date": "2026-04-07"', output)

    def test_status_command_shows_launch_mode_and_heartbeat(self):
        scheduler_state.save_trading_day_state(
            date(2026, 4, 8),
            account_id="krx_vmq",
            status="running",
            phase="running",
            launch_mode="degraded_sell_only",
            launch_reason="selection missing",
            last_heartbeat_at="2026-04-08T09:00:00+09:00",
        )

        stdout = io.StringIO()
        with patch.object(sys, "argv", [
            "scheduler_admin",
            "status",
            "--run-date",
            "2026-04-08",
        ]), patch("pipelines.scheduler_admin.get_enabled_accounts", return_value=[self.account]), redirect_stdout(stdout):
            scheduler_admin.main()

        output = stdout.getvalue()
        self.assertIn('"launch_mode": "degraded_sell_only"', output)
        self.assertIn('"launch_reason": "selection missing"', output)
        self.assertIn('"last_heartbeat_at": "2026-04-08T09:00:00+09:00"', output)


if __name__ == "__main__":
    unittest.main()
