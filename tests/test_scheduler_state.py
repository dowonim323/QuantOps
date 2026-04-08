import os
import shutil
import sys
import tempfile
import unittest
from datetime import date
from pathlib import Path

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import tools.scheduler_state as scheduler_state


class TestSchedulerState(unittest.TestCase):
    def setUp(self):
        self.temp_dir = Path(tempfile.mkdtemp(prefix="scheduler-state-"))
        scheduler_state.DB_DIR = self.temp_dir
        scheduler_state.DB_PATH = self.temp_dir / "controller_state.db"
        scheduler_state.LOCK_DIR = self.temp_dir / "locks"
        scheduler_state._db_initialized.clear()

    def tearDown(self):
        scheduler_state._db_initialized.clear()
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def test_nightly_prep_defaults(self):
        state = scheduler_state.load_nightly_prep_state(date(2026, 4, 8))

        self.assertEqual(state["status"], "pending")
        self.assertIsNone(state["crawler_started_at"])
        self.assertIsNone(state["selection_finished_at"])

    def test_save_and_load_nightly_prep_state(self):
        scheduler_state.save_nightly_prep_state(
            date(2026, 4, 8),
            status="completed",
            crawler_started_at="2026-04-08T00:00:00+09:00",
            crawler_finished_at="2026-04-08T00:30:00+09:00",
            selection_started_at="2026-04-08T00:31:00+09:00",
            selection_finished_at="2026-04-08T00:33:00+09:00",
        )

        state = scheduler_state.load_nightly_prep_state("2026-04-08")
        self.assertEqual(state["status"], "completed")
        self.assertEqual(state["crawler_finished_at"], "2026-04-08T00:30:00+09:00")
        self.assertEqual(state["selection_finished_at"], "2026-04-08T00:33:00+09:00")

    def test_save_and_load_trading_day_state(self):
        scheduler_state.save_trading_day_state(
            date(2026, 4, 8),
            account_id="krx_vmq",
            status="blocked",
            session_started_at="2026-04-08T08:30:00+09:00",
            phase="manual_review",
            launch_mode="degraded_sell_only",
            launch_reason="selection missing",
            last_heartbeat_at="2026-04-08T09:00:00+09:00",
            restart_count=2,
            manual_review_required=True,
            error_text="manual review",
        )

        state = scheduler_state.load_trading_day_state(date(2026, 4, 8), account_id="krx_vmq")
        self.assertEqual(state["status"], "blocked")
        self.assertEqual(state["phase"], "manual_review")
        self.assertEqual(state["launch_mode"], "degraded_sell_only")
        self.assertEqual(state["launch_reason"], "selection missing")
        self.assertEqual(state["last_heartbeat_at"], "2026-04-08T09:00:00+09:00")
        self.assertEqual(state["restart_count"], 2)
        self.assertTrue(state["manual_review_required"])
        self.assertEqual(state["error_text"], "manual review")

    def test_trading_day_defaults_include_launch_metadata_fields(self):
        state = scheduler_state.load_trading_day_state(date(2026, 4, 8), account_id="krx_vmq")

        self.assertIsNone(state["launch_mode"])
        self.assertIsNone(state["launch_reason"])
        self.assertIsNone(state["last_heartbeat_at"])

    def test_existing_trading_day_schema_is_migrated_for_new_fields(self):
        self.temp_dir.mkdir(parents=True, exist_ok=True)
        with scheduler_state.sqlite3.connect(scheduler_state.DB_PATH, timeout=30.0) as conn:
            conn.execute(
                """
                CREATE TABLE trading_day_runs (
                    run_date TEXT NOT NULL,
                    account_id TEXT NOT NULL,
                    status TEXT NOT NULL DEFAULT 'pending',
                    session_started_at TEXT,
                    session_finished_at TEXT,
                    phase TEXT,
                    restart_count INTEGER NOT NULL DEFAULT 0,
                    manual_review_required INTEGER NOT NULL DEFAULT 0,
                    error_text TEXT,
                    updated_at TEXT NOT NULL,
                    PRIMARY KEY (run_date, account_id)
                )
                """
            )

        scheduler_state._db_initialized.clear()
        scheduler_state.save_trading_day_state(
            date(2026, 4, 8),
            account_id="krx_vmq",
            status="running",
            launch_mode="degraded_sell_only",
            launch_reason="selection missing",
            last_heartbeat_at="2026-04-08T09:00:00+09:00",
        )

        state = scheduler_state.load_trading_day_state(date(2026, 4, 8), account_id="krx_vmq")
        self.assertEqual(state["launch_mode"], "degraded_sell_only")
        self.assertEqual(state["launch_reason"], "selection missing")
        self.assertEqual(state["last_heartbeat_at"], "2026-04-08T09:00:00+09:00")

    def test_list_unresolved_trading_day_reviews_filters_prior_days(self):
        scheduler_state.save_trading_day_state(
            date(2026, 4, 7),
            account_id="krx_vmq",
            status="blocked",
            manual_review_required=True,
            error_text="review needed",
        )
        scheduler_state.save_trading_day_state(
            date(2026, 4, 8),
            account_id="krx_vmq",
            status="completed",
            manual_review_required=False,
        )

        unresolved = scheduler_state.list_unresolved_trading_day_reviews(
            before_run_date=date(2026, 4, 8),
            account_ids=["krx_vmq"],
        )

        self.assertEqual(len(unresolved), 1)
        self.assertEqual(unresolved[0]["run_date"], "2026-04-07")
        self.assertTrue(unresolved[0]["manual_review_required"])

    def test_clear_trading_day_manual_review_marks_state_reviewed(self):
        scheduler_state.save_trading_day_state(
            date(2026, 4, 7),
            account_id="krx_vmq",
            status="blocked",
            phase="manual_review",
            manual_review_required=True,
            error_text="review needed",
        )

        cleared = scheduler_state.clear_trading_day_manual_review(
            date(2026, 4, 7),
            account_id="krx_vmq",
        )

        self.assertEqual(cleared["status"], "reviewed")
        self.assertEqual(cleared["phase"], "reviewed")
        self.assertFalse(cleared["manual_review_required"])
        self.assertIsNone(cleared["error_text"])


if __name__ == "__main__":
    unittest.main()
