import os
import sys
import unittest
from datetime import date
from pathlib import Path

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from tools.account_record import (
    _resolve_db_path,
    get_daily_asset,
    get_opening_asset,
    get_previous_final_asset,
    save_final_asset,
    save_initial_asset,
    save_opening_asset,
)


class TestAccountRecordDailyAssets(unittest.TestCase):
    def setUp(self):
        self.account_id = f"test_daily_assets_{self._testMethodName}"
        self.db_path = _resolve_db_path(self.account_id)
        if self.db_path.exists():
            self.db_path.unlink()

    def tearDown(self):
        if self.db_path.exists():
            self.db_path.unlink()

    def test_get_previous_final_asset_skips_incomplete_days(self):
        save_final_asset(
            1_100_000.0,
            180_000.0,
            target_date=date(2026, 4, 7),
            account_id=self.account_id,
        )
        save_initial_asset(
            1_200_000.0,
            250_000.0,
            70_000.0,
            target_date=date(2026, 4, 8),
            account_id=self.account_id,
        )

        prev_asset, prev_d2 = get_previous_final_asset(
            target_date=date(2026, 4, 9),
            account_id=self.account_id,
        )

        self.assertEqual(prev_asset, 1_100_000.0)
        self.assertEqual(prev_d2, 180_000.0)

    def test_get_daily_asset_remains_backward_compatible_when_opening_asset_saved(self):
        target_date = date(2026, 4, 9)
        save_initial_asset(
            1_200_000.0,
            250_000.0,
            70_000.0,
            target_date=target_date,
            account_id=self.account_id,
        )
        save_opening_asset(
            1_240_000.0,
            target_date=target_date,
            account_id=self.account_id,
        )
        save_final_asset(
            1_260_000.0,
            255_000.0,
            target_date=target_date,
            account_id=self.account_id,
        )

        initial_asset, final_asset, transfer_amount = get_daily_asset(
            target_date=target_date,
            account_id=self.account_id,
        )

        self.assertEqual(initial_asset, 1_200_000.0)
        self.assertEqual(final_asset, 1_260_000.0)
        self.assertEqual(transfer_amount, 70_000.0)

    def test_opening_asset_can_be_saved_and_loaded_without_initial_asset(self):
        target_date = date(2026, 4, 9)

        self.assertIsNone(
            get_opening_asset(target_date=target_date, account_id=self.account_id),
        )

        save_opening_asset(
            1_230_000.0,
            target_date=target_date,
            account_id=self.account_id,
        )

        self.assertEqual(
            get_opening_asset(target_date=target_date, account_id=self.account_id),
            1_230_000.0,
        )


if __name__ == "__main__":
    unittest.main()
