import os
import sys
import unittest
from datetime import date
from pathlib import Path

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from tools.account_record import (
    _resolve_db_path,
    get_previous_final_asset,
    save_final_asset,
    save_initial_asset,
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


if __name__ == "__main__":
    unittest.main()
