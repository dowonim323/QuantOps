import os
import sys
import unittest
from datetime import date
from types import SimpleNamespace
from unittest.mock import MagicMock

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from strategies.schedule import is_first_trading_day_of_week, is_rebalance_due_by_elapsed_week


class TestStrategySchedule(unittest.TestCase):
    def test_returns_true_when_monday_is_open(self):
        kis = MagicMock()
        kis.chk_holiday.return_value = SimpleNamespace(is_open_day=True)

        result = is_first_trading_day_of_week(kis, base_date=date(2026, 3, 23))

        self.assertTrue(result)

    def test_rolls_to_tuesday_when_monday_is_holiday(self):
        def holiday_side_effect(*, base_date: str):
            open_days = {
                "20260324": True,
            }
            return SimpleNamespace(is_open_day=open_days.get(base_date, False))

        kis = MagicMock()
        kis.chk_holiday.side_effect = holiday_side_effect

        self.assertTrue(is_first_trading_day_of_week(kis, base_date=date(2026, 3, 24)))
        self.assertFalse(is_first_trading_day_of_week(kis, base_date=date(2026, 3, 25)))

    def test_rebalance_due_when_no_previous_date(self):
        self.assertTrue(is_rebalance_due_by_elapsed_week(None, base_date=date(2026, 3, 24)))

    def test_rebalance_due_after_seven_days(self):
        self.assertTrue(is_rebalance_due_by_elapsed_week("2026-03-17", base_date=date(2026, 3, 24)))

    def test_rebalance_not_due_before_seven_days(self):
        self.assertFalse(is_rebalance_due_by_elapsed_week("2026-03-18", base_date=date(2026, 3, 24)))
