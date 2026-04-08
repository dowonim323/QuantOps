from __future__ import annotations

from datetime import date, timedelta

from pykis import PyKis

from tools.time_utils import today_kst


def is_first_trading_day_of_week(
    kis: PyKis,
    *,
    base_date: date | None = None,
) -> bool:
    current_date = base_date or today_kst()
    week_start = current_date - timedelta(days=current_date.weekday())

    for offset in range(7):
        candidate = week_start + timedelta(days=offset)
        holiday_info = kis.chk_holiday(base_date=candidate.strftime("%Y%m%d"))
        if bool(getattr(holiday_info, "is_open_day", False)):
            return candidate == current_date

    return False


def is_rebalance_due_by_elapsed_week(
    last_rebalance_date: str | None,
    *,
    base_date: date | None = None,
    minimum_days: int = 7,
) -> bool:
    current_date = base_date or today_kst()
    if not last_rebalance_date:
        return True

    previous_rebalance_date = date.fromisoformat(last_rebalance_date)
    return (current_date - previous_rebalance_date).days >= minimum_days
