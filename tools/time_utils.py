from __future__ import annotations

from datetime import date, datetime, time as dt_time

try:
    from zoneinfo import ZoneInfo as _ZoneInfo
except Exception:  # Python<3.9 등에서 zoneinfo 미지원
    _ZoneInfo = None

_KST = _ZoneInfo("Asia/Seoul") if _ZoneInfo else None


def today_kst() -> date:
    """한국 표준시(KST) 기준 오늘 날짜를 반환합니다."""
    return datetime.now(_KST).date() if _KST else date.today()


def now_kst() -> datetime:
    return datetime.now(_KST) if _KST else datetime.now()


def combine_kst(target_date: date, target_time: dt_time) -> datetime:
    naive_dt = datetime.combine(target_date, target_time)
    return naive_dt.replace(tzinfo=_KST) if _KST else naive_dt


def within_kst_window(
    base_dt: datetime,
    *,
    start: dt_time,
    end: dt_time,
) -> bool:
    resolved_dt = base_dt.astimezone(_KST) if _KST and base_dt.tzinfo else base_dt
    current_time = resolved_dt.time()
    return start <= current_time < end


__all__ = ["combine_kst", "now_kst", "today_kst", "within_kst_window"]
