from __future__ import annotations

from datetime import date, datetime

try:
    from zoneinfo import ZoneInfo as _ZoneInfo
except Exception:  # Python<3.9 등에서 zoneinfo 미지원
    _ZoneInfo = None

_KST = _ZoneInfo("Asia/Seoul") if _ZoneInfo else None


def today_kst() -> date:
    """한국 표준시(KST) 기준 오늘 날짜를 반환합니다."""
    return datetime.now(_KST).date() if _KST else date.today()


__all__ = ["today_kst"]

