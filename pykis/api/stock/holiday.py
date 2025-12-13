from __future__ import annotations

from datetime import date, datetime
from typing import TYPE_CHECKING, Protocol, runtime_checkable

from pykis.responses.dynamic import KisDynamic, KisList
from pykis.responses.exceptions import KisNotFoundError
from pykis.responses.response import KisAPIResponse
from pykis.responses.types import KisBool, KisDate, KisString
from pykis.utils.repr import kis_repr

if TYPE_CHECKING:
    from pykis.kis import PyKis

__all__ = [
    "KisHolidayItem",
    "chk_holiday",
]


@runtime_checkable
class KisHolidayItemProtocol(Protocol):
    """국내 휴장일 정보 항목"""

    @property
    def base_date(self) -> date:
        """기준일자 (YYYY-MM-DD)"""
        ...

    @property
    def weekday_code(self) -> str:
        """요일 구분 코드"""
        ...

    @property
    def is_business_day(self) -> bool:
        """영업일 여부"""
        ...

    @property
    def is_trading_day(self) -> bool:
        """거래일 여부"""
        ...

    @property
    def is_open_day(self) -> bool:
        """개장일 여부"""
        ...

    @property
    def is_settlement_day(self) -> bool:
        """결제일 여부"""
        ...


@kis_repr(
    "base_date",
    "weekday_code",
    "is_business_day",
    "is_trading_day",
    "is_open_day",
    "is_settlement_day",
    lines="single",
)
class KisHolidayItem(KisDynamic):
    """국내 휴장일 정보 항목"""

    __ignore_missing__ = True

    base_date: date = KisDate()["bass_dt"]
    """기준일자"""

    weekday_code: str = KisString()["wday_dvsn_cd"]
    """요일 구분 코드"""

    is_business_day: bool = KisBool()["bzdy_yn"]
    """영업일 여부"""

    is_trading_day: bool = KisBool()["tr_day_yn"]
    """거래일 여부"""

    is_open_day: bool = KisBool()["opnd_yn"]
    """개장일 여부"""

    is_settlement_day: bool = KisBool()["sttl_day_yn"]
    """결제일 여부"""


class _KisHolidayPage(KisAPIResponse):
    """국내 휴장일 조회 응답 (단일 페이지)"""

    __path__ = None
    __ignore_missing__ = True

    items: list[KisHolidayItem] = KisList(KisHolidayItem)["output"]
    """휴장일 정보 목록"""

    context_fk: str = KisString["ctx_area_fk", ""]
    """연속 조회 키 (FK)"""

    context_nk: str = KisString["ctx_area_nk", ""]
    """연속 조회 키 (NK)"""

    def __pre_init__(self, data: dict[str, object]) -> None:
        super().__pre_init__(data)

        output = data.get("output")

        if isinstance(output, dict):
            data["output"] = [output]
        elif output is None:
            data["output"] = []


def chk_holiday(
    self: "PyKis",
    base_date: str,
    *,
    max_pages: int = 10,
) -> KisHolidayItem:
    """
    국내 휴장일 정보를 조회합니다.

    국내휴장일조회[국내주식-040]

    Args:
        base_date (str): 조회 기준일자 (예: '20250630')
        max_pages (int): 연속 조회 최대 페이지 수. 기본값 10.

    Returns:
        KisHolidayItem: 휴장일 정보

    Raises:
        ValueError: 잘못된 조회 조건인 경우
        KisNotFoundError: 기준일 데이터가 존재하지 않는 경우
    """

    if not base_date:
        raise ValueError("base_date 값을 입력해주세요. (예: '20250630')")

    try:
        target_date = datetime.strptime(base_date, "%Y%m%d").date()
    except ValueError as exc:
        raise ValueError("base_date는 'YYYYMMDD' 형식의 8자리 문자열이어야 합니다.") from exc

    params = {
        "BASS_DT": base_date,
        "CTX_AREA_FK": "",
        "CTX_AREA_NK": "",
    }

    headers: dict[str, str] | None = None
    last_response = None
    last_data: dict[str, object] | None = None

    for page_index in range(max_pages):
        page = self.fetch(
            "/uapi/domestic-stock/v1/quotations/chk-holiday",
            api="CTCA0903R",
            params=params,
            headers=headers,
            response_type=_KisHolidayPage,
            domain="real",
        )

        last_response = page.__response__
        last_data = page.raw()

        for item in page.items:
            if item.base_date == target_date:
                return item

        tr_cont = last_response.headers.get("tr_cont", "").strip().upper() if last_response else ""

        if tr_cont not in {"M", "F"}:
            break

        next_fk = (page.context_fk or "").strip()
        next_nk = (page.context_nk or "").strip()

        if not next_fk and not next_nk:
            break

        params["CTX_AREA_FK"] = next_fk
        params["CTX_AREA_NK"] = next_nk
        headers = {"tr_cont": "N"}

    if last_response is None:
        raise ValueError("휴장일 조회 응답을 수신하지 못했습니다.")

    raise KisNotFoundError(
        data=last_data or {},
        response=last_response,
        message="해당 기준일의 휴장일 정보를 찾을 수 없습니다.",
        fields={"base_date": base_date},
    )


