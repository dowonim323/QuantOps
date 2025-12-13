from __future__ import annotations

from typing import TYPE_CHECKING, Literal, Protocol, runtime_checkable

from pykis.api.base.product import KisProductProtocol
from pykis.responses.dynamic import KisDynamic, KisList
from pykis.responses.response import KisAPIResponse
from pykis.responses.types import KisDynamicDict, KisString
from pykis.utils.repr import kis_repr

if TYPE_CHECKING:
    from pykis.kis import PyKis

__all__ = [
    "KisPaidInCapitalScheduleItem",
    "paidin_capin",
    "product_paidin_capin",
]


@runtime_checkable
class KisPaidInCapitalScheduleItemProtocol(Protocol):
    """예탁원 유상증자 일정 항목"""

    @property
    def record_date(self) -> str:
        """기준일(YYYYMMDD)"""
        ...

    @property
    def symbol(self) -> str:
        """종목코드"""
        ...

    @property
    def listing_date(self) -> str:
        """상장/등록 예정일(YYYYMMDD)"""
        ...


@kis_repr(
    "record_date",
    "symbol",
    "listing_date",
    lines="single",
)
class KisPaidInCapitalScheduleItem(KisDynamic):
    """예탁원 유상증자 일정 항목"""

    __ignore_missing__ = True

    record_date: str = KisString()["record_date"]
    symbol: str = KisString()["sht_cd"]
    total_issue_quantity: str = KisString()["tot_issue_stk_qty"]
    issue_quantity: str = KisString()["issue_stk_qty"]
    fixed_allocation_rate: str = KisString()["fix_rate"]
    discount_rate: str = KisString()["disc_rate"]
    fixed_price: str = KisString()["fix_price"]
    rights_detach_date: str = KisString()["right_dt"]
    subscription_period_text: str = KisString()["sub_term"]
    subscription_period_detail: str = KisString()["sub_term_ft"]
    listing_date: str = KisString()["list_date"]
    stock_kind: str = KisString()["stk_kind"]
    remarks: str = KisString()["etc"]


class _KisPaidInCapitalSchedulePage(KisAPIResponse):
    """예탁원 유상증자 일정 단일 페이지"""

    __ignore_missing__ = True
    __path__ = None

    items: list[KisPaidInCapitalScheduleItem] = KisList(KisPaidInCapitalScheduleItem)["output1"]
    continuation: list[KisDynamicDict] = KisList(KisDynamicDict)["output2"]

    def next_cts(self) -> str:
        if not self.continuation:
            return ""

        first = self.continuation[0]
        value = getattr(first, "cts", "")
        return value or ""


def paidin_capin(
    self: "PyKis",
    symbol: str,
    *,
    start: str,
    end: str,
    gb1: Literal["1", "2"] = "1",
    max_pages: int = 10,
) -> list[KisPaidInCapitalScheduleItem]:
    """
    예탁원 유상증자 일정을 조회합니다.

    국내주식 종목정보 > 예탁원정보(유상증자일정)[국내주식-143]

    Args:
        symbol (str): 종목코드 (공백 시 전체)
        start (str): 조회 시작일 (YYYYMMDD)
        end (str): 조회 종료일 (YYYYMMDD)
        gb1 (Literal["1", "2"]): 조회 구분 (1: 청약일별, 2: 기준일별)
        max_pages (int): 연속조회 최대 페이지 수

    Returns:
        list[KisPaidInCapitalScheduleItem]: 유상증자 일정 목록
    """

    if not gb1:
        raise ValueError("gb1 값을 입력해주세요. (예: '1')")

    if not start:
        raise ValueError("start 값을 입력해주세요. (예: '20240101')")

    if not end:
        raise ValueError("end 값을 입력해주세요. (예: '20241231')")

    params = {
        "GB1": gb1,
        "F_DT": start,
        "T_DT": end,
        "SHT_CD": symbol or "",
    }

    cts = ""
    items: list[KisPaidInCapitalScheduleItem] = []

    for _ in range(max_pages):
        params["CTS"] = cts

        page = self.fetch(
            "/uapi/domestic-stock/v1/ksdinfo/paidin-capin",
            api="HHKDB669100C0",
            params=params,
            response_type=_KisPaidInCapitalSchedulePage,
            domain="real",
        )

        items.extend(page.items)

        response = page.__response__
        tr_cont = response.headers.get("tr_cont") if response else None

        if tr_cont != "M":
            break

        cts = page.next_cts()

        if not cts:
            break

    return items


def product_paidin_capin(
    self: "KisProductProtocol",
    *,
    start: str,
    end: str,
    gb1: Literal["1", "2"] = "1",
    max_pages: int = 10,
) -> list[KisPaidInCapitalScheduleItem]:
    """상품 객체에서 유상증자 일정을 조회합니다."""

    if self.market != "KRX":
        raise ValueError("유상증자 일정 조회는 국내주식만 지원합니다.")

    return paidin_capin(
        self.kis,
        symbol=self.symbol,
        start=start,
        end=end,
        gb1=gb1,
        max_pages=max_pages,
    )
