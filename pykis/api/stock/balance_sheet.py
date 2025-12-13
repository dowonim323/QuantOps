from datetime import date
from decimal import Decimal
from typing import TYPE_CHECKING, Literal, Protocol, runtime_checkable

from pykis.api.base.product import KisProductBase, KisProductProtocol
from pykis.responses.dynamic import KisDynamic, KisList
from pykis.responses.response import (
    KisAPIResponse,
    KisResponseProtocol,
)
from pykis.responses.types import KisDate, KisDecimal, KisString
from pykis.utils.repr import kis_repr
from pykis.utils.timezone import TIMEZONE

if TYPE_CHECKING:
    from pykis.kis import PyKis

__all__ = [
    "KisBalanceSheetItem",
    "KisBalanceSheet",
    "KisBalanceSheetResponse",
    "balance_sheet",
]


@runtime_checkable
class KisBalanceSheetItem(Protocol):
    """대차대조표 항목"""

    @property
    def base_date(self) -> date:
        """기준일자"""
        ...

    @property
    def total_assets(self) -> Decimal:
        """자산총계"""
        ...

    @property
    def current_assets(self) -> Decimal:
        """유동자산"""
        ...

    @property
    def non_current_assets(self) -> Decimal:
        """비유동자산"""
        ...

    @property
    def total_liabilities(self) -> Decimal:
        """부채총계"""
        ...

    @property
    def current_liabilities(self) -> Decimal:
        """유동부채"""
        ...

    @property
    def non_current_liabilities(self) -> Decimal:
        """비유동부채"""
        ...

    @property
    def total_equity(self) -> Decimal:
        """자본총계"""
        ...

    @property
    def capital_stock(self) -> Decimal:
        """자본금"""
        ...

    @property
    def capital_surplus(self) -> Decimal:
        """자본잉여금"""
        ...

    @property
    def retained_earnings(self) -> Decimal:
        """이익잉여금"""
        ...


@runtime_checkable
class KisBalanceSheet(Protocol):
    """대차대조표"""

    @property
    def items(self) -> list[KisBalanceSheetItem]:
        """대차대조표 항목 리스트"""
        ...

    def __len__(self) -> int:
        """항목 개수"""
        ...

    def __iter__(self):
        """항목 반복"""
        ...

    def __getitem__(self, index: int) -> KisBalanceSheetItem:
        """항목 조회"""
        ...


@runtime_checkable
class KisBalanceSheetResponse(KisBalanceSheet, KisResponseProtocol, Protocol):
    """대차대조표 응답"""


@kis_repr(
    "base_date",
    "total_assets",
    "current_assets",
    "non_current_assets",
    "total_liabilities",
    "current_liabilities",
    "non_current_liabilities",
    "total_equity",
    "capital_stock",
    "capital_surplus",
    "retained_earnings",
    lines="single",
)
class KisBalanceSheetItemRepr:
    """대차대조표 항목"""


class KisDomesticBalanceSheetItem(KisBalanceSheetItemRepr, KisDynamic):
    """국내주식 대차대조표 항목"""

    base_date: date = KisDate(format="%Y%m", timezone=TIMEZONE)["stac_yymm"]
    """기준일자 (매월 1일 기준)"""
    
    total_assets: Decimal = KisDecimal["total_aset"]
    """자산총계"""
    current_assets: Decimal = KisDecimal["cras"]
    """유동자산"""
    non_current_assets: Decimal = KisDecimal["fxas"]
    """비유동자산 (고정자산)"""
    
    total_liabilities: Decimal = KisDecimal["total_lblt"]
    """부채총계"""
    current_liabilities: Decimal = KisDecimal["flow_lblt"]
    """유동부채"""
    non_current_liabilities: Decimal = KisDecimal["fix_lblt"]
    """비유동부채 (고정부채)"""
    
    total_equity: Decimal = KisDecimal["total_cptl"]
    """자본총계"""
    capital_stock: Decimal = KisDecimal["cpfn"]
    """자본금"""
    capital_surplus: Decimal = KisDecimal["cfp_surp"]
    """자본잉여금"""
    retained_earnings: Decimal = KisDecimal["prfi_surp"]
    """이익잉여금"""


@kis_repr(
    "symbol",
    "items",
    lines="multiple",
    field_lines={
        "items": "multiple",
    },
)
class KisBalanceSheetRepr:
    """대차대조표"""


class KisDomesticBalanceSheet(KisBalanceSheetRepr, KisAPIResponse):
    """국내주식 대차대조표"""

    __path__ = None  # output을 자동으로 참조하지 않도록 설정

    symbol: str
    """종목코드"""
    period_type: Literal["year", "quarter"]
    """기간 구분 (년/분기)"""

    items: list[KisBalanceSheetItem] = KisList(KisDomesticBalanceSheetItem)["output"]
    """대차대조표 항목 리스트"""

    def __init__(self, symbol: str, period_type: Literal["year", "quarter"]):
        super().__init__()
        self.symbol = symbol
        self.period_type = period_type

    def __len__(self) -> int:
        """항목 개수"""
        return len(self.items)

    def __iter__(self):
        """항목 반복"""
        return iter(self.items)

    def __getitem__(self, index: int) -> KisBalanceSheetItem:
        """항목 조회"""
        return self.items[index]


def balance_sheet(
    self: "PyKis",
    symbol: str,
    period: Literal["year", "quarter"] = "year",
) -> KisBalanceSheetResponse:
    """
    국내주식 대차대조표 조회

    국내주식 대차대조표[v1_국내주식-078]
    (업데이트 날짜: 2024/03/30)

    Args:
        symbol (str): 종목코드
        period (Literal["year", "quarter"]): 기간 구분 (년/분기). 기본값: "year"

    Returns:
        KisBalanceSheetResponse: 대차대조표 응답

    Raises:
        KisAPIError: API 호출에 실패한 경우
        ValueError: 종목 코드가 올바르지 않은 경우

    Examples:
        >>> # 연간 대차대조표 조회
        >>> bs = kis.balance_sheet("005930", period="year")
        >>> for item in bs.items:
        ...     print(f"{item.base_date}: 자산총계={item.total_assets}, 부채총계={item.total_liabilities}")

        >>> # 분기 대차대조표 조회
        >>> bs = kis.balance_sheet("005930", period="quarter")
        >>> print(f"최근 분기 자본총계: {bs.items[0].total_equity}")
    """
    if not symbol:
        raise ValueError("종목코드를 입력해주세요.")

    period_code = "0" if period == "year" else "1"

    return self.fetch(
        "/uapi/domestic-stock/v1/finance/balance-sheet",
        api="FHKST66430100",
        params={
            "FID_DIV_CLS_CODE": period_code,
            "fid_cond_mrkt_div_code": "J",
            "fid_input_iscd": symbol,
        },
        response_type=KisDomesticBalanceSheet(
            symbol=symbol,
            period_type=period,
        ),
        domain="real",
    )


def product_balance_sheet(
    self: "KisProductProtocol",
    period: Literal["year", "quarter"] = "year",
) -> KisBalanceSheetResponse:
    """
    국내주식 대차대조표 조회

    국내주식 대차대조표[v1_국내주식-078]
    (업데이트 날짜: 2024/03/30)

    Args:
        period (Literal["year", "quarter"]): 기간 구분 (년/분기). 기본값: "year"

    Returns:
        KisBalanceSheetResponse: 대차대조표 응답

    Raises:
        KisAPIError: API 호출에 실패한 경우
        ValueError: 국내주식이 아닌 경우

    Examples:
        >>> stock = kis.stock("005930")
        >>> bs = stock.balance_sheet(period="year")
        >>> print(f"최근 연도 자본총계: {bs.items[0].total_equity}")
    """
    if self.market != "KRX":
        raise ValueError("대차대조표 조회는 국내주식만 지원합니다.")

    return balance_sheet(
        self.kis,
        symbol=self.symbol,
        period=period,
    )

