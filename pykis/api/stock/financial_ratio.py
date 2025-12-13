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
    "KisFinancialRatioItem",
    "KisFinancialRatio",
    "KisFinancialRatioResponse",
    "financial_ratio",
]


@runtime_checkable
class KisFinancialRatioItem(Protocol):
    """재무비율 항목 (Financial Ratio Item)"""

    @property
    def base_date(self) -> date:
        """기준일자 (Base Date)"""
        ...

    @property
    def revenue_growth_rate(self) -> Decimal:
        """매출액증가율 (Revenue Growth Rate) %"""
        ...

    @property
    def operating_income_growth_rate(self) -> Decimal:
        """영업이익증가율 (Operating Income Growth Rate) %"""
        ...

    @property
    def net_income_growth_rate(self) -> Decimal:
        """순이익증가율 (Net Income Growth Rate) %"""
        ...

    @property
    def roe(self) -> Decimal:
        """ROE - 자기자본이익률 (Return on Equity) %"""
        ...

    @property
    def eps(self) -> Decimal:
        """EPS - 주당순이익 (Earnings Per Share)"""
        ...

    @property
    def sps(self) -> Decimal:
        """SPS - 주당매출액 (Sales Per Share)"""
        ...

    @property
    def bps(self) -> Decimal:
        """BPS - 주당순자산 (Book-value Per Share)"""
        ...

    @property
    def reserve_ratio(self) -> Decimal:
        """유보율 (Reserve Ratio) %"""
        ...

    @property
    def debt_ratio(self) -> Decimal:
        """부채비율 (Debt Ratio) %"""
        ...


@runtime_checkable
class KisFinancialRatio(Protocol):
    """재무비율 (Financial Ratio)"""

    @property
    def items(self) -> list[KisFinancialRatioItem]:
        """재무비율 항목 리스트"""
        ...

    def __len__(self) -> int:
        """항목 개수"""
        ...

    def __iter__(self):
        """항목 반복"""
        ...

    def __getitem__(self, index: int) -> KisFinancialRatioItem:
        """항목 조회"""
        ...


@runtime_checkable
class KisFinancialRatioResponse(KisFinancialRatio, KisResponseProtocol, Protocol):
    """재무비율 응답 (Financial Ratio Response)"""


@kis_repr(
    "base_date",
    "revenue_growth_rate",
    "operating_income_growth_rate",
    "net_income_growth_rate",
    "roe",
    "eps",
    "bps",
    "sps",
    "reserve_ratio",
    "debt_ratio",
    lines="single",
)
class KisFinancialRatioItemRepr:
    """재무비율 항목 (Financial Ratio Item)"""


class KisDomesticFinancialRatioItem(KisFinancialRatioItemRepr, KisDynamic):
    """국내주식 재무비율 항목 (Domestic Stock Financial Ratio Item)"""

    base_date: date = KisDate(format="%Y%m", timezone=TIMEZONE)["stac_yymm"]
    """기준일자 (Base Date) - 매월 1일 기준"""

    revenue_growth_rate: Decimal = KisDecimal["grs"]
    """매출액증가율 (Revenue Growth Rate) %"""
    operating_income_growth_rate: Decimal = KisDecimal["bsop_prfi_inrt"]
    """영업이익증가율 (Operating Income Growth Rate) %"""
    net_income_growth_rate: Decimal = KisDecimal["ntin_inrt"]
    """순이익증가율 (Net Income Growth Rate) %"""

    roe: Decimal = KisDecimal["roe_val"]
    """ROE - 자기자본이익률 (Return on Equity) %"""

    eps: Decimal = KisDecimal["eps"]
    """EPS - 주당순이익 (Earnings Per Share)"""
    sps: Decimal = KisDecimal["sps"]
    """SPS - 주당매출액 (Sales Per Share)"""
    bps: Decimal = KisDecimal["bps"]
    """BPS - 주당순자산 (Book-value Per Share)"""

    reserve_ratio: Decimal = KisDecimal["rsrv_rate"]
    """유보율 (Reserve Ratio) %"""
    debt_ratio: Decimal = KisDecimal["lblt_rate"]
    """부채비율 (Debt Ratio) %"""


@kis_repr(
    "symbol",
    "items",
    lines="multiple",
    field_lines={
        "items": "multiple",
    },
)
class KisFinancialRatioRepr:
    """재무비율 (Financial Ratio)"""


class KisDomesticFinancialRatio(KisFinancialRatioRepr, KisAPIResponse):
    """국내주식 재무비율 (Domestic Stock Financial Ratio)"""

    __path__ = None  # output을 자동으로 참조하지 않도록 설정

    symbol: str
    """종목코드"""
    period_type: Literal["year", "quarter"]
    """기간 구분 (년/분기)"""

    items: list[KisFinancialRatioItem] = KisList(KisDomesticFinancialRatioItem)["output"]
    """재무비율 항목 리스트"""

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

    def __getitem__(self, index: int) -> KisFinancialRatioItem:
        """항목 조회"""
        return self.items[index]


def financial_ratio(
    self: "PyKis",
    symbol: str,
    period: Literal["year", "quarter"] = "year",
) -> KisFinancialRatioResponse:
    """
    국내주식 재무비율 조회

    국내주식 재무비율[v1_국내주식-080]
    (업데이트 날짜: 2024/03/30)

    Args:
        symbol (str): 종목코드
        period (Literal["year", "quarter"]): 기간 구분 (년/분기). 기본값: "year"

    Returns:
        KisFinancialRatioResponse: 재무비율 응답

    Raises:
        KisAPIError: API 호출에 실패한 경우
        ValueError: 종목 코드가 올바르지 않은 경우

    Examples:
        >>> # 연간 재무비율 조회
        >>> fr = kis.financial_ratio("005930", period="year")
        >>> for item in fr.items:
        ...     print(f"{item.base_date}: ROE={item.roe}%, EPS={item.eps}")

        >>> # 분기 재무비율 조회
        >>> fr = kis.financial_ratio("005930", period="quarter")
        >>> print(f"최근 분기 ROE: {fr.items[0].roe}%")
        >>> print(f"최근 분기 부채비율: {fr.items[0].debt_ratio}%")
    """
    if not symbol:
        raise ValueError("종목코드를 입력해주세요.")

    period_code = "0" if period == "year" else "1"

    return self.fetch(
        "/uapi/domestic-stock/v1/finance/financial-ratio",
        api="FHKST66430300",
        params={
            "FID_DIV_CLS_CODE": period_code,
            "fid_cond_mrkt_div_code": "J",
            "fid_input_iscd": symbol,
        },
        response_type=KisDomesticFinancialRatio(
            symbol=symbol,
            period_type=period,
        ),
        domain="real",
    )


def product_financial_ratio(
    self: "KisProductProtocol",
    period: Literal["year", "quarter"] = "year",
) -> KisFinancialRatioResponse:
    """
    국내주식 재무비율 조회

    국내주식 재무비율[v1_국내주식-080]
    (업데이트 날짜: 2024/03/30)

    Args:
        period (Literal["year", "quarter"]): 기간 구분 (년/분기). 기본값: "year"

    Returns:
        KisFinancialRatioResponse: 재무비율 응답

    Raises:
        KisAPIError: API 호출에 실패한 경우
        ValueError: 국내주식이 아닌 경우

    Examples:
        >>> stock = kis.stock("005930")
        >>> fr = stock.financial_ratio(period="year")
        >>> print(f"최근 연도 ROE: {fr.items[0].roe}%")
        >>> print(f"최근 연도 EPS: {fr.items[0].eps}")
    """
    if self.market != "KRX":
        raise ValueError("재무비율 조회는 국내주식만 지원합니다.")

    return financial_ratio(
        self.kis,
        symbol=self.symbol,
        period=period,
    )

