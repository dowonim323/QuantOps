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
    "KisIncomeStatementItem",
    "KisIncomeStatement",
    "KisIncomeStatementResponse",
    "income_statement",
]


@runtime_checkable
class KisIncomeStatementItem(Protocol):
    """손익계산서 항목 (Income Statement Item)"""

    @property
    def base_date(self) -> date:
        """기준일자 (Base Date)"""
        ...

    @property
    def revenue(self) -> Decimal:
        """매출액 (Revenue / Sales)"""
        ...

    @property
    def cost_of_sales(self) -> Decimal:
        """매출원가 (Cost of Sales / Cost of Goods Sold)"""
        ...

    @property
    def gross_profit(self) -> Decimal:
        """매출총이익 (Gross Profit)"""
        ...

    @property
    def selling_and_administrative_expenses(self) -> Decimal:
        """판매관리비 (Selling and Administrative Expenses / Operating Expenses)"""
        ...

    @property
    def operating_income(self) -> Decimal:
        """영업이익 (Operating Income / Operating Profit)"""
        ...

    @property
    def non_operating_income(self) -> Decimal:
        """영업외수익 (Non-operating Income)"""
        ...

    @property
    def non_operating_expenses(self) -> Decimal:
        """영업외비용 (Non-operating Expenses)"""
        ...

    @property
    def ordinary_income(self) -> Decimal:
        """경상이익 (Ordinary Income / Income Before Tax)"""
        ...

    @property
    def net_income(self) -> Decimal:
        """당기순이익 (Net Income / Net Profit)"""
        ...

    @property
    def depreciation(self) -> Decimal:
        """감가상각비 (Depreciation)"""
        ...


@runtime_checkable
class KisIncomeStatement(Protocol):
    """손익계산서 (Income Statement)"""

    @property
    def items(self) -> list[KisIncomeStatementItem]:
        """손익계산서 항목 리스트"""
        ...

    def __len__(self) -> int:
        """항목 개수"""
        ...

    def __iter__(self):
        """항목 반복"""
        ...

    def __getitem__(self, index: int) -> KisIncomeStatementItem:
        """항목 조회"""
        ...


@runtime_checkable
class KisIncomeStatementResponse(KisIncomeStatement, KisResponseProtocol, Protocol):
    """손익계산서 응답 (Income Statement Response)"""


@kis_repr(
    "base_date",
    "revenue",
    "cost_of_sales",
    "gross_profit",
    "selling_and_administrative_expenses",
    "operating_income",
    "non_operating_income",
    "non_operating_expenses",
    "ordinary_income",
    "net_income",
    lines="single",
)
class KisIncomeStatementItemRepr:
    """손익계산서 항목 (Income Statement Item)"""


class KisDomesticIncomeStatementItem(KisIncomeStatementItemRepr, KisDynamic):
    """국내주식 손익계산서 항목 (Domestic Stock Income Statement Item)"""

    base_date: date = KisDate(format="%Y%m", timezone=TIMEZONE)["stac_yymm"]
    """기준일자 (Base Date) - 매월 1일 기준"""

    revenue: Decimal = KisDecimal["sale_account"]
    """매출액 (Revenue / Sales)"""
    cost_of_sales: Decimal = KisDecimal["sale_cost"]
    """매출원가 (Cost of Sales / COGS)"""
    gross_profit: Decimal = KisDecimal["sale_totl_prfi"]
    """매출총이익 (Gross Profit)"""

    depreciation: Decimal = KisDecimal["depr_cost"]
    """감가상각비 (Depreciation)"""
    selling_and_administrative_expenses: Decimal = KisDecimal["sell_mang"]
    """판매관리비 (Selling and Administrative Expenses)"""

    operating_income: Decimal = KisDecimal["bsop_prti"]
    """영업이익 (Operating Income / Operating Profit)"""

    non_operating_income: Decimal = KisDecimal["bsop_non_ernn"]
    """영업외수익 (Non-operating Income)"""
    non_operating_expenses: Decimal = KisDecimal["bsop_non_expn"]
    """영업외비용 (Non-operating Expenses)"""

    ordinary_income: Decimal = KisDecimal["op_prfi"]
    """경상이익 (Ordinary Income / Income Before Tax)"""

    net_income: Decimal = KisDecimal["thtr_ntin"]
    """당기순이익 (Net Income / Net Profit / Profit for the Period)"""


@kis_repr(
    "symbol",
    "items",
    lines="multiple",
    field_lines={
        "items": "multiple",
    },
)
class KisIncomeStatementRepr:
    """손익계산서 (Income Statement)"""


class KisDomesticIncomeStatement(KisIncomeStatementRepr, KisAPIResponse):
    """국내주식 손익계산서 (Domestic Stock Income Statement)"""

    __path__ = None  # output을 자동으로 참조하지 않도록 설정

    symbol: str
    """종목코드"""
    period_type: Literal["year", "quarter"]
    """기간 구분 (년/분기)"""

    items: list[KisIncomeStatementItem] = KisList(KisDomesticIncomeStatementItem)["output"]
    """손익계산서 항목 리스트"""

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

    def __getitem__(self, index: int) -> KisIncomeStatementItem:
        """항목 조회"""
        return self.items[index]


def income_statement(
    self: "PyKis",
    symbol: str,
    period: Literal["year", "quarter"] = "year",
) -> KisIncomeStatementResponse:
    """
    국내주식 손익계산서 조회

    국내주식 손익계산서[v1_국내주식-079]
    (업데이트 날짜: 2024/03/30)

    Args:
        symbol (str): 종목코드
        period (Literal["year", "quarter"]): 기간 구분 (년/분기). 기본값: "year"
            * 분기 데이터는 연단위 누적 합산

    Returns:
        KisIncomeStatementResponse: 손익계산서 응답

    Raises:
        KisAPIError: API 호출에 실패한 경우
        ValueError: 종목 코드가 올바르지 않은 경우

    Examples:
        >>> # 연간 손익계산서 조회
        >>> is_data = kis.income_statement("005930", period="year")
        >>> for item in is_data.items:
        ...     print(f"{item.base_date}: 매출액={item.revenue}, 영업이익={item.operating_income}")

        >>> # 분기 손익계산서 조회 (연단위 누적)
        >>> is_data = kis.income_statement("005930", period="quarter")
        >>> print(f"최근 분기 당기순이익: {is_data.items[0].net_income}")
    """
    if not symbol:
        raise ValueError("종목코드를 입력해주세요.")

    period_code = "0" if period == "year" else "1"

    return self.fetch(
        "/uapi/domestic-stock/v1/finance/income-statement",
        api="FHKST66430200",
        params={
            "FID_DIV_CLS_CODE": period_code,
            "fid_cond_mrkt_div_code": "J",
            "fid_input_iscd": symbol,
        },
        response_type=KisDomesticIncomeStatement(
            symbol=symbol,
            period_type=period,
        ),
        domain="real",
    )


def product_income_statement(
    self: "KisProductProtocol",
    period: Literal["year", "quarter"] = "year",
) -> KisIncomeStatementResponse:
    """
    국내주식 손익계산서 조회

    국내주식 손익계산서[v1_국내주식-079]
    (업데이트 날짜: 2024/03/30)

    Args:
        period (Literal["year", "quarter"]): 기간 구분 (년/분기). 기본값: "year"
            * 분기 데이터는 연단위 누적 합산

    Returns:
        KisIncomeStatementResponse: 손익계산서 응답

    Raises:
        KisAPIError: API 호출에 실패한 경우
        ValueError: 국내주식이 아닌 경우

    Examples:
        >>> stock = kis.stock("005930")
        >>> is_data = stock.income_statement(period="year")
        >>> print(f"최근 연도 당기순이익: {is_data.items[0].net_income}")
    """
    if self.market != "KRX":
        raise ValueError("손익계산서 조회는 국내주식만 지원합니다.")

    return income_statement(
        self.kis,
        symbol=self.symbol,
        period=period,
    )

