from typing import Literal, Protocol, runtime_checkable

from pykis.api.stock.balance_sheet import KisBalanceSheetResponse

__all__ = [
    "KisBalanceSheetProduct",
    "KisBalanceSheetProductMixin",
]


@runtime_checkable
class KisBalanceSheetProduct(Protocol):
    """한국투자증권 대차대조표 조회 가능 상품 프로토콜"""

    def balance_sheet(
        self,
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
        ...


class KisBalanceSheetProductMixin:
    """한국투자증권 대차대조표 조회 가능 상품 믹스인"""

    from pykis.api.stock.balance_sheet import product_balance_sheet as balance_sheet

