from typing import Literal, Protocol, runtime_checkable

from pykis.api.stock.financial_ratio import KisFinancialRatioResponse

__all__ = [
    "KisFinancialRatioProduct",
    "KisFinancialRatioProductMixin",
]


@runtime_checkable
class KisFinancialRatioProduct(Protocol):
    """한국투자증권 재무비율 조회 가능 상품 프로토콜"""

    def financial_ratio(
        self,
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
        """
        ...


class KisFinancialRatioProductMixin:
    """한국투자증권 재무비율 조회 가능 상품 믹스인"""

    from pykis.api.stock.financial_ratio import product_financial_ratio as financial_ratio

