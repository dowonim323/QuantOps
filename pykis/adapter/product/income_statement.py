from typing import Literal, Protocol, runtime_checkable

from pykis.api.stock.income_statement import KisIncomeStatementResponse

__all__ = [
    "KisIncomeStatementProduct",
    "KisIncomeStatementProductMixin",
]


@runtime_checkable
class KisIncomeStatementProduct(Protocol):
    """한국투자증권 손익계산서 조회 가능 상품 프로토콜"""

    def income_statement(
        self,
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
        ...


class KisIncomeStatementProductMixin:
    """한국투자증권 손익계산서 조회 가능 상품 믹스인"""

    from pykis.api.stock.income_statement import product_income_statement as income_statement

