from typing import Literal, Protocol, runtime_checkable

from pykis.api.stock.paidin_capin import KisPaidInCapitalScheduleItem

__all__ = [
    "KisPaidInCapitalProduct",
    "KisPaidInCapitalProductMixin",
]


@runtime_checkable
class KisPaidInCapitalProduct(Protocol):
    """유상증자 일정을 조회할 수 있는 상품 프로토콜"""

    def paidin_capin(
        self,
        *,
        start: str,
        end: str,
        gb1: Literal["1", "2"] = "1",
        max_pages: int = 10,
    ) -> list[KisPaidInCapitalScheduleItem]:
        ...


class KisPaidInCapitalProductMixin:
    """유상증자 일정 조회 믹스인"""

    from pykis.api.stock.paidin_capin import product_paidin_capin as paidin_capin
