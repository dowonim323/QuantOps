from __future__ import annotations

from typing import TYPE_CHECKING

from pykis.api.stock.market import MARKET_TYPE
from pykis.responses.response import KisAPIResponse, raise_not_found
from pykis.responses.types import KisBool

if TYPE_CHECKING:
    from pykis.kis import PyKis

__all__ = [
    "KisDomesticTradingStatus",
    "domestic_trading_status",
]


class KisDomesticTradingStatus(KisAPIResponse):
    """한국투자증권 국내 종목 거래 상태"""

    symbol: str
    """종목코드"""
    market: MARKET_TYPE = "KRX"
    """시장 구분"""

    halt: bool = KisBool["trht_yn"]
    """거래정지 여부"""
    liquidation_only: bool = KisBool["sltr_yn"]
    """정리매매 여부"""

    def __init__(self, symbol: str):
        super().__init__()
        self.symbol = symbol

    def __pre_init__(self, data: dict) -> None:
        output = data.get("output")

        if not output:
            raise_not_found(
                data,
                "해당 종목의 거래 상태를 조회할 수 없습니다.",
                code=self.symbol,
                market=self.market,
            )

        super().__pre_init__(data)


def domestic_trading_status(
    self: "PyKis",
    symbol: str,
) -> KisDomesticTradingStatus:
    """
    한국투자증권 국내 종목 거래 상태 조회.

    국내주식 기본시세 -> 주식현재가 시세2[v1_국내주식-054]
    """
    if not symbol:
        raise ValueError("종목코드를 입력해주세요.")

    result = KisDomesticTradingStatus(symbol)

    return self.fetch(
        "/uapi/domestic-stock/v1/quotations/inquire-price-2",
        api="FHPST01010000",
        params={
            "FID_COND_MRKT_DIV_CODE": "J",
            "FID_INPUT_ISCD": symbol,
        },
        response_type=result,
        domain="real",
    )
