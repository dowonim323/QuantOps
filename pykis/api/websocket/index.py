from __future__ import annotations

from datetime import datetime, timedelta
from decimal import Decimal
from typing import TYPE_CHECKING, Callable, Protocol, runtime_checkable

from pykis.api.stock.index import INDEX_CODE, INDEX_NAME_MAP, resolve_index_code
from pykis.api.stock.quote import STOCK_SIGN_TYPE, STOCK_SIGN_TYPE_MAP
from pykis.event.handler import KisEventFilter, KisEventTicket
from pykis.event.subscription import KisSubscriptionEventArgs
from pykis.responses.types import KisAny, KisDecimal, KisInt, KisString
from pykis.responses.websocket import KisWebsocketResponse, KisWebsocketResponseProtocol
from pykis.utils.repr import kis_repr
from pykis.utils.timezone import TIMEZONE
from pykis.utils.typing import Checkable

if TYPE_CHECKING:
    from pykis.client.websocket import KisWebsocketClient

__all__ = [
    "KisRealtimeIndexPrice",
    "KisDomesticRealtimeIndexPrice",
    "on_domestic_index_price",
]


@runtime_checkable
class KisRealtimeIndexPrice(KisWebsocketResponseProtocol, Protocol):
    """한국투자증권 국내지수 실시간 체결가"""

    index_code: str
    """지수 코드"""

    index_name: str
    """지수 명"""

    time: datetime
    """체결 시간"""

    time_kst: datetime
    """체결 시간(KST)"""

    price: Decimal
    """현재 지수"""

    change: Decimal
    """전일 대비"""

    change_rate: Decimal
    """전일 대비율"""

    sign: STOCK_SIGN_TYPE
    """전일 대비 부호"""

    open: Decimal
    """시가"""

    high: Decimal
    """고가"""

    low: Decimal
    """저가"""

    volume: int
    """누적 거래량"""

    amount: Decimal
    """누적 거래대금"""

    per_trade_volume: int
    """체결 건별 거래량"""

    per_trade_amount: Decimal
    """체결 건별 거래대금"""

    open_vs_price: Decimal
    """시가 대비 현재가"""

    open_vs_price_sign: STOCK_SIGN_TYPE
    """시가 대비 부호"""

    high_vs_price: Decimal
    """고가 대비 현재가"""

    high_vs_price_sign: STOCK_SIGN_TYPE
    """고가 대비 부호"""

    low_vs_price: Decimal
    """저가 대비 현재가"""

    low_vs_price_sign: STOCK_SIGN_TYPE
    """저가 대비 부호"""

    prev_close_to_open_rate: Decimal
    """전일 종가 대비 시가 비율"""

    prev_close_to_high_rate: Decimal
    """전일 종가 대비 고가 비율"""

    prev_close_to_low_rate: Decimal
    """전일 종가 대비 저가 비율"""

    upper_limit_count: int
    """상한 종목 수"""

    rising_count: int
    """상승 종목 수"""

    steady_count: int
    """보합 종목 수"""

    falling_count: int
    """하락 종목 수"""

    lower_limit_count: int
    """하한 종목 수"""

    strong_rising_count: int
    """기세 상승 종목 수"""

    strong_falling_count: int
    """기세 하락 종목 수"""

    tick_change: int
    """틱 대비"""


@kis_repr("index_code", "price", "change", lines="single")
class KisRealtimeIndexPriceRepr:
    """국내지수 실시간 체결가"""


class KisRealtimeIndexPriceBase(KisRealtimeIndexPriceRepr, KisWebsocketResponse):
    """국내지수 실시간 체결가"""

    index_code: str
    index_name: str

    time: datetime
    time_kst: datetime
    timezone = TIMEZONE

    price: Decimal
    change: Decimal
    change_rate: Decimal
    sign: STOCK_SIGN_TYPE

    open: Decimal
    high: Decimal
    low: Decimal

    volume: int
    amount: Decimal
    per_trade_volume: int
    per_trade_amount: Decimal

    open_vs_price: Decimal
    open_vs_price_sign: STOCK_SIGN_TYPE
    high_vs_price: Decimal
    high_vs_price_sign: STOCK_SIGN_TYPE
    low_vs_price: Decimal
    low_vs_price_sign: STOCK_SIGN_TYPE

    prev_close_to_open_rate: Decimal
    prev_close_to_high_rate: Decimal
    prev_close_to_low_rate: Decimal

    upper_limit_count: int
    rising_count: int
    steady_count: int
    falling_count: int
    lower_limit_count: int
    strong_rising_count: int
    strong_falling_count: int
    tick_change: int

    __fields__ = [
        KisString["index_code"],  # 0 BSTP_CLS_CODE
        None,  # 1 BSOP_HOUR
        KisDecimal["price"],  # 2 PRPR_NMIX
        KisAny(STOCK_SIGN_TYPE_MAP.__getitem__)["sign"],  # 3 PRDY_VRSS_SIGN
        KisDecimal["change"],  # 4 BSTP_NMIX_PRDY_VRSS
        KisInt["volume"],  # 5 ACML_VOL
        KisDecimal["amount"],  # 6 ACML_TR_PBMN
        KisInt["per_trade_volume"],  # 7 PCAS_VOL
        KisDecimal["per_trade_amount"],  # 8 PCAS_TR_PBMN
        KisDecimal["change_rate"],  # 9 PRDY_CTRT
        KisDecimal["open"],  # 10 OPRC_NMIX
        KisDecimal["high"],  # 11 NMIX_HGPR
        KisDecimal["low"],  # 12 NMIX_LWPR
        KisDecimal["open_vs_price"],  # 13 OPRC_VRSS_NMIX_PRPR
        KisAny(STOCK_SIGN_TYPE_MAP.__getitem__)["open_vs_price_sign"],  # 14 OPRC_VRSS_NMIX_SIGN
        KisDecimal["high_vs_price"],  # 15 HGPR_VRSS_NMIX_PRPR
        KisAny(STOCK_SIGN_TYPE_MAP.__getitem__)["high_vs_price_sign"],  # 16 HGPR_VRSS_NMIX_SIGN
        KisDecimal["low_vs_price"],  # 17 LWPR_VRSS_NMIX_PRPR
        KisAny(STOCK_SIGN_TYPE_MAP.__getitem__)["low_vs_price_sign"],  # 18 LWPR_VRSS_NMIX_SIGN
        KisDecimal["prev_close_to_open_rate"],  # 19 PRDY_CLPR_VRSS_OPRC_RATE
        KisDecimal["prev_close_to_high_rate"],  # 20 PRDY_CLPR_VRSS_HGPR_RATE
        KisDecimal["prev_close_to_low_rate"],  # 21 PRDY_CLPR_VRSS_LWPR_RATE
        KisInt["upper_limit_count"],  # 22 UPLM_ISSU_CNT
        KisInt["rising_count"],  # 23 ASCN_ISSU_CNT
        KisInt["steady_count"],  # 24 STNR_ISSU_CNT
        KisInt["falling_count"],  # 25 DOWN_ISSU_CNT
        KisInt["lower_limit_count"],  # 26 LSLM_ISSU_CNT
        KisInt["strong_rising_count"],  # 27 QTQT_ASCN_ISSU_CNT
        KisInt["strong_falling_count"],  # 28 QTQT_DOWN_ISSU_CNT
        KisInt["tick_change"],  # 29 TICK_VRSS
    ]

    def __pre_init__(self, data: list[str]):
        super().__pre_init__(data)

        now = datetime.now(TIMEZONE)

        try:
            clock = datetime.strptime(data[1], "%H%M%S").time()
            candidate = datetime.combine(now.date(), clock, tzinfo=TIMEZONE)

            # 보정: 장 마감 직후/자정 전후 데이터의 날짜 차이 보정
            delta = candidate - now
            if delta > timedelta(hours=12):
                candidate -= timedelta(days=1)
            elif delta < timedelta(hours=-12):
                candidate += timedelta(days=1)
        except (ValueError, TypeError):
            candidate = now

        self.time = candidate
        self.time_kst = candidate

    def __post_init__(self):
        super().__post_init__()
        self.index_name = INDEX_NAME_MAP.get(self.index_code, self.index_code)


class KisDomesticRealtimeIndexPrice(KisRealtimeIndexPriceBase):
    """국내지수 실시간 체결가"""


# IDE Type Checker
if TYPE_CHECKING:
    Checkable[KisRealtimeIndexPrice](KisDomesticRealtimeIndexPrice)


def on_domestic_index_price(
    self: "KisWebsocketClient",
    index: str | INDEX_CODE,
    callback: Callable[["KisWebsocketClient", KisSubscriptionEventArgs[KisRealtimeIndexPrice]], None],
    where: KisEventFilter["KisWebsocketClient", KisSubscriptionEventArgs[KisRealtimeIndexPrice]] | None = None,
    once: bool = False,
) -> KisEventTicket["KisWebsocketClient", KisSubscriptionEventArgs[KisRealtimeIndexPrice]]:
    """
    국내지수 실시간 체결가(H0UPCNT0) 스트림을 구독합니다.

    Args:
        index (str | INDEX_CODE): 지수 코드 또는 별칭 (예: 'KOSPI', '코스닥', '0001')
        callback: 이벤트 수신 콜백
        where: 추가 이벤트 필터
        once: 한 번만 실행할지 여부
    """
    code, _ = resolve_index_code(index)

    return self.on(
        id="H0UPCNT0",
        key=code,
        callback=callback,
        where=where,
        once=once,
    )

