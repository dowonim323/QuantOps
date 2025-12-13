from typing import TYPE_CHECKING, Callable, Protocol, runtime_checkable

from pykis.api.base.product import KisProductProtocol
from pykis.api.stock.market import MARKET_TYPE
from pykis.event.filters.product import KisProductEventFilter
from pykis.event.handler import KisEventFilter, KisEventTicket, KisMultiEventFilter
from pykis.event.subscription import KisSubscriptionEventArgs
from pykis.responses.types import KisString
from pykis.responses.websocket import KisWebsocketResponse, KisWebsocketResponseProtocol
from pykis.utils.repr import kis_repr
from pykis.utils.typing import Checkable

if TYPE_CHECKING:
    from pykis.client.websocket import KisWebsocketClient

__all__ = [
    "KisRealtimeMarketStatus",
    "KisDomesticMarketStatus",
    "on_market_status",
    "on_product_market_status",
]


@runtime_checkable
class KisRealtimeMarketStatus(KisWebsocketResponseProtocol, Protocol):
    """한국투자증권 실시간 장운영정보"""

    @property
    def symbol(self) -> str:
        """종목코드"""
        ...

    @property
    def trading_halt(self) -> str:
        """거래정지 여부"""
        ...

    @property
    def halt_reason(self) -> str:
        """거래정지 사유"""
        ...

    @property
    def market_operation_code(self) -> str:
        """장운영 구분 코드"""
        ...

    @property
    def expected_market_operation_code(self) -> str:
        """예상 장운영 구분 코드"""
        ...

    @property
    def discretionary_extension_code(self) -> str:
        """임의 연장 구분 코드"""
        ...

    @property
    def allocation_code(self) -> str:
        """동시호가 배분 처리 구분 코드"""
        ...

    @property
    def status_code(self) -> str:
        """종목 상태 구분 코드"""
        ...

    @property
    def vi_code(self) -> str:
        """VI 적용 구분 코드"""
        ...

    @property
    def after_hours_vi_code(self) -> str:
        """시간외단일가 VI 적용 구분 코드"""
        ...

    @property
    def exchange_code(self) -> str:
        """거래소 구분 코드"""
        ...


@kis_repr(
    "symbol",
    "market_operation_code",
    "vi_code",
    lines="single",
)
class KisRealtimeMarketStatusRepr:
    """한국투자증권 실시간 장운영정보"""


class KisRealtimeMarketStatusBase(KisRealtimeMarketStatusRepr, KisWebsocketResponse):
    """한국투자증권 실시간 장운영정보"""

    symbol: str
    """종목코드"""
    trading_halt: str
    """거래정지 여부"""
    halt_reason: str
    """거래정지 사유"""
    market_operation_code: str
    """장운영 구분 코드"""
    expected_market_operation_code: str
    """예상 장운영 구분 코드"""
    discretionary_extension_code: str
    """임의 연장 구분 코드"""
    allocation_code: str
    """동시호가 배분 처리 구분 코드"""
    status_code: str
    """종목 상태 구분 코드"""
    vi_code: str
    """VI 적용 구분 코드"""
    after_hours_vi_code: str
    """시간외단일가 VI 적용 구분 코드"""
    exchange_code: str
    """거래소 구분 코드"""


class KisDomesticMarketStatus(KisRealtimeMarketStatusBase):
    """국내주식 실시간 장운영정보"""

    market: MARKET_TYPE = "KRX"
    """시장 구분"""

    __fields__ = [
        KisString["symbol"],  # 0 mksc_shrn_iscd
        KisString["trading_halt"],  # 1 trht_yn
        KisString["halt_reason"],  # 2 tr_susp_reas_cntt
        KisString["market_operation_code"],  # 3 mkop_cls_code
        KisString["expected_market_operation_code"],  # 4 antc_mkop_cls_code
        KisString["discretionary_extension_code"],  # 5 mrkt_trtm_cls_code
        KisString["allocation_code"],  # 6 divi_app_cls_code
        KisString["status_code"],  # 7 iscd_stat_cls_code
        KisString["vi_code"],  # 8 vi_cls_code
        KisString["after_hours_vi_code"],  # 9 ovtm_vi_cls_code
        KisString["exchange_code"],  # 10 EXCH_CLS_CODE
    ]


# IDE Type Checker
if TYPE_CHECKING:
    Checkable[KisRealtimeMarketStatus](KisDomesticMarketStatus)


def on_market_status(
    self: "KisWebsocketClient",
    market: MARKET_TYPE,
    symbol: str,
    callback: Callable[["KisWebsocketClient", KisSubscriptionEventArgs[KisRealtimeMarketStatus]], None],
    where: KisEventFilter["KisWebsocketClient", KisSubscriptionEventArgs[KisRealtimeMarketStatus]] | None = None,
    once: bool = False,
) -> KisEventTicket["KisWebsocketClient", KisSubscriptionEventArgs[KisRealtimeMarketStatus]]:
    """
    웹소켓 이벤트 핸들러 등록

    [국내주식] 실시간시세 -> 국내주식 장운영정보[실시간-049]

    Args:
        market (MARKET_TYPE): 시장유형
        symbol (str): 종목코드
        callback (Callable[[KisWebsocketClient, KisSubscriptionEventArgs[KisRealtimeMarketStatus]], None]): 콜백 함수
        where (KisEventFilter[KisWebsocketClient, KisSubscriptionEventArgs[KisRealtimeMarketStatus]] | None, optional): 이벤트 필터. Defaults to None.
        once (bool, optional): 한번만 실행 여부. Defaults to False.
    """
    if market != "KRX":
        raise ValueError("장운영정보는 국내주식(KRX) 시장만 지원합니다.")

    filter = KisProductEventFilter(symbol=symbol, market=market)

    return self.on(
        id="H0STMKO0",
        key=symbol,
        callback=callback,
        where=KisMultiEventFilter(filter, where) if where else filter,
        once=once,
    )


def on_product_market_status(
    self: "KisProductProtocol",
    callback: Callable[["KisWebsocketClient", KisSubscriptionEventArgs[KisRealtimeMarketStatus]], None],
    where: KisEventFilter["KisWebsocketClient", KisSubscriptionEventArgs[KisRealtimeMarketStatus]] | None = None,
    once: bool = False,
) -> KisEventTicket["KisWebsocketClient", KisSubscriptionEventArgs[KisRealtimeMarketStatus]]:
    """
    웹소켓 이벤트 핸들러 등록

    [국내주식] 실시간시세 -> 국내주식 장운영정보[실시간-049]

    Args:
        callback (Callable[[KisWebsocketClient, KisSubscriptionEventArgs[KisRealtimeMarketStatus]], None]): 콜백 함수
        where (KisEventFilter[KisWebsocketClient, KisSubscriptionEventArgs[KisRealtimeMarketStatus]] | None, optional): 이벤트 필터. Defaults to None.
        once (bool, optional): 한번만 실행 여부. Defaults to False.
    """
    return on_market_status(
        self.kis.websocket,
        market=self.market,
        symbol=self.symbol,
        callback=callback,
        where=where,
        once=once,
    )

