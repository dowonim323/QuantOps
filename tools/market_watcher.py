import logging
import os
import threading
import time
from datetime import datetime, time as dt_time
from typing import Any, Iterable, TYPE_CHECKING

from pykis import (
    PyKis,
    KisWebsocketClient,
    KisSubscriptionEventArgs,
    KisRealtimeIndexPrice,
)

from tools.notifications import send_notification

logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    from pykis import PyKis

DEFAULT_INDEXES: tuple[str, ...] = ("KOSPI", "KOSDAQ")
DEFAULT_CHANNEL = os.environ.get("NOTIFICATION_CHANNEL", "trade_execution")
DEFAULT_MARKET_CLOSE_TIME = os.environ.get("DEFAULT_MARKET_CLOSE_TIME", "15:30")


def _parse_market_close_time(value: str = DEFAULT_MARKET_CLOSE_TIME) -> dt_time:
    hour_str, minute_str = value.split(":", 1)
    return dt_time(hour=int(hour_str), minute=int(minute_str))


def _default_market_close_datetime(base_dt: datetime | None = None) -> datetime:
    current_dt = base_dt or datetime.now()
    close_time = _parse_market_close_time()
    return current_dt.replace(
        hour=close_time.hour,
        minute=close_time.minute,
        second=0,
        microsecond=0,
    )


def check_market_open_by_indexes(
    kis: "PyKis",
    indexes: Iterable[str] = ("KOSPI", "KOSDAQ"),
    *,
    timeout: float = 180.0,
) -> bool:
    """
    지정된 지수 틱을 감시해 시장 개장 여부를 판별합니다.

    모든 지수에서 최소 1회 틱을 수신하면 True, timeout까지 대기해도
    하나라도 미수신이면 False를 반환합니다.
    """
    kis.websocket.ensure_connected()

    pending = set(indexes)
    done = threading.Event()
    tickets = []

    def make_handler(key: str):
        def _handler(
            sender: KisWebsocketClient,
            event: KisSubscriptionEventArgs[KisRealtimeIndexPrice],
        ):
            if key in pending:
                pending.remove(key)
                if not pending:
                    done.set()

        return _handler

    try:
        for idx in pending:
            ticket = kis.websocket.on_domestic_index_price(idx, make_handler(idx))
            tickets.append(ticket)

        if done.wait(timeout=timeout):
            return True

        return False
    finally:
        for ticket in tickets:
            ticket.unsubscribe()


def wait_until_market_open(
    kis: "PyKis",
    indexes: Iterable[str] = ("KOSPI", "KOSDAQ"),
    *,
    timeout: float = 180.0,
    poll_interval: float = 30.0,
    verbose: bool = False,
) -> datetime:
    """
    `check_market_open_by_indexes`를 반복 호출해 시장이 열릴 때까지 대기합니다.

    Returns
    -------
    datetime
        시장 개방이 감지된 시각
    """
    if verbose:
        logger.info("Waiting for market open... (indexes=%s, timeout=%s, interval=%s)", indexes, timeout, poll_interval)

    while True:
        if check_market_open_by_indexes(
            kis,
            indexes=indexes,
            timeout=timeout,
        ):
            if verbose:
                logger.info("Market open detected!")
            return datetime.now()

        if verbose:
            logger.info("Market not open yet. Retrying in %ss...", poll_interval)

        if poll_interval > 0:
            time.sleep(poll_interval)


def wait_until_market_close(
    kis: "PyKis",
    indexes: Iterable[str] = ("KOSPI", "KOSDAQ"),
    *,
    timeout: float = 180.0,
    poll_interval: float = 60.0,
    max_error_retries: int = 5,
    default_close_time: dt_time | None = None,
    verbose: bool = False,
) -> datetime:
    """
    `check_market_open_by_indexes`를 반복 호출해 시장이 닫힐 때까지 대기합니다.

    Returns
    -------
    datetime
        시장 폐장이 감지된 시각
    """
    if verbose:
        logger.info(
            "Waiting for market close... (indexes=%s, timeout=%s, interval=%s, max_error_retries=%s)",
            indexes,
            timeout,
            poll_interval,
            max_error_retries,
        )

    error_retries = 0

    while True:
        try:
            market_open = check_market_open_by_indexes(
                kis,
                indexes=indexes,
                timeout=timeout,
            )
        except Exception as exc:
            error_retries += 1
            if verbose:
                logger.info(
                    "Market-close probe errored (%d/%d): %s",
                    error_retries,
                    max_error_retries,
                    exc,
                )
            if error_retries > max_error_retries:
                current_dt = datetime.now()
                fallback_dt = _default_market_close_datetime(current_dt)
                if default_close_time is not None:
                    fallback_dt = current_dt.replace(
                        hour=default_close_time.hour,
                        minute=default_close_time.minute,
                        second=0,
                        microsecond=0,
                    )

                wait_seconds = max(0.0, (fallback_dt - current_dt).total_seconds())
                if verbose:
                    logger.info(
                        "Market-close probe errors exceeded retry limit. Falling back to configured close time %s.",
                        fallback_dt.strftime("%H:%M:%S"),
                    )
                if wait_seconds > 0:
                    time.sleep(wait_seconds)
                if verbose:
                    logger.info("Market close detected by fallback close time.")
                return fallback_dt
            if poll_interval > 0:
                time.sleep(poll_interval)
            continue

        error_retries = 0

        if not market_open:
            if verbose:
                logger.info("Market close detected!")
            return datetime.now()

        if verbose:
            logger.info("Market still open. Retrying in %ss...", poll_interval)

        if poll_interval > 0:
            time.sleep(poll_interval)


def wait_and_notify(
    kis: PyKis,
    *,
    indexes: Iterable[str] = DEFAULT_INDEXES,
    channel: str = DEFAULT_CHANNEL,
    verbose: bool = False,
) -> None:
    """시장 개장 및 폐장을 감지하고 알림을 보냅니다."""
    indexes_tuple = tuple(indexes)
    timeout = float(os.environ.get("MARKET_CHECK_TIMEOUT", "180"))
    poll_interval_open = float(os.environ.get("MARKET_OPEN_POLL_INTERVAL", "30"))
    poll_interval_close = float(os.environ.get("MARKET_CLOSE_POLL_INTERVAL", "60"))

    if verbose:
        logger.info("Starting wait_and_notify loop. Channel: %s", channel)

    # 장 시작 대기
    open_dt = wait_until_market_open(
        kis,
        indexes=indexes_tuple,
        timeout=timeout,
        poll_interval=poll_interval_open,
        verbose=verbose,
    )
    open_time = open_dt.strftime("%Y-%m-%d %H:%M:%S")
    
    msg_open = f"Market is open based on {', '.join(indexes_tuple)}.\nDetected at: {open_time}"
    if verbose:
        logger.info("[Notification] %s", msg_open)

    send_notification(
        channel,
        msg_open,
        title="Market Open Detected",
        tags=("sunrise",),
    )

    # 장 마감 대기
    close_dt = wait_until_market_close(
        kis,
        indexes=indexes_tuple,
        timeout=timeout,
        poll_interval=poll_interval_close,
        verbose=verbose,
    )
    close_time = close_dt.strftime("%Y-%m-%d %H:%M:%S")

    msg_close = f"Market is closed based on {', '.join(indexes_tuple)}.\nDetected at: {close_time}"
    if verbose:
        logger.info("[Notification] %s", msg_close)

    send_notification(
        channel,
        msg_close,
        title="Market Close Detected",
        tags=("city_sunset",),
    )


from tools.trading_utils import retry_execution


def get_previous_close_signal(kis: "PyKis") -> dict[str, Any]:
    """
    전일 종가 기준으로 시그널을 계산합니다.
    (장 시작 전 SELL 조건 확인용)
    """
    from datetime import date, timedelta

    today = date.today()
    start_date = today - timedelta(days=60)

    def _fetch():
        return kis.domestic_index_daily_chart("KOSDAQ", start=start_date, end=today)

    success, chart = retry_execution(_fetch, max_retries=10, context="Fetching KOSDAQ chart for previous close")

    if not success or not chart:
        return {"signal": None, "reason": "Failed to fetch data"}

    past_closes = []
    prev_close = None
    for bar in chart.bars:
        if bar.time.date() < today:
            past_closes.append(float(bar.close))
            prev_close = float(bar.close)

    if len(past_closes) < 10 or prev_close is None:
        return {"signal": None, "reason": "Insufficient data"}

    ma3 = sum(past_closes[-3:]) / 3
    ma5 = sum(past_closes[-5:]) / 5
    ma10 = sum(past_closes[-10:]) / 10

    safe = prev_close > ma3 or prev_close > ma5 or prev_close > ma10
    signal = "buy" if safe else "sell"

    return {
        "signal": signal,
        "prev_close": prev_close,
        "ma3": ma3,
        "ma5": ma5,
        "ma10": ma10,
        "reason": f"PrevClose({prev_close:.2f}) vs MA3({ma3:.2f})/MA5({ma5:.2f})/MA10({ma10:.2f})"
    }


def fetch_historical_indices(kis: "PyKis") -> dict[str, list[float]]:
    """
    KOSPI, KOSDAQ의 과거 종가 데이터를 조회하여 반환합니다.
    (오늘 날짜 제외, 과거 60일 기준)
    """
    from datetime import date, timedelta

    today = date.today()
    start_date = today - timedelta(days=60)

    result = {}
    for name in ["KOSDAQ"]:
        def _fetch():
            return kis.domestic_index_daily_chart(name, start=start_date, end=today)

        success, chart = retry_execution(_fetch, max_retries=10, context=f"Fetching {name} chart")
        
        if not success or not chart:
            logger.warning("Failed to fetch historical data for %s", name)
            continue

        past_closes = []
        for bar in chart.bars:
            if bar.time.date() < today:
                past_closes.append(float(bar.close))
        result[name] = past_closes
    
    return result


def fetch_current_index_prices(
    kis: "PyKis",
    indexes: Iterable[str] = ("KOSPI", "KOSDAQ"),
    timeout: float = 60.0,
) -> dict[str, float] | None:
    """
    웹소켓을 통해 지정된 지수들의 현재가를 조회합니다.
    모든 지수의 데이터를 수신할 때까지 대기하며, timeout 내에 수신하지 못하면 None을 반환합니다.
    """
    kis.websocket.ensure_connected()
    
    results = {}
    pending = set(indexes)
    done = threading.Event()
    tickets = []
    
    def make_handler(key: str):
        def _handler(sender, event):
            results[key] = float(event.price)
            if key in pending:
                pending.remove(key)
                if not pending:
                    done.set()
        return _handler

    try:
        for idx in indexes:
            ticket = kis.websocket.on_domestic_index_price(idx, make_handler(idx))
            tickets.append(ticket)
            
        if done.wait(timeout=timeout):
            return results
        return None
    finally:
        for ticket in tickets:
            ticket.unsubscribe()


def get_market_signal(
    kis: "PyKis",
    kosdaq_current: float | None = None,
    historical_data: dict[str, list[float]] | None = None,
    verbose: bool = False,
) -> dict[str, Any]:
    """
    현재 시장 상태를 분석하여 매수/매도 시그널을 반환합니다.

    Args:
        kis (PyKis): PyKis 인스턴스
        kosdaq_current (float, optional): 현재 KOSDAQ 지수. 없으면 조회합니다.
        historical_data (dict, optional): fetch_historical_indices로 가져온 과거 데이터. 없으면 새로 조회합니다.
        verbose (bool, optional): 상세 정보 출력 여부. Defaults to False.
    """
    # 1. 과거 데이터 준비
    if historical_data is None:
        historical_data = fetch_historical_indices(kis)
    
    kosdaq_history = historical_data.get("KOSDAQ", [])

    # 2. 현재가 확인 (인자가 없으면 REST API로 조회 - 비효율적이지만 fallback)
    if kosdaq_current is None:
        from datetime import date
        today = date.today()
        
        if kosdaq_current is None:
            chart = kis.domestic_index_daily_chart("KOSDAQ", start=today, end=today)
            if chart.bars:
                kosdaq_current = float(chart.bars[-1].close)
            else:
                raise ValueError("KOSDAQ 현재가를 가져올 수 없습니다.")

    # 3. 이동평균 계산 및 시그널 판별 함수
    def analyze_index(name, past_closes, current_price):
        if len(past_closes) < 10:
            if verbose:
                logger.info("[%s] 데이터 부족 (10일 미만): Unsafe", name)
            return {
                "safe": False,
                "current": current_price,
                "ma3": 0.0, "ma5": 0.0, "ma10": 0.0,
                "reason": "Insufficient data"
            }

        closes = past_closes
        
        ma3_threshold = sum(closes[-3:]) / 3
        ma5_threshold = sum(closes[-5:]) / 5
        ma10_threshold = sum(closes[-10:]) / 10
        
        safe = False
        reason = "All MAs are higher than current price"
        
        if current_price > ma3_threshold:
            safe = True
            reason = f"Current({current_price:.2f}) > MA3({ma3_threshold:.2f})"
        elif current_price > ma5_threshold:
            safe = True
            reason = f"Current({current_price:.2f}) > MA5({ma5_threshold:.2f})"
        elif current_price > ma10_threshold:
            safe = True
            reason = f"Current({current_price:.2f}) > MA10({ma10_threshold:.2f})"
            
        if verbose:
            status = "Safe" if safe else "Unsafe"
            logger.info("[%s] Current: %.2f", name, current_price)
            logger.info("  MA3: %.2f, MA5: %.2f, MA10: %.2f", ma3_threshold, ma5_threshold, ma10_threshold)
            logger.info("  Result: %s (%s)", status, reason)
            
        return {
            "safe": safe,
            "current": current_price,
            "ma3": ma3_threshold,
            "ma5": ma5_threshold,
            "ma10": ma10_threshold,
            "reason": reason
        }

    kosdaq_analysis = analyze_index("KOSDAQ", kosdaq_history, kosdaq_current)

    # 4. 결합 시그널 (KOSDAQ Only)
    # 소형주 위주의 포트폴리오이므로 KOSDAQ 지수를 활용하여 시그널 산출
    if kosdaq_analysis["safe"]:
        signal = "buy"
    else:
        signal = "sell"
        
    if verbose:
        logger.info("[Final Signal] %s (Based on KOSDAQ: %s)", signal.upper(), "Safe" if kosdaq_analysis["safe"] else "Unsafe")
        
    return {
        "signal": signal,
        "details": {
            "KOSDAQ": kosdaq_analysis
        }
    }


def is_today_open_day(kis: "PyKis") -> bool:
    """지정한 KIS 세션(kis)을 사용해 오늘이 개장일인지 여부를 반환합니다."""
    from tools.time_utils import today_kst
    
    base_date = today_kst().strftime("%Y%m%d")
    holiday_info = kis.chk_holiday(base_date=base_date)
    return bool(getattr(holiday_info, "is_open_day", False))


class MarketMonitor:
    def __init__(self) -> None:
        self.prices: dict[str, float | None] = {"KOSPI": None, "KOSDAQ": None}
        self.last_update: float = time.time()
    
    def update(self, name: str, price: float) -> None:
        self.prices[name] = float(price)
        self.last_update = time.time()
        
    def is_active(self, timeout: int | None = 180) -> bool:
        # 데이터가 있고, 마지막 업데이트가 timeout 이내인지 확인
        if self.prices["KOSPI"] is None or self.prices["KOSDAQ"] is None:
            return False
        
        if timeout is None:
            return True
            
        return (time.time() - self.last_update) < timeout
