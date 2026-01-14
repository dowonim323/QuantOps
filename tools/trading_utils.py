from __future__ import annotations

import math
import threading
import time
import logging
from decimal import Decimal
from typing import Any, Callable, Iterable, Literal, Mapping, TYPE_CHECKING

COUNTRY_TYPE = Literal["KR", "US", "HK", "JP", "VN", "CN"]

from pykis import (
    KisEventTicket,
    KisRealtimeOrderbook,
    KisSubscriptionEventArgs,
    KisWebsocketClient,
)
from tools.notifications import send_notification
from tools.account_record import save_unfilled_orders

if TYPE_CHECKING:
    from pykis import PyKis
    from pykis.scope.account import KisAccount
    from pykis.scope.stock import KisStock

MAX_STOCKS_ALLOWED = 20

OrderSide = Literal["buy", "sell"]
OrderCallable = Callable[..., Any]


# ---------------------------------------------------------------------------
# 로깅 및 공통 유틸
# ---------------------------------------------------------------------------

logger = logging.getLogger(__name__)

class _VerboseLog:
    """반복되는 verbose 메시지를 모아 둡니다."""

    SUBSCRIBE = "호가 구독 시작: {symbol}"
    WS_READY = "웹소켓 연결 완료, 주문 이벤트 대기 시작"
    REMAINING_QTY = "[{symbol}] 잔여 수량 {qty_left}, 다음 틱 대기"
    TIMEOUT = "타임아웃으로 미체결 종목: {payload}"
    COMPLETE_QTY = "목표 수량 충족, 호가 구독 종료"
    COMPLETE_VALUE = "목표 달성, 호가 구독 종료"


def _print_with_timestamp(message: str) -> None:
    """표준 출력 앞에 타임스탬프를 붙입니다."""
    logger.info(message)


def _print_verbose(message: str, *, verbose: bool) -> None:
    """verbose 플래그가 True일 때만 메시지를 출력합니다."""
    if verbose:
        logger.info(message)


def _log_verbose(message: str, *, verbose: bool, **kwargs: Any) -> None:
    """verbose 모드에서만 지정한 템플릿 메시지를 출력합니다."""
    if verbose:
        logger.info(message.format(**kwargs))


def _notify_unfilled_orders(
    unfilled: dict[str, Any],
    *,
    order_type: str = "qty",
    context: str = "",
) -> None:
    """
    미체결 주문 발생 시 Discord 알림을 전송합니다.
    
    Parameters
    ----------
    unfilled : dict
        미체결 종목 정보. qty 타입: {symbol: qty}, value 타입: {symbol: {current, target}}
    order_type : str
        "qty" 또는 "value"
    context : str
        추가 컨텍스트 (예: "Step 1", "Sell All")
    """
    if not unfilled:
        return
    
    try:
        lines = []
        for symbol, info in unfilled.items():
            if order_type == "qty":
                lines.append(f"  - {symbol}: {info}주 미체결")
            else:
                current = info.get("current", 0)
                target = info.get("target", 0)
                lines.append(f"  - {symbol}: 현재 {current:,.0f}원 / 목표 {target:,.0f}원")
        
        detail_str = "\n".join(lines)
        msg = f"[타임아웃] {context}\n미체결 종목:\n{detail_str}"
        
        send_notification(
            "trade_execution",
            msg,
            title="Order Timeout Warning",
            tags=("warning",),
        )
    except Exception as e:
        logger.error("Failed to send unfilled order notification: %s", e)



class RealtimeSubscriptionManager:
    """웹소켓 구독 및 완료 이벤트를 관리하는 헬퍼."""

    def __init__(self, *, verbose: bool):
        self.verbose = verbose
        self.lock = threading.Lock()
        self.all_done = threading.Event()
        self.tickets: dict[str, KisEventTicket] = {}

    def register(self, symbol: str, ticket: KisEventTicket) -> None:
        """구독 티켓을 저장하고 verbose 로그를 출력합니다."""
        self.tickets[symbol] = ticket
        _log_verbose(_VerboseLog.SUBSCRIBE, verbose=self.verbose, symbol=symbol)

    def wait(self, timeout: float | None, check_alive: Callable[[], bool] | None = None) -> None:
        """
        모든 주문이 완료될 때까지 대기합니다.
        check_alive: 주기적으로 호출하여 False 반환 시 대기 중단 (예: 장 마감 확인)
        """
        if check_alive is None:
            self.all_done.wait(timeout=timeout)
            return

        # check_alive가 있으면 폴링 방식으로 대기
        deadline = time.monotonic() + timeout if timeout is not None else None
        while not self.all_done.is_set():
            if deadline is not None and time.monotonic() >= deadline:
                break
            
            if not check_alive():
                if self.verbose:
                    _print_with_timestamp("check_alive returned False. Stopping wait.")
                break
            
            # 짧게 대기하며 이벤트 확인
            if self.all_done.wait(timeout=1.0):
                break

    def finalize(self) -> None:
        """남은 모든 구독을 해제합니다."""
        for symbol, ticket in self.tickets.items():
            try:
                ticket.unsubscribe()
            except Exception as e:
                logger.debug("Failed to unsubscribe %s: %s", symbol, e)

    def complete(self, symbol: str, *, active: dict[str, Any], message: str) -> None:
        """특정 심볼에 대한 구독을 종료합니다."""
        _complete_subscription(
            symbol,
            active=active,
            tickets=self.tickets,
            all_done=self.all_done,
            verbose=self.verbose,
            message=message,
        )


def _append_error(
    errors: list[dict[str, Any]],
    *,
    symbol: str,
    market: str | None,
    qty: int,
    price: Any,
    exc: Exception,
) -> None:
    """errors 리스트에 일관된 포맷으로 오류를 추가합니다."""
    errors.append(
        {"symbol": symbol, "market": market, "qty": qty, "price": price, "error": repr(exc)}
    )


def _update_qty_state(
    *,
    symbol: str,
    order_qty: int,
    remaining: dict[str, int],
    manager: RealtimeSubscriptionManager,
) -> None:
    """잔여 수량을 갱신하고 완료 시 구독을 해제합니다."""
    qty_left = remaining.get(symbol, 0) - order_qty
    if qty_left <= 0:
        manager.complete(
            symbol,
            active=remaining,
            message=_VerboseLog.COMPLETE_QTY,
        )
    else:
        remaining[symbol] = qty_left
        _log_verbose(
            _VerboseLog.REMAINING_QTY,
            verbose=manager.verbose,
            symbol=symbol,
            qty_left=qty_left,
        )


def _finalize_value_progress(
    *,
    symbol: str,
    state: dict[str, dict[str, Any]],
    target: dict[str, Any],
    goal_met: bool,
    eager_complete: bool,
    manager: RealtimeSubscriptionManager,
    verbose: bool,
) -> None:
    """평가금액 기반 주문의 완료 여부를 판단합니다."""
    if goal_met or eager_complete:
        manager.complete(
            symbol,
            active=state,
            message=_VerboseLog.COMPLETE_VALUE,
        )
        return

    if verbose:
        _print_verbose(
            f"[{symbol}] 진행 상황 | current={target['current_value']} "
            f"target={target['target_value']}",
            verbose=verbose,
        )


# ---------------------------------------------------------------------------
# KIS 객체 래퍼 및 주문 기록
# ---------------------------------------------------------------------------


def _get_kis_instance(account: "KisAccount") -> "PyKis":
    """계좌 스코프에서 PyKis 인스턴스를 안전하게 추출합니다."""
    kis = getattr(account, "kis", None)
    if kis is None:
        raise RuntimeError("account에서 PyKis 인스턴스를 찾을 수 없습니다.")
    return kis


def _orderbook_key(side: OrderSide) -> str:
    """주문 방향에 맞는 호가 키를 반환합니다."""
    return "asks" if side == "buy" else "bids"


def _order_callable(account: "KisAccount", side: OrderSide) -> OrderCallable:
    """주문 방향별 account API 메서드를 선택합니다."""
    return account.buy if side == "buy" else account.sell


def _complete_subscription(
    symbol: str,
    *,
    active: dict[str, Any],
    tickets: dict[str, KisEventTicket],
    all_done: threading.Event,
    verbose: bool,
    message: str | None = None,
) -> None:
    """공통 구독 해제 헬퍼."""
    active.pop(symbol, None)
    ticket = tickets.pop(symbol, None)
    if ticket:
        ticket.unsubscribe()
    if verbose and message:
        _print_verbose(f"[{symbol}] {message}", verbose=verbose)
        if active:
            _print_verbose(f"남은 종목: {active.keys()}", verbose=verbose)
    if not active:
        all_done.set()
        _print_verbose("모든 주문 완료", verbose=verbose)


def _record_order(
    *,
    orders: list[Any],
    symbol: str,
    market: str | None,
    price: Any,
    qty: int,
    dry_run: bool,
    virtual: bool = False,
    executed_order: Any | None = None,
) -> None:
    """주문 성공 시 orders 리스트에 결과를 기록합니다."""
    if virtual or dry_run:
        orders.append(
            {
                "symbol": symbol,
                "market": market,
                "price": price,
                "qty": qty,
                "status": "mock" if virtual else "dry_run",
            }
        )
        return

    if executed_order is None:
        raise ValueError("executed_order must be provided for real orders.")

    orders.append(executed_order)


# ---------------------------------------------------------------------------
# 실시간 호가 기반 주문 처리
# ---------------------------------------------------------------------------


def retry_execution(
    func: Callable[[], Any],
    max_retries: int,
    context: str,
    verbose: bool = False,
) -> tuple[bool, Any]:
    """
    주어진 함수를 최대 max_retries만큼 재시도합니다.
    
    Returns
    -------
    tuple[bool, Any]
        (성공 여부, 결과값)
        실패 시 (False, None) 반환
    """
    for attempt in range(max_retries):
        try:
            result = func()
            return True, result
        except Exception as e:
            if attempt < max_retries - 1:
                time.sleep(1)
            else:
                if verbose:
                    _print_verbose(f"[{context}] 최종 실패: {e}", verbose=verbose)
    return False, None

def get_balance_safe(
    account: "KisAccount", 
    max_retries: int = 10, 
    verbose: bool = False,
    country: COUNTRY_TYPE | None = None
) -> Any:
    """
    account.balance()를 안전하게 호출합니다 (재시도 로직 포함).
    """
    def _fetch():
        return account.balance(country=country)
        
    success, result = retry_execution(_fetch, max_retries, "잔고 조회", verbose)
    if success:
        return result
    else:
        raise Exception("잔고 조회에 최종 실패했습니다.")

def _is_order_pending(order: Any, max_retries: int = 3) -> bool:
    """
    주문 객체의 pending 속성을 안전하게 확인합니다.
    재시도 후에도 실패하면 안전을 위해 True(미체결)를 반환합니다.
    """
    def _check():
        return order.pending

    success, is_pending = retry_execution(_check, max_retries, "주문 상태 확인", verbose=False)
    
    if success:
        return is_pending
    
    # 최종 실패 시 안전하게 True(미체결) 반환 및 로그
    logger.warning(f"Failed to check order status after {max_retries} attempts. Assuming pending.")
    return True

def _process_qty_orders(
    account: "KisAccount",
    stocks: dict[str, tuple["KisStock", int]],
    *,
    side: OrderSide,
    max_fill_ratio: float,
    dry_run: bool,
    timeout: float | None,
    verbose: bool = False,
    check_alive: Callable[[], bool] | None = None,
    max_sub_retries: int = 10,
) -> tuple[list[Any], list[dict[str, Any]]]:
    if not stocks:
        return [], []
    if len(stocks) > MAX_STOCKS_ALLOWED:
        raise ValueError(f"stocks는 최대 {MAX_STOCKS_ALLOWED}개까지만 허용됩니다. (현재: {len(stocks)}개)")

    if not 0 < max_fill_ratio <= 1:
        raise ValueError("max_fill_ratio must be within (0, 1].")

    kis = _get_kis_instance(account)

    remaining = {symbol: qty for symbol, (_, qty) in stocks.items() if qty > 0}
    if not remaining:
        return [], []

    if verbose:
        _print_verbose(
            f"[qty] 총 {len(remaining)}개 종목 주문 준비 | side={side} "
            f"max_fill_ratio={max_fill_ratio:.2f} dry_run={dry_run} timeout={timeout}",
            verbose=verbose,
        )
        for symbol, (stock_obj, qty) in stocks.items():
            if qty <= 0:
                continue
            market = getattr(stock_obj, "market", None)
            _print_verbose(f"  - {symbol}@{market}: target_qty={qty}", verbose=verbose)

    orders: list[Any] = []
    errors: list[dict[str, Any]] = []
    manager = RealtimeSubscriptionManager(verbose=verbose)
    book_key = _orderbook_key(side)
    order_fn = _order_callable(account, side)

    def make_handler(symbol: str):
        stock_obj, _ = stocks[symbol]
        market = getattr(stock_obj, "market", None)

        def handler(sender: KisWebsocketClient, e: KisSubscriptionEventArgs[KisRealtimeOrderbook]):
            quotes = getattr(e.response, book_key, None) or []
            if not quotes:
                return

            # 1호가 (최우선 호가)
            q1 = quotes[0]
            p1 = getattr(q1, "price", None)
            v1 = getattr(q1, "volume", 0)
            
            if p1 is None or v1 <= 0:
                return

            # 2호가 (차선 호가, 존재 시)
            # 시장가 매도/매수 시 슬리피지를 줄이기 위해 2호가까지 물량을 확인하거나
            # 2호가 가격으로 주문을 내는 전략을 사용할 수 있음
            if len(quotes) > 1:
                q2 = quotes[1]
                p2 = getattr(q2, "price", None)
                v2 = getattr(q2, "volume", 0)
            else:
                p2, v2 = p1, 0

            # 가격 결정 전략:
            # - 매수/매도 모두 상대방이 받아줄 수 있는 유리한 가격(2호가)을 우선 고려
            # - 2호가가 없으면 1호가 사용
            # - 물량은 1호가와 2호가 잔량을 합산하여 체결 가능성 판단
            price = p2 if p2 is not None else p1
            total_vol = v1 + v2

            with manager.lock:
                qty_left = remaining.get(symbol, 0)
                if qty_left <= 0:
                    return

                if verbose:
                    _print_verbose(
                        f"[{symbol}] tick p1={p1} v1={v1} p2={p2} v2={v2} total_vol={total_vol} qty_left={qty_left}",
                        verbose=verbose,
                    )

                fill_cap = max(1, int(math.floor(total_vol * max_fill_ratio)))
                order_qty = min(qty_left, fill_cap)
                if order_qty <= 0:
                    if verbose:
                        _print_verbose(
                            f"[{symbol}] 주문 불가 | fill_cap={fill_cap} qty_left={qty_left}",
                            verbose=verbose,
                        )
                    return

                if verbose:
                    _print_verbose(
                        f"[{symbol}] fill_cap={fill_cap} order_qty={order_qty} "
                        f"ratio={max_fill_ratio:.2f}",
                        verbose=verbose,
                    )

                order_success = False
                try:
                    if dry_run:
                        if verbose:
                            _print_verbose(
                                f"시뮬레이션 주문: {symbol} {market} {price} {order_qty} {side} "
                                f"잔량→{qty_left - order_qty}",
                                verbose=verbose,
                            )
                        _record_order(
                            orders=orders,
                            symbol=symbol,
                            market=market,
                            price=price,
                            qty=order_qty,
                            dry_run=True,
                        )
                    else:
                        if verbose:
                            _print_verbose(
                                f"주문 실행: {symbol} {market} {price} {order_qty} {side} "
                                f"잔량→{qty_left - order_qty}",
                                verbose=verbose,
                            )
                        executed = order_fn(
                            market=market,
                            symbol=symbol,
                            price=price,
                            qty=order_qty,
                        )
                        _record_order(
                            orders=orders,
                            symbol=symbol,
                            market=market,
                            price=price,
                            qty=order_qty,
                            dry_run=False,
                            executed_order=executed,
                        )
                    order_success = True
                except Exception as exc:
                    error_msg = str(exc)
                    if "모의투자 주문처리가 안되었습니다(매매불가 종목)" in error_msg:
                        if verbose:
                            _print_verbose(
                                f"[{symbol}] 모의투자 주문처리가 안되었습니다(매매불가 종목)",
                                verbose=verbose,
                            )
                        _record_order(
                            orders=orders,
                            symbol=symbol,
                            market=market,
                            price=price,
                            qty=order_qty,
                            dry_run=dry_run,
                            virtual=True,
                        )
                        order_success = True
                    else:
                        if verbose:
                            _print_verbose(
                                f"주문 실패: {symbol} {market} {price} {order_qty} {side} "
                                f"error={repr(exc)}",
                                verbose=verbose,
                            )
                        _append_error(
                            errors,
                            symbol=symbol,
                            market=market,
                            qty=order_qty,
                            price=price,
                            exc=exc,
                        )
                        return

                if not order_success:
                    return

                _update_qty_state(
                    symbol=symbol,
                    order_qty=order_qty,
                    remaining=remaining,
                    manager=manager,
                )

        return handler

    for symbol in list(remaining.keys()):
        stock_obj, _ = stocks.get(symbol, (None, None))
        if stock_obj is None:
            continue
        current_stock = stock_obj
        def subscribe():
            ticket = current_stock.on("orderbook", make_handler(symbol))
            manager.register(symbol, ticket)

        if not retry_execution(subscribe, max_sub_retries, f"{symbol} 호가 구독", verbose)[0]:
            remaining.pop(symbol, None)

    try:
        kis.websocket.ensure_connected()
        _log_verbose(_VerboseLog.WS_READY, verbose=verbose)
        manager.wait(timeout=timeout, check_alive=check_alive)
    finally:
        manager.finalize()
    
    with manager.lock:
        if remaining:
            _log_verbose(_VerboseLog.TIMEOUT, verbose=verbose, payload=remaining)
            _notify_unfilled_orders(remaining, order_type="qty", context=f"{side.upper()} qty orders")
            save_unfilled_orders(remaining, side=side, order_type="qty", context=f"{side.upper()} qty orders")
            for symbol, qty in remaining.items():
                errors.append({
                    "type": "unfilled",
                    "symbol": symbol,
                    "qty": qty,
                    "side": side,
                    "order_type": "qty",
                })

    return orders, errors


def _process_value_orders(
    account: "KisAccount",
    stocks: dict[str, tuple["KisStock", float, float]],
    *,
    side: OrderSide,
    max_fill_ratio: float,
    dry_run: bool,
    timeout: float | None,
    verbose: bool = False,
    check_alive: Callable[[], bool] | None = None,
    max_sub_retries: int = 10,
) -> tuple[list[Any], list[dict[str, Any]]]:
    if not stocks:
        return [], []
    if len(stocks) > MAX_STOCKS_ALLOWED:
        raise ValueError(f"stocks는 최대 {MAX_STOCKS_ALLOWED}개까지만 허용됩니다. (현재: {len(stocks)}개)")

    if not 0 < max_fill_ratio <= 1:
        raise ValueError("max_fill_ratio must be within (0, 1].")

    kis = _get_kis_instance(account)

    state: dict[str, dict[str, Any]] = {}
    for symbol, entry in stocks.items():
        if not isinstance(entry, (tuple, list)) or len(entry) != 3:
            if verbose:
                _print_verbose(
                    f"[{symbol}] 입력 형식 오류: (stock_obj, current_value, target_value) 필요",
                    verbose=verbose,
                )
            continue

        stock_obj, current_raw, target_raw = entry
        if stock_obj is None:
            continue

        market = getattr(stock_obj, "market", None)
        if market is None:
            continue

        try:
            current_value = float(current_raw)
            target_value = float(target_raw)
        except (TypeError, ValueError):
            if verbose:
                _print_verbose(
                    f"[{symbol}] value 변환 실패: {current_raw}, {target_raw}",
                    verbose=verbose,
                )
            continue

        if side == "sell":
            if current_value <= target_value:
                continue
        else:
            if current_value >= target_value:
                continue

        state[symbol] = {
            "stock": stock_obj,
            "market": str(market),
            "current_value": current_value,
            "target_value": target_value,
        }

    if not state:
        return [], []

    if verbose:
        _print_verbose(
            f"[value] 총 {len(state)}개 종목 주문 준비 | side={side} "
            f"max_fill_ratio={max_fill_ratio:.2f} dry_run={dry_run} timeout={timeout}",
            verbose=verbose,
        )
        for symbol, target in state.items():
            _print_verbose(
                f"  - {symbol}@{target['market']}: "
                f"current={target['current_value']} target={target['target_value']}",
                verbose=verbose,
            )

    orders: list[Any] = []
    errors: list[dict[str, Any]] = []
    manager = RealtimeSubscriptionManager(verbose=verbose)
    book_key = _orderbook_key(side)
    order_fn = _order_callable(account, side)

    def make_handler(symbol: str):
        def handler(sender: KisWebsocketClient, e: KisSubscriptionEventArgs[KisRealtimeOrderbook]):
            quotes = getattr(e.response, book_key, None) or []
            if not quotes:
                return

            # 1호가
            q1 = quotes[0]
            p1 = getattr(q1, "price", None)
            v1 = getattr(q1, "volume", 0)
            
            if p1 is None or v1 <= 0:
                return

            # 2호가 (존재 시)
            if len(quotes) > 1:
                q2 = quotes[1]
                p2 = getattr(q2, "price", None)
                v2 = getattr(q2, "volume", 0)
            else:
                p2, v2 = p1, 0

            # 가격: 2호가 우선 (없으면 1호가)
            # 물량: 1+2호가 합산
            price = p2 if p2 is not None else p1
            total_vol = v1 + v2
            
            price_f = float(price)

            with manager.lock:
                target = state.get(symbol)
                if not target:
                    return

                current_value = float(target["current_value"])
                target_value = float(target["target_value"])

                if verbose:
                    _print_verbose(
                        f"[{symbol}] tick p1={p1} v1={v1} p2={p2} v2={v2} total_vol={total_vol} "
                        f"current={current_value} target={target_value}",
                        verbose=verbose,
                    )

                # 목표 가치와의 차이 계산
                # sell: 현재 가치가 목표보다 높으므로 차이만큼 매도
                # buy: 현재 가치가 목표보다 낮으므로 차이만큼 매수
                value_gap = current_value - target_value if side == "sell" else target_value - current_value
                if value_gap <= 0:
                    manager.complete(
                        symbol,
                        active=state,
                        message=_VerboseLog.COMPLETE_VALUE,
                    )
                    return

                # 체결 가능한 최대 수량 계산 (호가 잔량 * 비율)
                fill_cap = max(1, int(math.floor(total_vol * max_fill_ratio)))
                if fill_cap <= 0:
                    if verbose:
                        _print_verbose(
                            f"[{symbol}] 주문 불가 | fill_cap={fill_cap} volume={total_vol}",
                            verbose=verbose,
                        )
                    return

                # 필요 수량 계산
                if side == "sell":
                    # 매도 시에는 목표 금액을 맞추기 위해 올림 처리하여 충분히 매도
                    order_need = max(1, int(math.ceil(value_gap / price_f)))
                else:
                    # 매수 시에는 목표 금액을 초과하지 않도록 내림 처리
                    order_need = int(math.floor(value_gap / price_f))
                    if order_need <= 0:
                        manager.complete(
                            symbol,
                            active=state,
                            message="추가 매수 시 목표 초과, 호가 구독 종료",
                        )
                        return

                eager_complete = order_need <= fill_cap
                order_qty = order_need if eager_complete else fill_cap

                market = target["market"]

                if verbose:
                    _print_verbose(
                        f"[{symbol}] fill_cap={fill_cap} order_need={order_need} "
                        f"order_qty={order_qty}",
                        verbose=verbose,
                    )

                order_success = False

                try:
                    if dry_run:
                        if verbose:
                            simulated_value = current_value + (-1 if side == "sell" else 1) * order_qty * price_f
                            _print_verbose(
                                f"시뮬레이션 주문: {symbol} {market} {price_f} {order_qty} {side} "
                                f"value→{simulated_value}",
                                verbose=verbose,
                            )
                        _record_order(
                            orders=orders,
                            symbol=symbol,
                            market=market,
                            price=price,
                            qty=order_qty,
                            dry_run=True,
                        )
                    else:
                        if verbose:
                            _print_verbose(
                                f"주문 실행: {symbol} {market} {price_f} {order_qty} {side}",
                                verbose=verbose,
                            )
                        executed = order_fn(
                            market=market,
                            symbol=symbol,
                            price=price,
                            qty=order_qty,
                        )
                        _record_order(
                            orders=orders,
                            symbol=symbol,
                            market=market,
                            price=price,
                            qty=order_qty,
                            dry_run=False,
                            executed_order=executed,
                        )
                    order_success = True
                except Exception as exc:
                    error_msg = str(exc)
                    if "모의투자 주문처리가 안되었습니다(매매불가 종목)" in error_msg:
                        if verbose:
                            _print_verbose(
                                f"[{symbol}] 모의투자 주문처리가 안되었습니다(매매불가 종목)",
                                verbose=verbose,
                            )
                        _record_order(
                            orders=orders,
                            symbol=symbol,
                            market=market,
                            price=price,
                            qty=order_qty,
                            dry_run=dry_run,
                            virtual=True,
                        )
                        order_success = True
                    else:
                        if verbose:
                            _print_verbose(
                                f"주문 실패: {symbol} {market} {price_f} {order_qty} {side} "
                                f"error={repr(exc)}",
                                verbose=verbose,
                            )
                        _append_error(
                            errors,
                            symbol=symbol,
                            market=market,
                            qty=order_qty,
                            price=price,
                            exc=exc,
                        )
                        return

                if not order_success:
                    return

                value_delta = order_qty * price_f
                if side == "sell":
                    target["current_value"] = max(current_value - value_delta, 0.0)
                    goal_met = target["current_value"] <= target_value
                else:
                    target["current_value"] = min(current_value + value_delta, target_value)
                    goal_met = target["current_value"] >= target_value - 1e-6

                _finalize_value_progress(
                    symbol=symbol,
                    state=state,
                    target=target,
                    goal_met=goal_met,
                    eager_complete=eager_complete,
                    manager=manager,
                    verbose=verbose,
                )

        return handler

    for symbol in list(state.keys()):
        target = state[symbol]
        stock_obj = target["stock"]
        def subscribe():
            ticket = stock_obj.on("orderbook", make_handler(symbol))
            manager.register(symbol, ticket)

        if not retry_execution(subscribe, max_sub_retries, f"{symbol} 호가 구독", verbose)[0]:
            state.pop(symbol, None)

    try:
        kis.websocket.ensure_connected()
        _log_verbose(_VerboseLog.WS_READY, verbose=verbose)
        manager.wait(timeout=timeout, check_alive=check_alive)
    finally:
        manager.finalize()

    with manager.lock:
        if state:
            pending = {
                symbol: {
                    "current": target["current_value"],
                    "target": target["target_value"],
                }
                for symbol, target in state.items()
            }
            _log_verbose(_VerboseLog.TIMEOUT, verbose=verbose, payload=pending)
            _notify_unfilled_orders(pending, order_type="value", context=f"{side.upper()} value orders")
            save_unfilled_orders(pending, side=side, order_type="value", context=f"{side.upper()} value orders")
            for symbol, target in state.items():
                errors.append({
                    "type": "unfilled",
                    "symbol": symbol,
                    "current_value": target["current_value"],
                    "target_value": target["target_value"],
                    "side": side,
                    "order_type": "value",
                })

    return orders, errors


# ---------------------------------------------------------------------------
# 공개 API
# ---------------------------------------------------------------------------


def sell_qty(
    account: "KisAccount",
    stocks: dict[str, tuple["KisStock", int]],
    *,
    max_fill_ratio: float,
    dry_run: bool = False,
    timeout: float | None = None,
    verbose: bool = False,
    check_alive: Callable[[], bool] | None = None,
    max_sub_retries: int = 10,
) -> tuple[list[Any], list[dict[str, Any]]]:
    """
    실시간 호가 기반으로 지정 종목을 매도합니다.

    Parameters
    ----------
    account : KisAccount
        주문을 실행할 계좌 스코프.
    stocks : dict[str, tuple[KisStock, int]]
        `{symbol: (KisStock, orderable_qty)}` 형식의 매도 대상.
    max_fill_ratio : float
        틱 당 체결에 사용할 최대 비중(0~1].
    dry_run : bool, default False
        True면 주문을 시뮬레이션만 수행.
    timeout : float | None
        실시간 체결을 기다릴 최대 시간(초).
    verbose : bool, default False
        내부 진행 상황을 stdout에 출력.

    Returns
    -------
    tuple[list[Any], list[dict[str, Any]]]
        - orders: 실행(또는 시뮬레이션)된 주문 목록
        - errors: 주문 실패 정보
    """
    return _process_qty_orders(
        account=account,
        stocks=stocks,
        side="sell",
        max_fill_ratio=max_fill_ratio,
        dry_run=dry_run,
        timeout=timeout,
        verbose=verbose,
        check_alive=check_alive,
        max_sub_retries=max_sub_retries,
    )

def buy_qty(
    account: "KisAccount",
    stocks: dict[str, tuple["KisStock", int]],
    *,
    max_fill_ratio: float,
    dry_run: bool = False,
    timeout: float | None = None,
    verbose: bool = False,
    check_alive: Callable[[], bool] | None = None,
    max_sub_retries: int = 10,
) -> tuple[list[Any], list[dict[str, Any]]]:
    """
    실시간 호가 기반으로 지정 종목을 매수합니다.

    stocks 파라미터는 `{symbol: (KisStock, target_qty)}` 형식을 따릅니다.
    나머지 파라미터와 반환값은 `sell_qty`와 동일합니다.
    """
    return _process_qty_orders(
        account=account,
        stocks=stocks,
        side="buy",
        max_fill_ratio=max_fill_ratio,
        dry_run=dry_run,
        timeout=timeout,
        verbose=verbose,
        check_alive=check_alive,
        max_sub_retries=max_sub_retries,
    )


def sell_value(
    account: "KisAccount",
    stocks: dict[str, tuple["KisStock", float, float]],
    *,
    max_fill_ratio: float,
    dry_run: bool = False,
    timeout: float | None = None,
    verbose: bool = False,
    check_alive: Callable[[], bool] | None = None,
    max_sub_retries: int = 10,
) -> tuple[list[Any], list[dict[str, Any]]]:
    """
    평가금액 기준으로 초과 보유분을 정리합니다.

    Parameters
    ----------
    stocks : dict[str, tuple[KisStock, float, float]]
        `{symbol: (KisStock, current_value, target_value)}` 형식.
        - current_value: 현재 평가금액
        - target_value: 매도 이후 목표 평가금액(미만이 되면 중단)

    나머지 인수/반환 설명은 `sell_qty`와 동일합니다.
    """

    return _process_value_orders(
        account=account,
        stocks=stocks,
        side="sell",
        max_fill_ratio=max_fill_ratio,
        dry_run=dry_run,
        timeout=timeout,
        verbose=verbose,
        check_alive=check_alive,
        max_sub_retries=max_sub_retries,
    )


def buy_value(
    account: "KisAccount",
    stocks: dict[str, tuple["KisStock", float, float]],
    *,
    max_fill_ratio: float,
    dry_run: bool = False,
    timeout: float | None = None,
    verbose: bool = False,
    check_alive: Callable[[], bool] | None = None,
    max_sub_retries: int = 10,
) -> tuple[list[Any], list[dict[str, Any]]]:
    """
    평가금액 기준으로 부족한 종목을 채웁니다.

    stocks 형식은 `{symbol: (KisStock, current_value, target_value)}`이며
    target_value를 넘지 않는 선에서 매수를 진행합니다.
    """

    return _process_value_orders(
        account=account,
        stocks=stocks,
        side="buy",
        max_fill_ratio=max_fill_ratio,
        dry_run=dry_run,
        timeout=timeout,
        verbose=verbose,
        check_alive=check_alive,
        max_sub_retries=max_sub_retries,
    )

def _wait_until_filled(
    order_objs: Iterable[Any],
    *,
    poll_interval: float = 1.0,
    timeout: float | None = None,
    check_alive: Callable[[], bool] | None = None,
    max_retries: int = 3,
) -> None:
    """
    지정된 주문 객체들이 체결 완료 상태가 될 때까지 대기합니다.

    Steps
    -----
    - `pending` 속성이 True인 주문만 추려 반복 확인합니다.
    - `poll_interval` 간격으로 상태를 재확인합니다.
    - `timeout`을 초과하면 `TimeoutError`를 발생시킵니다.
    - `check_alive`가 False를 반환하면 즉시 중단합니다.
    """
    if not order_objs:
        return

    deadline = time.monotonic() + timeout if timeout is not None else None
    # hasattr check removed; assume all order objects are valid KisOrder-like objects
    pending = list(order_objs)

    while pending:
        if check_alive and not check_alive():
            raise TimeoutError("Market check failed (market closed).")

        next_pending = []
        for order in pending:
            if _is_order_pending(order, max_retries=max_retries):
                next_pending.append(order)
        
        pending = next_pending
        if not pending:
            break

        if deadline is not None and time.monotonic() >= deadline:
            raise TimeoutError("일부 주문이 제한 시간 내 체결되지 않았습니다.")
        time.sleep(poll_interval)


def _execute_with_retry(
    kis: "PyKis",
    max_retries: int,
    execution_timeout: float,
    verbose: bool,
    dry_run: bool,
    step_name: str,
    attempt_func: Callable[[], tuple[list[Any], list[dict[str, Any]]]],
    check_alive: Callable[[], bool] | None = None,
    max_sub_retries: int = 3,
) -> tuple[list[Any], list[dict[str, Any]]]:
    """
    주문 생성 및 체결 대기 로직을 재시도와 함께 실행하는 헬퍼 함수입니다.
    """
    all_orders: list[Any] = []
    all_errors: list[dict[str, Any]] = []
    retries = 0

    while retries < max_retries:
        if check_alive and not check_alive():
            if verbose:
                _print_with_timestamp(f"{step_name}: Market check failed. Stopping retry loop.")
            break

        orders, errors = attempt_func()
        all_orders.extend(orders)
        all_errors.extend(errors)

        has_unfilled = any(e.get("type") == "unfilled" for e in errors)

        if not orders:
            if has_unfilled and retries < max_retries - 1:
                retries += 1
                if verbose:
                    _print_with_timestamp(
                        f"{step_name}: 호가 부적합으로 주문 미생성, 재시도합니다. ({retries}/{max_retries})"
                    )
                time.sleep(1)
                continue
            if verbose:
                _print_with_timestamp(f"{step_name}: 주문이 생성되지 않았거나 대상이 없습니다.")
            break

        if dry_run or kis.virtual:
            break

        try:
            _wait_until_filled(
                orders, 
                timeout=execution_timeout, 
                check_alive=check_alive,
                max_retries=max_sub_retries
            )
            break
        except TimeoutError:
            if check_alive and not check_alive():
                if verbose:
                    _print_with_timestamp(f"{step_name}: Market check failed during wait. Stopping retry loop.")
                # 대기 중이던 주문 취소 시도
                for order in orders:
                    if _is_order_pending(order, max_retries=max_sub_retries):
                        try:
                            order.cancel()
                        except Exception as e:
                            logger.debug("Failed to cancel order during market close: %s", e)
                break

            retries += 1
            if verbose:
                _print_with_timestamp(
                    f"{step_name}: 주문 미체결 타임아웃, 취소 후 재시도합니다. ({retries}/{max_retries})"
                )
            for order in orders:
                if _is_order_pending(order, max_retries=max_sub_retries):
                    try:
                        order.cancel()
                    except Exception as e:
                        if verbose:
                            _print_with_timestamp(f"주문 취소 실패: {e}")
            time.sleep(1)

    return all_orders, all_errors

def rebalance(
    kis: "PyKis",
    stocks_selected: Mapping[str, "KisStock"],
    *,
    cash_ratio: float,
    dry_run: bool = False,
    max_fill_ratio: float = 0.8,
    order_timeout: float | None = None,
    execution_timeout: float = 600.0,
    max_retries: int = 10,
    verbose: bool = False,
    check_alive: Callable[[], bool] | None = None,
    max_sub_retries: int = 10,
) -> dict[str, list[Any]]:
    """
    목표 종목을 동일 비중으로 보유하도록 계좌를 리밸런싱합니다.

    Workflow
    --------
    - 비목표 종목을 전량 매도합니다.
    - 목표 비중을 초과한 종목을 `sell_value`로 줄입니다.
    - 부족한 종목을 `buy_value`로 채우고, 필요 시 체결 완료까지 대기합니다.

    Parameters, Returns
    -------------------
    기존 문서와 동일하며 `orders`/`errors` 키를 가진 dict를 반환합니다.
    """
    if not 0 <= cash_ratio < 1:
        raise ValueError("cash_ratio는 0 이상 1 미만이어야 합니다.")

    target_codes = list(stocks_selected.keys())
    if not target_codes:
        raise ValueError("stocks_selected에는 최소 한 개의 종목이 필요합니다.")

    account = kis.account()

    def _snapshot_holdings(balance_obj) -> dict[str, Any]:
        snapshot: dict[str, Any] = {}
        for stock in getattr(balance_obj, "stocks", []):
            symbol = getattr(stock, "symbol", None)
            if symbol:
                snapshot[str(symbol)] = stock
        return snapshot

    orders: list[Any] = []
    errors: list[dict[str, Any]] = []

    # Step 1: 목표 목록에 없는 종목 전량 매도
    def step1_attempt() -> tuple[list[Any], list[dict[str, Any]]]:
        balance = get_balance_safe(account, max_retries=max_retries, verbose=verbose)
        holdings = _snapshot_holdings(balance)
        non_target_positions: dict[str, tuple["KisStock", int]] = {}
        
        for symbol, stock in holdings.items():
            if symbol in target_codes:
                continue

            qty = int(getattr(stock, "orderable", getattr(stock, "qty", 0)) or 0)
            if qty <= 0:
                continue
            stock_scope = kis.stock(symbol)
            non_target_positions[symbol] = (stock_scope, qty)

        if not non_target_positions:
            return [], []

        return sell_qty(
            account=account,
            stocks=non_target_positions,
            max_fill_ratio=max_fill_ratio,
            dry_run=dry_run,
            timeout=order_timeout,
            verbose=verbose,
            check_alive=check_alive,
            max_sub_retries=max_sub_retries,
        )

    s1_orders, s1_errors = _execute_with_retry(
        kis=kis,
        max_retries=max_retries,
        execution_timeout=execution_timeout,
        verbose=verbose,
        dry_run=dry_run,
        step_name="Step 1",
        attempt_func=step1_attempt,
        check_alive=check_alive,
        max_sub_retries=max_sub_retries,
    )
    orders.extend(s1_orders)
    errors.extend(s1_errors)

    # Step 2: 목표 비중을 초과한 종목 차익 실현
    def step2_attempt() -> tuple[list[Any], list[dict[str, Any]]]:
        balance = get_balance_safe(account, max_retries=max_retries, verbose=verbose)
        holdings = _snapshot_holdings(balance)
        target_value = float(balance.amount) * (1 - cash_ratio) / len(target_codes)
        
        excess_positions: dict[str, tuple["KisStock", float, float]] = {}
        for symbol, stock in holdings.items():
            if symbol not in target_codes:
                continue

            qty = int(getattr(stock, "qty", 0))
            if qty <= 0:
                continue

            holding_value = float(getattr(stock, "amount", 0))
            if holding_value <= target_value:
                continue

            orderable_qty = int(getattr(stock, "orderable", qty))
            if orderable_qty <= 0:
                continue

            stock_scope = stocks_selected.get(symbol) or kis.stock(symbol)
            excess_positions[symbol] = (stock_scope, holding_value, target_value)

        if not excess_positions:
            return [], []

        return sell_value(
            account=account,
            stocks=excess_positions,
            max_fill_ratio=max_fill_ratio,
            dry_run=dry_run,
            timeout=order_timeout,
            verbose=verbose,
            check_alive=check_alive,
            max_sub_retries=max_sub_retries,
        )

    s2_orders, s2_errors = _execute_with_retry(
        kis=kis,
        max_retries=max_retries,
        execution_timeout=execution_timeout,
        verbose=verbose,
        dry_run=dry_run,
        step_name="Step 2",
        attempt_func=step2_attempt,
        check_alive=check_alive,
        max_sub_retries=max_sub_retries,
    )
    orders.extend(s2_orders)
    errors.extend(s2_errors)

    # Step 3: 부족한 종목을 목표 비중까지 매수
    def step3_attempt() -> tuple[list[Any], list[dict[str, Any]]]:
        balance = get_balance_safe(account, max_retries=max_retries, verbose=verbose)
        holdings = _snapshot_holdings(balance)
        target_value = float(balance.amount) * (1 - cash_ratio) / len(target_codes)
        
        buy_candidates: dict[str, tuple["KisStock", float, float]] = {}
        for symbol, stock in stocks_selected.items():
            holding = holdings.get(symbol)
            holding_value = float(getattr(holding, "amount", 0)) if holding else 0.0

            if holding_value >= target_value:
                continue

            buy_candidates[symbol] = (stock, holding_value, target_value)

        if not buy_candidates:
            return [], []

        return buy_value(
            account=account,
            stocks=buy_candidates,
            max_fill_ratio=max_fill_ratio,
            dry_run=dry_run,
            timeout=order_timeout,
            verbose=verbose,
            check_alive=check_alive,
            max_sub_retries=max_sub_retries,
        )

    s3_orders, s3_errors = _execute_with_retry(
        kis=kis,
        max_retries=max_retries,
        execution_timeout=execution_timeout,
        verbose=verbose,
        dry_run=dry_run,
        step_name="Step 3",
        attempt_func=step3_attempt,
        check_alive=check_alive,
        max_sub_retries=max_sub_retries,
    )
    orders.extend(s3_orders)
    errors.extend(s3_errors)

    return {
        "orders": orders,
        "errors": errors,
    }

def sell_all(
    kis: "PyKis",
    *,
    country: COUNTRY_TYPE | None = None,
    dry_run: bool = False,
    max_fill_ratio: float = 0.8,
    order_timeout: float | None = None,
    execution_timeout: float = 600.0,
    max_retries: int = 10,
    verbose: bool = False,
    check_alive: Callable[[], bool] | None = None,
    max_sub_retries: int = 10,
) -> dict[str, list[Any]]:
    """
    보유 중인 모든 종목을 시장 호가 기반으로 정리합니다.

    Steps
    -----
    - 계좌 잔고에서 주문 가능 수량이 있는 종목을 수집합니다.
    - `sell_qty`를 호출해 일괄 매도합니다.
    - 체결되지 않은 주문은 대기 후 취소하고 재시도합니다.
    - 결과(`orders`, `errors`)를 반환합니다.
    """
    account = kis.account()

    def attempt() -> tuple[list[Any], list[dict[str, Any]]]:
        balance = get_balance_safe(account, max_retries=max_retries, verbose=verbose, country=country)
        stocks = {}

        for stock in getattr(balance, "stocks", []):
            symbol = getattr(stock, "symbol", None)
            qty = int(getattr(stock, "orderable", 0))
            if symbol and qty > 0:
                stocks[symbol] = (kis.stock(symbol), qty)

        if not stocks:
            return [], []

        return sell_qty(
            account=account,
            stocks=stocks,
            max_fill_ratio=max_fill_ratio,
            dry_run=dry_run,
            timeout=order_timeout,
            verbose=verbose,
            check_alive=check_alive,
            max_sub_retries=max_sub_retries,
        )

    all_orders, all_errors = _execute_with_retry(
        kis=kis,
        max_retries=max_retries,
        execution_timeout=execution_timeout,
        verbose=verbose,
        dry_run=dry_run,
        step_name="Sell All",
        attempt_func=attempt,
        check_alive=check_alive,
        max_sub_retries=max_sub_retries,
    )

    return {"orders": all_orders, "errors": all_errors}


__all__ = [
    "sell_qty",
    "buy_qty",
    "sell_value",
    "buy_value",
    "rebalance",
    "sell_all",
    "get_balance_safe",
]

def get_account_state(kis: PyKis) -> tuple[str, float]:
    """
    현재 계좌 상태를 반환합니다.
    주식 보유 수량이 하나라도 있으면 'STOCK', 아니면 'CASH'로 간주합니다.
    """
    account = kis.account()
    balance = get_balance_safe(account, verbose=True)
    
    has_stocks = False
    for stock in getattr(balance, "stocks", []):
        qty = int(getattr(stock, "qty", 0))
        if qty > 0:
            has_stocks = True
            break
            
    return "STOCK" if has_stocks else "CASH", float(balance.amount)


def execute_rebalance_safe(
    kis: PyKis,
    stocks_selected: Any,
    check_alive: Callable[[], bool],
    context: str = "",
    cash_ratio: float = 0.03,
    order_timeout: float = 600,
    execution_timeout: float = 600,
    max_sub_retries: int = 10,
) -> bool:
    """
    안전하게 리밸런싱(매수)을 수행합니다.
    예외 처리, 알림 전송, 타임아웃 설정을 캡슐화합니다.
    """
    try:
        rebalance(
            kis, 
            stocks_selected, 
            cash_ratio=cash_ratio, 
            verbose=True, 
            order_timeout=order_timeout,
            execution_timeout=execution_timeout,
            check_alive=check_alive,
            max_sub_retries=max_sub_retries
        )
        send_notification("trade_execution", f"{context} BUY executed.", title="Trade Complete", tags=("white_check_mark",))
        return True
    except Exception as e:
        logger.error(f"Error during {context} buy: {e}", exc_info=True)
        send_notification("trade_execution", f"Error during {context} buy: {e}", title="Trading Error", tags=("warning",))
        return False


def execute_sell_all_safe(
    kis: PyKis,
    check_alive: Callable[[], bool],
    context: str = "",
    order_timeout: float = 600,
    execution_timeout: float = 600,
    max_sub_retries: int = 10,
) -> bool:
    """
    안전하게 전량 매도를 수행합니다.
    예외 처리, 알림 전송, 타임아웃 설정을 캡슐화합니다.
    """
    try:
        sell_all(
            kis, 
            verbose=True, 
            order_timeout=order_timeout,
            execution_timeout=execution_timeout,
            check_alive=check_alive,
            max_sub_retries=max_sub_retries
        )
        send_notification("trade_execution", f"{context} SELL executed.", title="Trade Complete", tags=("white_check_mark",))
        return True
    except Exception as e:
        logger.error(f"Error during {context} sell: {e}", exc_info=True)
        send_notification("trade_execution", f"Error during {context} sell: {e}", title="Trading Error", tags=("warning",))
        return False

