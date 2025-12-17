import time
import os
import logging
from datetime import datetime
from pykis import PyKis, KisAuth
from tools.logger import setup_logging
from tools.market_watcher import (
    wait_until_market_open, wait_until_market_close, get_market_signal, 
    check_market_open_by_indexes, is_today_open_day, fetch_historical_indices,
    MarketMonitor
)
from tools.trading_utils import (
    rebalance, sell_all, get_balance_safe, retry_execution,
    get_account_state, execute_rebalance_safe, execute_sell_all_safe
)
from tools.selection_store import load_stock_selection
from tools.notifications import send_notification
from tools.quant_utils import create_stock_objects
from tools.account_record import save_initial_asset, save_final_asset, get_daily_asset, save_daily_orders, get_previous_final_asset, save_stock_performance, get_latest_stock_performance

# Configuration Constants
CASH_RATIO = 0.03
ORDER_TIMEOUT = 600
EXECUTION_TIMEOUT = 600
MARKET_CHECK_TIMEOUT = 180  # For check_alive callback
MARKET_WAIT_TIMEOUT = 180   # For main loop market active check

# Path Constants
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
LOG_DIR = os.path.join(BASE_DIR, "logs")
SECRETS_FILE = os.path.join(BASE_DIR, "secrets/real.json")

# Logger setup
logger = logging.getLogger("autorebalance")


def finalize_trading_day(kis: PyKis, initial_asset: float, transfer_amount: float):
    """
    장 마감 후 최종 리포트 작성 및 DB 저장을 수행합니다.
    에러가 발생하더라도 최대한 저장을 시도합니다.
    """
    try:
        logger.info("Finalizing trading day...")
        
        # 1. 최종 자산 조회
        try:
            balance = get_balance_safe(kis.account(), verbose=True)
            final_asset = float(balance.total)
            
            deposit_d2 = 0.0
            if 'KRW' in balance.deposits:
                deposit_d2 = float(balance.deposits['KRW'].d2_amount)
                
            save_final_asset(final_asset, deposit_d2)
            logger.info(f"Saved final asset to DB: {final_asset:,.0f}, D+2 Deposit: {deposit_d2:,.0f}")
        except Exception as e:
            logger.error(f"Error fetching/saving final asset: {e}")
            final_asset = initial_asset # Fallback to avoid division by zero if fetch fails
            deposit_d2 = 0.0

        # 2. 수익률 계산
        try:
            # 장초 대비
            diff_open = final_asset - initial_asset
            diff_open_rate = (diff_open / initial_asset * 100) if initial_asset > 0 else 0.0
            
            # 전일 종가 대비 (입출금 반영)
            prev_asset, _ = get_previous_final_asset()
            if prev_asset:
                base_asset = prev_asset + transfer_amount
                diff_prev = final_asset - base_asset
                diff_prev_rate = (diff_prev / base_asset * 100) if base_asset > 0 else 0.0
                prev_msg = f"Vs Prev: {diff_prev:+,.0f} ({diff_prev_rate:+.2f}%)"
                if transfer_amount != 0:
                    prev_msg += f" (Adj for transfer: {transfer_amount:+,.0f})"
            else:
                prev_msg = "Vs Prev: N/A"
        except Exception as e:
            logger.error(f"Error calculating profits: {e}")
            diff_open = 0.0
            diff_open_rate = 0.0
            prev_msg = "Vs Prev: Error"

        # 3. 금일 체결 내역 조회 및 저장
        today_orders = []
        try:
            def _fetch_orders():
                return kis.account().daily_orders(start=date.today())

            success, today_orders = retry_execution(_fetch_orders, max_retries=10, context="Fetching daily orders")
            if not success:
                logger.error("Failed to fetch daily orders. Skipping order report.")
                today_orders = []
            
            orders_to_save = []
            for order in today_orders:
                status = "체결" if order.executed_qty > 0 else "미체결"
                if order.canceled:
                    status = "취소"
                elif order.rejected:
                    status = f"거부({order.rejected_reason})"

                orders_to_save.append({
                    "order_number": str(order.order_number),
                    "time": order.time_kst.strftime("%H:%M:%S"),
                    "type": order.type,
                    "name": order.name,
                    "qty": int(order.qty),
                    "executed_qty": int(order.executed_qty),
                    "price": float(order.price or 0),
                    "status": status
                })
                
            if orders_to_save:
                save_daily_orders(orders_to_save)
                logger.info(f"Saved {len(orders_to_save)} orders to DB.")
        except Exception as e:
            logger.error(f"Error processing daily orders: {e}")

        # 4. 종목별 성과 저장 (누적 방식)
        try:
            # 이전 누적 데이터 로드
            prev_perf_map = get_latest_stock_performance()
            stock_perf_map = {}
            
            # 이전 데이터 복사 초기화
            for symbol, data in prev_perf_map.items():
                stock_perf_map[symbol] = {
                    "symbol": symbol,
                    "name": data["name"],
                    "invested_amount": data["invested_amount"],
                    "sell_amount": data["sell_amount"],
                    "current_value": 0.0,
                    "realized_profit": 0.0,
                    "quantity": 0
                }

            # 금일 체결 내역 반영
            for order in today_orders:
                if order.executed_qty <= 0:
                    continue
                    
                symbol = order.symbol
                amt = float(order.price) * int(order.executed_qty)
                
                if symbol not in stock_perf_map:
                    stock_perf_map[symbol] = {
                        "symbol": symbol,
                        "name": order.name,
                        "invested_amount": 0.0,
                        "sell_amount": 0.0,
                        "current_value": 0.0,
                        "realized_profit": 0.0,
                        "quantity": 0
                    }
                
                if order.type == "buy":
                    stock_perf_map[symbol]["invested_amount"] += amt
                elif order.type == "sell":
                    stock_perf_map[symbol]["sell_amount"] += amt

            # 현재 잔고 가치 업데이트
            try:
                balance = get_balance_safe(kis.account(), verbose=True)
                for stock in balance.stocks:
                    symbol = stock.symbol
                    
                    if symbol not in stock_perf_map:
                        stock_perf_map[symbol] = {
                            "symbol": symbol,
                            "name": stock.name,
                            "invested_amount": float(stock.purchase_amount),
                            "sell_amount": 0.0,
                            "current_value": 0.0,
                            "realized_profit": 0.0,
                            "quantity": 0
                        }
                    
                    stock_perf_map[symbol]["current_value"] = float(stock.amount)
                    stock_perf_map[symbol]["quantity"] = int(stock.quantity)
            except Exception as e:
                logger.error(f"Error updating stock values from balance: {e}")

            # 당일 실현 손익
            try:
                profits = kis.account().profits(start=date.today(), end=date.today())
                for order_profit in profits.orders:
                    symbol = order_profit.symbol
                    if symbol in stock_perf_map:
                        stock_perf_map[symbol]["realized_profit"] += float(order_profit.profit)
            except:
                pass 

            # DB 저장
            if stock_perf_map:
                save_stock_performance(list(stock_perf_map.values()))
                logger.info(f"Saved cumulative performance for {len(stock_perf_map)} stocks.")
                
        except Exception as e:
            logger.error(f"Error saving stock performance: {e}")

        # 5. 최종 리포트 전송
        try:
            market_close_time = datetime.now().strftime("%H:%M:%S")
            msg = f"""
Market Closed: {market_close_time}
Final: {final_asset:,.0f}
Vs Open: {diff_open:+,.0f} ({diff_open_rate:+.2f}%)
{prev_msg}
            """.strip()
            
            logger.info(msg)
            send_notification(
                "trade_execution", 
                msg, 
                title=f"Daily Report ({date.today()})", 
                tags=("chart_with_upwards_trend" if diff_open >= 0 else "chart_with_downwards_trend",)
            )
        except Exception as e:
            logger.error(f"Error sending final report: {e}")

    except Exception as e:
        logger.error(f"Critical error in finalize_trading_day: {e}", exc_info=True)


def main():
    setup_logging(LOG_DIR)

    # KIS 인스턴스 생성 (토큰 유지)
    kis = PyKis(
        KisAuth.load(SECRETS_FILE),
        keep_token=True
    )

    # Initialize tickets to None for safe cleanup
    ticket_kospi = None
    ticket_kosdaq = None
    
    # Initialize asset variables early for finally block
    initial_asset = 0.0
    transfer_amount = 0.0

    try:
        # 0. 휴장일 확인
        if not is_today_open_day(kis):
            msg = f"Today ({date.today()}) is a holiday. No trading."
            logger.info(msg)
            send_notification("trade_execution", msg, title="Market Closed", tags=("sleeping",))
            return

        # 1. 과거 지수 데이터 로드 (장 시작 전)
        logger.info("Fetching historical indices...")
        historical_indices = fetch_historical_indices(kis)
        logger.info("Historical indices loaded.")

        # 2. 초기 상태 확인 (장 시작 전)
        # 웹소켓 세션 클린업 (이전 세션이 남아있을 경우를 대비)
        try:
            logger.info("Ensuring single active websocket session...")

            kis.websocket.ensure_connected()
            
            if kis.websocket.connected:
                logger.info("Websocket session stable.")
            else:
                logger.info("Websocket disconnected. Retrying...")
                kis.websocket.ensure_connected()
                    
            logger.info("Websocket session established successfully.")
        except Exception as e:
            logger.warning(f"Session init warning: {e}")

        # 2. 초기 상태 확인 (장 시작 전)
        state, _ = get_account_state(kis)
        logger.info(f"Initial State: {state}")

        # 3. 웹소켓 모니터링 시작 (지속 연결)
        monitor = MarketMonitor()
        
        def make_handler(name, expected_code):
            def handler(sender, e):
                # 이벤트가 해당 지수의 것이 맞는지 확인 (필터링)
                if e.response.index_code != expected_code:
                    return
                monitor.update(name, e.response.price)
            return handler

        # 웹소켓 구독 (티켓 유지 필요)
        # on_domestic_index_price 호출 시 자동으로 재연결됨
        logger.info("Subscribing to KOSPI/KOSDAQ...")
        ticket_kospi = kis.websocket.on_domestic_index_price("KOSPI", make_handler("KOSPI", "0001"))
        ticket_kosdaq = kis.websocket.on_domestic_index_price("KOSDAQ", make_handler("KOSDAQ", "1001"))

        # 4. 장 시작 대기 (MarketMonitor 활용)
        logger.info(f"Waiting for market open (monitoring ticks)...")
        market_open_time = None
        while True:
            try:
                kis.websocket.ensure_connected()
            except Exception as e:
                logger.warning(f"WebSocket connection issue during wait: {e}. Retrying...")

            # 10초 간격으로 데이터 수신 여부 확인
            # timeout=None: 시간 관계없이 데이터 수신 여부만 확인
            if monitor.is_active(timeout=None):
                market_open_time = datetime.now()
                logger.info(f"Market open detected (ticks received).")
                break
            
            # 장 시작 전에는 10초마다 체크
            time.sleep(10)
            
        logger.info(f"Market is open.")
        
        # 5. 장초 자산 기록 (장 시작 직후)
        # DB에 저장된 장초 평가금이 있으면 그것을 사용 (재시작 시 일관성 유지)
        db_initial_asset, _, db_transfer_amount = get_daily_asset()
        
        # Fetch balance once after market open for initial asset calculation and stock ratio
        balance = get_balance_safe(kis.account(), verbose=True)

        if db_initial_asset is not None:
            initial_asset = db_initial_asset
            transfer_amount = db_transfer_amount
            logger.info(f"Loaded initial asset from DB: {initial_asset:,.0f} (Transfer: {transfer_amount:,.0f})")
        else:
            # DB에 없으면 현재 잔고로 기록
            initial_asset = float(balance.total)
            
            deposit_d2 = 0.0
            if 'KRW' in balance.deposits:
                deposit_d2 = float(balance.deposits['KRW'].d2_amount)
                
            # 입출금액 계산 (현재 D+2 예수금 - 전일 종가 D+2 예수금)
            # 자산 변동이 아닌 순수 현금 이동만 감지
            prev_asset_check, prev_d2_check = get_previous_final_asset()
            
            if prev_d2_check is not None:
                 diff = deposit_d2 - prev_d2_check
                 # 1원 이상의 변동만 입출금으로 기록 (부동소수점 오차 고려)
                 if abs(diff) > 1:
                     transfer_amount = diff
                     logger.info(f"Detected overnight transfer (D+2 diff): {transfer_amount:,.0f} KRW")
                
            save_initial_asset(initial_asset, deposit_d2, transfer_amount)
            logger.info(f"Saved initial asset to DB: {initial_asset:,.0f}, D+2: {deposit_d2:,.0f}, Transfer: {transfer_amount:,.0f}")
            
        logger.info(f"Asset at Start: {initial_asset:,.0f}")
        
        # 장 시작 알림 전송
        msg = f"Market Open: {market_open_time.strftime('%H:%M:%S')}\nInitial Asset: {initial_asset:,.0f} KRW"
        if transfer_amount != 0:
            msg += f"\n(Net Transfer: {transfer_amount:+,.0f})"
        send_notification("trade_execution", msg, title="Market Open & Initial Asset", tags=("moneybag",))

        # 6. 불완전 상태(Incomplete State) 확인 및 해결
        # 주식 비중이 0% 초과 90% 이하인 경우, 이전 매매가 중단된 것으로 간주하고 즉시 신호에 따라 매매 수행
        stock_value = sum(float(s.amount) for s in balance.stocks)
        stock_ratio = stock_value / initial_asset if initial_asset > 0 else 0.0
        
        logger.info(f"Current Stock Ratio: {stock_ratio*100:.2f}% (Value: {stock_value:,.0f})")
        
        if (0 < stock_ratio <= 0.9) and (initial_asset - stock_value > 100000):
            msg = f"Incomplete state detected (Stock Ratio: {stock_ratio*100:.2f}%). Resolving..."
            logger.info(msg)
            send_notification("trade_execution", msg, title="Incomplete State Resolution", tags=("wrench",))
            
            # 시그널 확인
            signal_data = get_market_signal(
                kis, 
                kospi_current=monitor.prices["KOSPI"],
                kosdaq_current=monitor.prices["KOSDAQ"],
                historical_data=historical_indices,
                verbose=True
            )
            signal = signal_data["signal"]
            details = signal_data["details"]
            logger.info(f"Resolution Signal: {signal}")
            
            if signal == "buy":
                kospi_reason = details["KOSPI"]["reason"]
                kosdaq_reason = details["KOSDAQ"]["reason"]
                logger.info(f"Signal is BUY (KOSPI: {kospi_reason}, KOSDAQ: {kosdaq_reason}). Executing rebalance to fill position...")
                df_selection = load_stock_selection(kis=kis)
                if df_selection.empty:
                    logger.info("No stocks selected. Skipping buy.")
                else:
                    stocks_selected = create_stock_objects(df_selection, kis)
                    execute_rebalance_safe(
                        kis, 
                        stocks_selected, 
                        check_alive=lambda: monitor.is_active(timeout=MARKET_CHECK_TIMEOUT),
                        context="Incomplete state resolution",
                        cash_ratio=CASH_RATIO,
                        order_timeout=ORDER_TIMEOUT,
                        execution_timeout=EXECUTION_TIMEOUT
                    )
                    
            elif signal == "sell":
                kospi_reason = details["KOSPI"]["reason"]
                kosdaq_reason = details["KOSDAQ"]["reason"]
                logger.info(f"Signal is SELL (KOSPI: {kospi_reason}, KOSDAQ: {kosdaq_reason}). Executing sell_all to clear position...")
                execute_sell_all_safe(
                    kis, 
                    check_alive=lambda: monitor.is_active(timeout=MARKET_CHECK_TIMEOUT),
                    context="Incomplete state resolution",
                    order_timeout=ORDER_TIMEOUT,
                    execution_timeout=EXECUTION_TIMEOUT
                )
            
            else:
                logger.info(f"Signal is {signal}. No action taken for resolution.")
            
        # 6. 마켓 시그널 확인 및 매매 (1분 간격 반복)
        action_taken = "HOLD"
        
        while True:
            now = datetime.now()
            
            # 5-1. 연결 상태 확인 및 유지
            try:
                kis.websocket.ensure_connected()
            except Exception as e:
                logger.warning(f"Error checking connection: {e}. Retrying in 1 minute...", exc_info=True)
                time.sleep(60)
                continue
            
            # 5-2. 장 마감 확인 (데이터 수신 타임아웃 MARKET_WAIT_TIMEOUT초)
            if not monitor.is_active(timeout=MARKET_WAIT_TIMEOUT):
                logger.info(f"Market appears to be closed (no WS updates for {MARKET_WAIT_TIMEOUT}s). Stopping checks.")
                break

            try:
                # 상태 및 시그널 확인
                # state는 루프 진입 전의 값을 그대로 사용 (중복 조회 제거)
                
                # 캐시된 과거 데이터와 웹소켓 실시간 현재가 사용
                signal_data = get_market_signal(
                    kis, 
                    kospi_current=monitor.prices["KOSPI"],
                    kosdaq_current=monitor.prices["KOSDAQ"],
                    historical_data=historical_indices,
                    verbose=True
                )
                signal = signal_data["signal"]
                details = signal_data["details"]
                logger.info(f"State: {state}, Signal: {signal}")
                
                trade_executed = False

                if state == "CASH" and signal == "buy":
                    kospi_reason = details["KOSPI"]["reason"]
                    kosdaq_reason = details["KOSDAQ"]["reason"]
                    msg = f"Action: BUY (Cash -> Stock)\nIndices: KOSPI[{kospi_reason}], KOSDAQ[{kosdaq_reason}]\nStarting rebalance..."
                    logger.info(msg)
                    send_notification("trade_execution", msg, title="Trade Action Triggered", tags=("rocket",))
                    
                    df_selection = load_stock_selection(kis=kis)
                    if df_selection.empty:
                        logger.info("No stocks selected. Skipping buy.")
                    else:
                        stocks_selected = create_stock_objects(df_selection, kis)
                        if execute_rebalance_safe(
                            kis, 
                            stocks_selected, 
                            check_alive=lambda: monitor.is_active(timeout=MARKET_CHECK_TIMEOUT),
                            context="Main loop",
                            cash_ratio=CASH_RATIO,
                            order_timeout=ORDER_TIMEOUT,
                            execution_timeout=EXECUTION_TIMEOUT
                        ):
                            action_taken = "BUY"
                            trade_executed = True
                        else:
                            logger.warning("Rebalance failed (likely due to error). Will retry next loop.")

                elif state == "STOCK" and signal == "sell":
                    kospi_reason = details["KOSPI"]["reason"]
                    kosdaq_reason = details["KOSDAQ"]["reason"]
                    msg = f"Action: SELL (Stock -> Cash)\nIndices: KOSPI[{kospi_reason}], KOSDAQ[{kosdaq_reason}]\nSelling all holdings..."
                    logger.info(msg)
                    send_notification("trade_execution", msg, title="Trade Action Triggered", tags=("chart_with_downwards_trend",))
                    
                    if execute_sell_all_safe(
                        kis, 
                        check_alive=lambda: monitor.is_active(timeout=MARKET_CHECK_TIMEOUT),
                        context="Main loop",
                        order_timeout=ORDER_TIMEOUT,
                        execution_timeout=EXECUTION_TIMEOUT
                    ):
                        action_taken = "SELL"
                        trade_executed = True
                
                else:
                    logger.info(f"Action: HOLD (State: {state}, Signal: {signal})")
                
                # 매매가 실행되었으면 추가 거래 없이 장 마감 대기
                if trade_executed:
                    logger.info("Trade executed. Stopping checks for today.")
                    break
                    
            except Exception as e:
                logger.error(f"Error during main loop execution: {e}", exc_info=True)
                send_notification("trade_execution", f"Error in main loop: {e}", title="Loop Error", tags=("warning",))
                time.sleep(60)
                continue

            # 1분 대기
            logger.info("Waiting 1 minute...")
            time.sleep(60)
            
        # 6. 장 마감 대기 (웹소켓 구독 해제는 자동 혹은 명시적 처리)
        # 루프를 빠져나왔다는 것은 이미 장이 마감되었거나 타임아웃된 상태임
        logger.info(f"Loop finished. Proceeding to final report.")
            
        # 5. 장 마감 대기
        logger.info(f"Waiting for market close...")
        wait_until_market_close(kis, verbose=True)
        logger.info(f"Market is closed.")
        
    except KeyboardInterrupt:
        logger.info("\nProgram interrupted by user.")
    except Exception as e:
        logger.error(f"\nUnexpected error: {e}", exc_info=True)
        send_notification("trade_execution", f"Unexpected error: {e}", title="Program Error", tags=("warning",))
    finally:
        # 7. 최종 리포트 및 DB 저장 (에러 발생 시에도 실행 보장)
        if initial_asset > 0: # 초기 자산이 설정된 경우에만 실행
            finalize_trading_day(kis, initial_asset, transfer_amount)

        logger.info("Closing websocket connection...")
        try:
            if ticket_kospi:
                ticket_kospi.unsubscribe()
                logger.info("Unsubscribed KOSPI ticket.")
            if ticket_kosdaq:
                ticket_kosdaq.unsubscribe()
                logger.info("Unsubscribed KOSDAQ ticket.")
                
            kis.websocket.unsubscribe_all()
            logger.info("Unsubscribed from all channels.")
            kis.websocket.disconnect()
            logger.info("Websocket connection closed.")
        except Exception as e:
            logger.error(f"Error closing websocket: {e}")

if __name__ == "__main__":
    from datetime import date
    main()
