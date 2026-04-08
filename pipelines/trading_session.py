from __future__ import annotations

import logging
import os
import threading
import time
from datetime import date, datetime
from pathlib import Path
from typing import Callable, Iterable, Literal

from pykis import KisAuth, PyKis

from strategies import get_strategy_definition
from strategies.base import StrategyRuntimeContext
from tools.account_record import (
    get_daily_asset,
    get_latest_stock_performance,
    get_previous_final_asset,
    save_daily_orders,
    save_final_asset,
    save_initial_asset,
    save_stock_performance,
)
from tools.logger import setup_logging
from tools.market_watcher import (
    MarketMonitor,
    is_today_open_day,
    wait_until_market_close,
)
from tools.notifications import send_notification
from tools.selection_store import load_stock_selection
from tools.time_utils import today_kst
from tools.trading_profiles import (
    AccountProfile,
    get_enabled_accounts,
    get_strategy_profile,
    resolve_secret_path,
)
from tools.trading_utils import (
    get_balance_safe,
    retry_execution,
)


CASH_RATIO = 0.03
ORDER_TIMEOUT = 600
EXECUTION_TIMEOUT = 600
MARKET_CHECK_TIMEOUT = 180
MARKET_WAIT_TIMEOUT = 180
MAX_DAILY_TRADES = 3
TRADE_INTERVAL_SECONDS = 3600

BASE_DIR = str(Path(__file__).resolve().parent.parent)
LOG_DIR = os.path.join(BASE_DIR, "logs")

logger = logging.getLogger("trading_session")

AccountRunStatus = Literal["completed", "error", "holiday", "interrupted"]


def _account_prefix(account: AccountProfile) -> str:
    return f"[{account.account_id}]"


def _notify(
    account: AccountProfile,
    message: str,
    *,
    title: str,
    priority: str | None = None,
    tags: Iterable[str] | None = None,
) -> None:
    send_notification(
        "trade_execution",
        f"{_account_prefix(account)} {message}",
        title=f"{title} [{account.account_id}]",
        priority=priority,
        tags=tags,
    )


def _make_strategy_notifier(account: AccountProfile) -> Callable[[str, str, Iterable[str] | None], None]:
    def notify(message: str, title: str, tags: Iterable[str] | None = None) -> None:
        _notify(account, message, title=title, tags=tags)

    return notify


def _warn_if_today_selection_missing(
    account: AccountProfile,
    *,
    requires_selection: bool,
    selection_top_n: int,
    kis: PyKis,
    account_logger: logging.Logger,
) -> None:
    if not requires_selection:
        return

    selection_date = today_kst()

    try:
        df_selection = load_stock_selection(
            table_date=selection_date,
            kis=kis,
            rerank=False,
            top_n=selection_top_n,
            strategy_id=account.strategy_id,
        )
    except KeyError:
        message = (
            "Today's saved stock selection is unavailable.\n"
            f"Strategy: {account.strategy_id}\n"
            f"Date: {selection_date.isoformat()}\n"
            "Trading will continue, but rebalances that require saved selections may be skipped."
        )
        account_logger.warning(message)
        _notify(account, message, title="Today's Selection Missing", tags=("warning",))
        return

    if df_selection.empty:
        message = (
            "Today's saved stock selection is empty.\n"
            f"Strategy: {account.strategy_id}\n"
            f"Date: {selection_date.isoformat()}\n"
            "Trading will continue, but rebalances that require saved selections may be skipped."
        )
        account_logger.warning(message)
        _notify(account, message, title="Today's Selection Empty", tags=("warning",))


def _load_or_capture_initial_asset(
    account: AccountProfile,
    kis: PyKis,
    *,
    account_logger: logging.Logger,
) -> tuple[float, float]:
    db_initial_asset, _, db_transfer_amount = get_daily_asset(account_id=account.account_id)
    if db_initial_asset is not None:
        initial_asset = db_initial_asset
        transfer_amount = db_transfer_amount
        account_logger.info(
            "Loaded initial asset from DB: %s (Transfer: %s)",
            f"{initial_asset:,.0f}",
            f"{transfer_amount:,.0f}",
        )
        return initial_asset, transfer_amount

    balance = get_balance_safe(kis.account(), verbose=True)
    initial_asset = float(balance.total)

    deposit_d2 = 0.0
    if "KRW" in balance.deposits:
        deposit_d2 = float(balance.deposits["KRW"].d2_amount)

    transfer_amount = 0.0
    prev_asset_check, prev_d2_check = get_previous_final_asset(account_id=account.account_id)
    if prev_asset_check is not None and prev_d2_check is not None:
        diff = deposit_d2 - prev_d2_check
        if abs(diff) > 1:
            transfer_amount = diff
            account_logger.info(
                "Detected overnight transfer (D+2 diff): %s KRW",
                f"{transfer_amount:,.0f}",
            )

    save_initial_asset(
        initial_asset,
        deposit_d2,
        transfer_amount,
        account_id=account.account_id,
    )
    account_logger.info(
        "Saved initial asset to DB: %s, D+2: %s, Transfer: %s",
        f"{initial_asset:,.0f}",
        f"{deposit_d2:,.0f}",
        f"{transfer_amount:,.0f}",
    )
    return initial_asset, transfer_amount


def finalize_trading_day(
    kis: PyKis,
    initial_asset: float,
    transfer_amount: float,
    *,
    account: AccountProfile,
    account_logger: logging.Logger,
) -> None:
    try:
        account_logger.info("Finalizing trading day...")

        try:
            balance = get_balance_safe(kis.account(), verbose=True)
            final_asset = float(balance.total)

            deposit_d2 = 0.0
            if "KRW" in balance.deposits:
                deposit_d2 = float(balance.deposits["KRW"].d2_amount)

            save_final_asset(
                final_asset,
                deposit_d2,
                account_id=account.account_id,
            )
            account_logger.info(
                "Saved final asset to DB: %s, D+2 Deposit: %s",
                f"{final_asset:,.0f}",
                f"{deposit_d2:,.0f}",
            )
        except Exception as exc:
            account_logger.error("Error fetching/saving final asset: %s", exc)
            final_asset = initial_asset
            deposit_d2 = 0.0

        try:
            diff_open = final_asset - initial_asset
            diff_open_rate = (diff_open / initial_asset * 100) if initial_asset > 0 else 0.0

            prev_asset, _ = get_previous_final_asset(account_id=account.account_id)
            if prev_asset:
                base_asset = prev_asset + transfer_amount
                diff_prev = final_asset - base_asset
                diff_prev_rate = (diff_prev / base_asset * 100) if base_asset > 0 else 0.0
                prev_msg = f"Vs Prev: {diff_prev:+,.0f} ({diff_prev_rate:+.2f}%)"
                if transfer_amount != 0:
                    prev_msg += f" (Adj for transfer: {transfer_amount:+,.0f})"
            else:
                prev_msg = "Vs Prev: N/A"
        except Exception as exc:
            account_logger.error("Error calculating profits: %s", exc)
            diff_open = 0.0
            diff_open_rate = 0.0
            prev_msg = "Vs Prev: Error"

        today_orders = []
        try:
            def _fetch_orders():
                return kis.account().daily_orders(start=date.today())

            success, today_orders = retry_execution(
                _fetch_orders,
                max_retries=10,
                context=f"Fetching daily orders ({account.account_id})",
            )
            if not success:
                account_logger.error("Failed to fetch daily orders. Skipping order report.")
                today_orders = []

            orders_to_save = []
            for order in today_orders:
                status = "체결" if order.executed_qty > 0 else "미체결"
                if order.canceled:
                    status = "취소"
                elif order.rejected:
                    status = f"거부({order.rejected_reason})"

                orders_to_save.append(
                    {
                        "order_number": str(order.order_number),
                        "time": order.time_kst.strftime("%H:%M:%S"),
                        "type": order.type,
                        "name": order.name,
                        "qty": int(order.qty),
                        "executed_qty": int(order.executed_qty),
                        "price": float(order.price or 0),
                        "status": status,
                    }
                )

            if orders_to_save:
                save_daily_orders(orders_to_save, account_id=account.account_id)
                account_logger.info("Saved %d orders to DB.", len(orders_to_save))
        except Exception as exc:
            account_logger.error("Error processing daily orders: %s", exc)

        try:
            prev_perf_map = get_latest_stock_performance(account_id=account.account_id)
            stock_perf_map = {}

            for symbol, data in prev_perf_map.items():
                stock_perf_map[symbol] = {
                    "symbol": symbol,
                    "name": data["name"],
                    "invested_amount": data["invested_amount"],
                    "sell_amount": data["sell_amount"],
                    "current_value": 0.0,
                    "realized_profit": 0.0,
                    "quantity": data.get("quantity", 0),
                }

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
                        "quantity": 0,
                    }

                if order.type == "buy":
                    stock_perf_map[symbol]["invested_amount"] += amt
                elif order.type == "sell":
                    stock_perf_map[symbol]["sell_amount"] += amt

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
                            "quantity": 0,
                        }

                    stock_perf_map[symbol]["current_value"] = float(stock.amount)
                    stock_perf_map[symbol]["quantity"] = int(stock.quantity)
            except Exception as exc:
                account_logger.error("Error updating stock values from balance: %s", exc)

            try:
                profits = kis.account().profits(start=date.today(), end=date.today())
                for order_profit in profits.orders:
                    symbol = order_profit.symbol
                    if symbol in stock_perf_map:
                        stock_perf_map[symbol]["realized_profit"] += float(order_profit.profit)
            except Exception:
                pass

            if stock_perf_map:
                save_stock_performance(
                    list(stock_perf_map.values()),
                    account_id=account.account_id,
                )
                account_logger.info(
                    "Saved cumulative performance for %d stocks.",
                    len(stock_perf_map),
                )
        except Exception as exc:
            account_logger.error("Error saving stock performance: %s", exc)

        try:
            market_close_time = datetime.now().strftime("%H:%M:%S")
            msg = (
                f"Market Closed: {market_close_time}\n"
                f"Final: {final_asset:,.0f}\n"
                f"Vs Open: {diff_open:+,.0f} ({diff_open_rate:+.2f}%)\n"
                f"{prev_msg}"
            )
            account_logger.info(msg)
            _notify(
                account,
                msg,
                title=f"Daily Report ({date.today()})",
                tags=("chart_with_upwards_trend" if diff_open >= 0 else "chart_with_downwards_trend",),
            )
        except Exception as exc:
            account_logger.error("Error sending final report: %s", exc)
    except Exception as exc:
        account_logger.error("Critical error in finalize_trading_day: %s", exc, exc_info=True)


def run_account(account: AccountProfile) -> AccountRunStatus:
    strategy = get_strategy_profile(account.strategy_id)
    strategy_def = get_strategy_definition(account.strategy_id)
    account_logger = logging.getLogger(f"trading_session.{account.account_id}")
    secret_path = resolve_secret_path(Path(BASE_DIR), account)
    if not secret_path.exists():
        raise FileNotFoundError(f"Secret file not found: {secret_path}")

    kis = PyKis(KisAuth.load(secret_path), keep_token=True)
    ticket_kospi = None
    ticket_kosdaq = None
    initial_asset = 0.0
    transfer_amount = 0.0
    market_open_detected = False
    market_close_completed = False
    result: AccountRunStatus = "completed"

    try:
        if not is_today_open_day(kis):
            msg = f"Today ({date.today()}) is a holiday. No trading."
            account_logger.info(msg)
            _notify(account, msg, title="Market Closed", tags=("sleeping",))
            result = "holiday"
            return result

        initial_asset, transfer_amount = _load_or_capture_initial_asset(
            account,
            kis,
            account_logger=account_logger,
        )
        account_logger.info("Asset at Start: %s", f"{initial_asset:,.0f}")

        try:
            account_logger.info("Ensuring single active websocket session...")
            kis.websocket.ensure_connected()

            if kis.websocket.connected:
                account_logger.info("Websocket session stable.")
            else:
                account_logger.info("Websocket disconnected. Retrying...")
                kis.websocket.ensure_connected()

            account_logger.info("Websocket session established successfully.")
        except Exception as exc:
            account_logger.warning("Session init warning: %s", exc)

        monitor = MarketMonitor()

        def make_handler(name: str, expected_code: str):
            def handler(sender, event):
                if event.response.index_code != expected_code:
                    return
                monitor.update(name, event.response.price)

            return handler

        account_logger.info("Subscribing to KOSPI/KOSDAQ...")
        ticket_kospi = kis.websocket.on_domestic_index_price("KOSPI", make_handler("KOSPI", "0001"))
        ticket_kosdaq = kis.websocket.on_domestic_index_price("KOSDAQ", make_handler("KOSDAQ", "1001"))

        account_logger.info("Waiting for market open (monitoring ticks)...")
        market_open_time = None
        while True:
            try:
                kis.websocket.ensure_connected()
            except Exception as exc:
                account_logger.warning("WebSocket connection issue during wait: %s. Retrying...", exc)

            if monitor.is_active(timeout=None):
                market_open_time = datetime.now()
                market_open_detected = True
                account_logger.info("Market open detected (ticks received).")
                break

            time.sleep(10)

        account_logger.info("Market is open.")

        msg = f"Market Open: {market_open_time.strftime('%H:%M:%S')}\nInitial Asset: {initial_asset:,.0f} KRW"
        if transfer_amount != 0:
            msg += f"\n(Net Transfer: {transfer_amount:+,.0f})"
        _notify(account, msg, title="Market Open & Initial Asset", tags=("moneybag",))
        runtime_context = StrategyRuntimeContext(
            kis=kis,
            account=account,
            strategy_profile=strategy,
            account_logger=account_logger,
            monitor=monitor,
            initial_asset=initial_asset,
            order_timeout=ORDER_TIMEOUT,
            execution_timeout=EXECUTION_TIMEOUT,
            market_check_timeout=MARKET_CHECK_TIMEOUT,
            market_wait_timeout=MARKET_WAIT_TIMEOUT,
            max_daily_trades=MAX_DAILY_TRADES,
            trade_interval_seconds=TRADE_INTERVAL_SECONDS,
            notify=_make_strategy_notifier(account),
        )
        _warn_if_today_selection_missing(
            account,
            requires_selection=strategy_def.requires_selection,
            selection_top_n=strategy.selection_top_n,
            kis=kis,
            account_logger=account_logger,
        )
        strategy_def.run_trading_day(runtime_context)

        account_logger.info("Waiting for market close...")
        wait_until_market_close(kis, verbose=True)
        market_close_completed = True
        account_logger.info("Market is closed.")
    except KeyboardInterrupt:
        account_logger.info("Program interrupted by user.")
        result = "interrupted"
    except Exception as exc:
        account_logger.error("Unexpected error: %s", exc, exc_info=True)
        _notify(account, f"Unexpected error: {exc}", title="Program Error", tags=("warning",))
        result = "error"
    finally:
        if market_open_detected and market_close_completed and initial_asset > 0:
            finalize_trading_day(
                kis,
                initial_asset,
                transfer_amount,
                account=account,
                account_logger=account_logger,
            )

        account_logger.info("Closing websocket connection...")
        try:
            if ticket_kospi:
                ticket_kospi.unsubscribe()
                account_logger.info("Unsubscribed KOSPI ticket.")
            if ticket_kosdaq:
                ticket_kosdaq.unsubscribe()
                account_logger.info("Unsubscribed KOSDAQ ticket.")

            kis.websocket.unsubscribe_all()
            account_logger.info("Unsubscribed from all channels.")
            kis.websocket.disconnect()
            account_logger.info("Websocket connection closed.")
        except Exception as exc:
            account_logger.error("Error closing websocket: %s", exc)

    return result


def _run_account_thread(
    account: AccountProfile,
    results: dict[str, AccountRunStatus],
    results_lock: threading.Lock,
) -> None:
    thread_logger = logging.getLogger(f"trading_session.{account.account_id}")
    thread_logger.info("Starting account worker for strategy %s", account.strategy_id)
    result = run_account(account)
    with results_lock:
        results[account.account_id] = result


def run_trading_session() -> dict[str, AccountRunStatus]:
    setup_logging(LOG_DIR)
    accounts = get_enabled_accounts()
    if not accounts:
        raise ValueError("No enabled trading accounts configured.")

    threads: list[threading.Thread] = []
    results: dict[str, AccountRunStatus] = {}
    results_lock = threading.Lock()
    for account in accounts:
        thread = threading.Thread(
            target=_run_account_thread,
            args=(account, results, results_lock),
            name=f"trading-session-{account.account_id}",
            daemon=False,
        )
        thread.start()
        threads.append(thread)

    for thread in threads:
        thread.join()

    return results


def main() -> None:
    run_trading_session()


if __name__ == "__main__":
    main()
