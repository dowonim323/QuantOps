from __future__ import annotations

import time
from datetime import datetime, date
from typing import Any, Mapping, cast

import pandas as pd
from pykis import PyKis

from strategies.base import StrategyDefinition, StrategyRuntimeContext
from strategies.schedule import is_rebalance_due_by_elapsed_week
from tools.account_record import load_strategy_runtime_state, save_strategy_runtime_state
from tools.market_watcher import fetch_historical_indices, get_market_signal, get_previous_close_signal
from tools.quant_utils import create_stock_objects, select_stocks
from tools.selection_store import load_stock_selection
from tools.time_utils import today_kst
from tools.trading_utils import (
    execute_rebalance_safe,
    execute_sell_all_safe,
    get_account_state,
    get_balance_safe,
)


def build_selection_snapshot(
    df_codes: pd.DataFrame,
    stocks: Mapping[str, Any],
    kis: PyKis,
    top_n: int,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    del kis
    return cast(
        tuple[pd.DataFrame, pd.DataFrame],
        select_stocks(
        df_codes,
        stocks,
        top_n=top_n,
        include_full_data=True,
        ),
    )


def should_rebalance_today(kis: PyKis, base_date: date | None = None) -> bool:
    del kis, base_date
    return True


def get_target_weights(df_selection: pd.DataFrame) -> dict[str, float] | None:
    del df_selection
    return None


def _load_saved_selection_or_warn(
    context: StrategyRuntimeContext,
    *,
    trigger: str,
    selection_date: date,
    warned_events: set[tuple[str, str]],
) -> pd.DataFrame:
    account = context.account
    strategy = context.strategy_profile
    account_logger = context.account_logger

    def warn_once(warning_kind: str, title: str, message: str) -> None:
        warning_key = (trigger, warning_kind)
        if warning_key in warned_events:
            return
        warned_events.add(warning_key)
        account_logger.warning(message)
        context.notify(message, title, ("warning",))

    try:
        df_selection = load_stock_selection(
            table_date=selection_date,
            kis=context.kis,
            rerank=STRATEGY.rerank_on_load,
            top_n=strategy.selection_top_n,
            strategy_id=account.strategy_id,
        )
    except KeyError:
        message = (
            "Saved stock selection is unavailable for rebalance.\n"
            f"Strategy: {strategy.strategy_id}\n"
            f"Date: {selection_date.isoformat()}\n"
            f"Trigger: {trigger}\n"
            "Skipping rebalance."
        )
        warn_once("missing", "Saved Selection Missing", message)
        return pd.DataFrame()

    if df_selection.empty:
        message = (
            "No usable saved stock selection is available for rebalance.\n"
            f"Strategy: {strategy.strategy_id}\n"
            f"Date: {selection_date.isoformat()}\n"
            f"Trigger: {trigger}\n"
            "Skipping rebalance."
        )
        warn_once("empty", "Saved Selection Unavailable", message)

    return df_selection


def run_trading_day(context: StrategyRuntimeContext) -> None:
    account = context.account
    strategy = context.strategy_profile
    account_logger = context.account_logger
    kis = context.kis
    monitor = context.monitor

    historical_indices = fetch_historical_indices(kis)
    account_logger.info("Historical indices loaded.")
    current_date = today_kst()
    runtime_state = load_strategy_runtime_state(
        STRATEGY.strategy_id,
        account_id=account.account_id,
    )
    scheduled_rebalance = is_rebalance_due_by_elapsed_week(
        runtime_state.get("last_rebalance_date"),
        base_date=current_date,
    )
    account_logger.info("Scheduled rebalance day: %s", scheduled_rebalance)
    warned_selection_events: set[tuple[str, str]] = set()

    prev_close_signal_data = get_previous_close_signal(kis)
    prev_close_signal = prev_close_signal_data.get("signal")
    account_logger.info(
        "Previous close signal: %s (%s)",
        prev_close_signal,
        prev_close_signal_data.get("reason", "N/A"),
    )

    state, _ = get_account_state(kis)
    account_logger.info("Initial State: %s", state)

    if state == "STOCK" and prev_close_signal == "sell":
        opening_signal_data = get_market_signal(
            kis,
            kosdaq_current=monitor.prices["KOSDAQ"],
            historical_data=historical_indices,
            verbose=True,
        )
        opening_signal = opening_signal_data["signal"]

        if opening_signal == "sell":
            kosdaq_reason = opening_signal_data["details"]["KOSDAQ"]["reason"]
            message = (
                "Opening SELL confirmed (Prev Close: SELL, Opening: SELL)\n"
                f"KOSDAQ: {kosdaq_reason}\n"
                "Selling all holdings..."
            )
            account_logger.info(message)
            context.notify(message, "Opening Sell Triggered", ("chart_with_downwards_trend",))

            if execute_sell_all_safe(
                kis,
                check_alive=lambda: monitor.is_active(timeout=context.market_check_timeout),
                context=f"Opening sell ({account.account_id})",
                order_timeout=context.order_timeout,
                execution_timeout=context.execution_timeout,
            ):
                state = "CASH"
                account_logger.info("Opening sell completed. State changed to CASH.")
        else:
            account_logger.info(
                "Prev close was SELL but opening signal is %s. No sell executed.",
                opening_signal,
            )
    elif state == "STOCK":
        account_logger.info("Prev close signal was %s. Skipping opening sell check.", prev_close_signal)

    balance = get_balance_safe(kis.account(), verbose=True)
    stock_value = sum(float(stock.amount) for stock in balance.stocks)
    stock_ratio = stock_value / context.initial_asset if context.initial_asset > 0 else 0.0
    account_logger.info(
        "Current Stock Ratio: %.2f%% (Value: %s)",
        stock_ratio * 100,
        f"{stock_value:,.0f}",
    )

    if (0 < stock_ratio <= 0.9) and (context.initial_asset - stock_value > 100000):
        message = f"Incomplete state detected (Stock Ratio: {stock_ratio*100:.2f}%). Resolving..."
        account_logger.info(message)
        context.notify(message, "Incomplete State Resolution", ("wrench",))

        signal_data = get_market_signal(
            kis,
            kosdaq_current=monitor.prices["KOSDAQ"],
            historical_data=historical_indices,
            verbose=True,
        )
        signal = signal_data["signal"]
        details = signal_data["details"]
        account_logger.info("Resolution Signal: %s", signal)

        if signal == "buy":
            kosdaq_reason = details["KOSDAQ"]["reason"]
            account_logger.info(
                "Signal is BUY (KOSDAQ: %s). Executing rebalance to fill position...",
                kosdaq_reason,
            )
            df_selection = _load_saved_selection_or_warn(
                context,
                trigger="incomplete state resolution",
                selection_date=current_date,
                warned_events=warned_selection_events,
            )
            if df_selection.empty:
                account_logger.info("No stocks selected. Skipping buy.")
            else:
                stocks_selected = create_stock_objects(df_selection, kis)
                if execute_rebalance_safe(
                    kis,
                    stocks_selected,
                    check_alive=lambda: monitor.is_active(timeout=context.market_check_timeout),
                    context=f"Incomplete state resolution ({account.account_id})",
                    cash_ratio=strategy.cash_ratio,
                    order_timeout=context.order_timeout,
                    execution_timeout=context.execution_timeout,
                ):
                    save_strategy_runtime_state(
                        STRATEGY.strategy_id,
                        runtime_state["stage"],
                        account_id=account.account_id,
                        last_signal_date=runtime_state.get("last_signal_date"),
                        last_rsi=runtime_state.get("last_rsi"),
                        last_rebalance_date=current_date.isoformat(),
                    )
                    state = "STOCK"
        else:
            account_logger.info("Signal is %s. No action taken for resolution.", signal)

        state, _ = get_account_state(kis)
        account_logger.info("State after resolution check: %s", state)

    if scheduled_rebalance:
        signal_data = get_market_signal(
            kis,
            kosdaq_current=monitor.prices["KOSDAQ"],
            historical_data=historical_indices,
            verbose=True,
        )
        signal = signal_data["signal"]
        details = signal_data["details"]
        account_logger.info("Scheduled rebalance signal: %s", signal)

        if signal == "buy":
            kosdaq_reason = details["KOSDAQ"]["reason"]
            message = (
                "Scheduled weekly rebalance triggered\n"
                f"Signal: KOSDAQ[{kosdaq_reason}]\n"
                f"Strategy: {strategy.strategy_id}\n"
                "Starting rebalance..."
            )
            df_selection = _load_saved_selection_or_warn(
                context,
                trigger="scheduled weekly rebalance",
                selection_date=current_date,
                warned_events=warned_selection_events,
            )
            if df_selection.empty:
                account_logger.info("No stocks selected. Skipping scheduled rebalance.")
            else:
                account_logger.info(message)
                context.notify(message, "Scheduled Rebalance Triggered", ("calendar",))
                stocks_selected = create_stock_objects(df_selection, kis)
                if execute_rebalance_safe(
                    kis,
                    stocks_selected,
                    check_alive=lambda: monitor.is_active(timeout=context.market_check_timeout),
                    context=f"Scheduled rebalance ({account.account_id})",
                    cash_ratio=strategy.cash_ratio,
                    order_timeout=context.order_timeout,
                    execution_timeout=context.execution_timeout,
                ):
                    save_strategy_runtime_state(
                        STRATEGY.strategy_id,
                        runtime_state["stage"],
                        account_id=account.account_id,
                        last_signal_date=runtime_state.get("last_signal_date"),
                        last_rsi=runtime_state.get("last_rsi"),
                        last_rebalance_date=current_date.isoformat(),
                    )
                    state = "STOCK"
        else:
            account_logger.info("Scheduled rebalance skipped because signal is %s.", signal)

    last_trade_time = None
    daily_trade_count = 0

    while True:
        now = datetime.now()

        try:
            kis.websocket.ensure_connected()
        except Exception as exc:
            account_logger.warning("Error checking connection: %s. Retrying in 1 minute...", exc, exc_info=True)
            time.sleep(60)
            continue

        if not monitor.is_active(timeout=context.market_wait_timeout):
            account_logger.info(
                "Market appears to be closed (no WS updates for %ss). Stopping checks.",
                context.market_wait_timeout,
            )
            break

        try:
            signal_data = get_market_signal(
                kis,
                kosdaq_current=monitor.prices["KOSDAQ"],
                historical_data=historical_indices,
                verbose=True,
            )
            signal = signal_data["signal"]
            details = signal_data["details"]
            account_logger.info("State: %s, Signal: %s", state, signal)

            can_trade = False
            if daily_trade_count >= context.max_daily_trades:
                account_logger.info(
                    "Daily trade limit reached (%d/%d). Skipping trade.",
                    daily_trade_count,
                    context.max_daily_trades,
                )
            elif last_trade_time is not None and (now - last_trade_time).total_seconds() < context.trade_interval_seconds:
                wait_remaining = context.trade_interval_seconds - (now - last_trade_time).total_seconds()
                account_logger.info("Trade interval not met. Waiting %.0fs. Skipping trade.", wait_remaining)
            else:
                can_trade = True

            if can_trade:
                if state == "CASH" and signal == "buy":
                    kosdaq_reason = details["KOSDAQ"]["reason"]
                    message = (
                        "Action: BUY (Cash -> Stock)\n"
                        f"Signal: KOSDAQ[{kosdaq_reason}]\n"
                        f"Strategy: {strategy.strategy_id}\n"
                        "Starting rebalance..."
                    )
                    df_selection = _load_saved_selection_or_warn(
                        context,
                        trigger="cash-to-stock rebalance",
                        selection_date=current_date,
                        warned_events=warned_selection_events,
                    )
                    if df_selection.empty:
                        account_logger.info("No stocks selected. Skipping buy.")
                    else:
                        account_logger.info(message)
                        context.notify(message, "Trade Action Triggered", ("rocket",))
                        stocks_selected = create_stock_objects(df_selection, kis)
                        if execute_rebalance_safe(
                            kis,
                            stocks_selected,
                            check_alive=lambda: monitor.is_active(timeout=context.market_check_timeout),
                            context=f"Main loop ({account.account_id})",
                            cash_ratio=strategy.cash_ratio,
                            order_timeout=context.order_timeout,
                            execution_timeout=context.execution_timeout,
                        ):
                            save_strategy_runtime_state(
                                STRATEGY.strategy_id,
                                runtime_state["stage"],
                                account_id=account.account_id,
                                last_signal_date=runtime_state.get("last_signal_date"),
                                last_rsi=runtime_state.get("last_rsi"),
                                last_rebalance_date=current_date.isoformat(),
                            )
                            daily_trade_count += 1
                            last_trade_time = datetime.now()
                            state = "STOCK"
                            account_logger.info(
                                "Trade #%d completed. Next trade allowed after %ss.",
                                daily_trade_count,
                                context.trade_interval_seconds,
                            )
                        else:
                            account_logger.warning("Rebalance failed (likely due to error). Will retry next loop.")
                else:
                    account_logger.info("Action: HOLD (State: %s, Signal: %s)", state, signal)

            if daily_trade_count >= context.max_daily_trades:
                account_logger.info("Daily trade limit reached. Continuing monitoring only.")
        except Exception as exc:
            account_logger.error("Error during main loop execution: %s", exc, exc_info=True)
            context.notify(f"Error in main loop: {exc}", "Loop Error", ("warning",))
            time.sleep(60)
            continue

        account_logger.info("Waiting 1 minute...")
        time.sleep(60)

    account_logger.info("Loop finished. Proceeding to final report.")


STRATEGY = StrategyDefinition(
    strategy_id="krx_vmq",
    rebalance_mode="signal_loop",
    requires_selection=True,
    rerank_on_load=True,
    build_selection_snapshot=build_selection_snapshot,
    should_rebalance_today=should_rebalance_today,
    get_target_weights=get_target_weights,
    run_trading_day=run_trading_day,
)
