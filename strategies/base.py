from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import date
from typing import Any, Callable, Iterable, Literal, Mapping

import pandas as pd
from pykis import PyKis


BuildSelectionSnapshot = Callable[
    [pd.DataFrame, Mapping[str, Any], PyKis, int],
    tuple[pd.DataFrame, pd.DataFrame],
]
ShouldRebalanceToday = Callable[[PyKis, date | None], bool]
GetTargetWeights = Callable[[pd.DataFrame], dict[str, float] | None]
NotifyStrategyEvent = Callable[[str, str, Iterable[str] | None], None]


@dataclass(frozen=True)
class StrategyRuntimeContext:
    kis: PyKis
    account: Any
    strategy_profile: Any
    account_logger: logging.Logger
    monitor: Any
    initial_asset: float
    order_timeout: float
    execution_timeout: float
    market_check_timeout: int
    market_wait_timeout: int
    max_daily_trades: int
    trade_interval_seconds: int
    notify: NotifyStrategyEvent


RunTradingDay = Callable[[StrategyRuntimeContext], None]


@dataclass(frozen=True)
class StrategyDefinition:
    strategy_id: str
    rebalance_mode: Literal["signal_loop", "scheduled_once"]
    requires_selection: bool
    rerank_on_load: bool
    build_selection_snapshot: BuildSelectionSnapshot
    should_rebalance_today: ShouldRebalanceToday
    get_target_weights: GetTargetWeights
    run_trading_day: RunTradingDay
