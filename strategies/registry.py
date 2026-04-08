from __future__ import annotations

from strategies.base import StrategyDefinition
from strategies.krx_us_core4 import STRATEGY as KRX_US_CORE4_STRATEGY
from strategies.krx_vmq import STRATEGY as KRX_VMQ_STRATEGY

_STRATEGIES: dict[str, StrategyDefinition] = {
    KRX_VMQ_STRATEGY.strategy_id: KRX_VMQ_STRATEGY,
    KRX_US_CORE4_STRATEGY.strategy_id: KRX_US_CORE4_STRATEGY,
}


def get_strategy_definition(strategy_id: str) -> StrategyDefinition:
    if strategy_id not in _STRATEGIES:
        raise KeyError(f"Unknown strategy_id: {strategy_id}")

    return _STRATEGIES[strategy_id]
