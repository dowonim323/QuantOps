from __future__ import annotations

from datetime import date, timedelta
from typing import Any, Mapping, cast

import pandas as pd
from pykis import PyKis

from strategies.base import StrategyDefinition, StrategyRuntimeContext
from strategies.schedule import is_rebalance_due_by_elapsed_week
from tools.account_record import load_strategy_runtime_state, save_strategy_runtime_state
from tools.quant_utils import create_stock_objects
from tools.time_utils import today_kst
from tools.trading_utils import execute_rebalance_safe, get_balance_safe

CORE4_WEIGHTS: dict[str, float] = {
    "418660": 0.30,
    "390390": 0.20,
    "476760": 0.30,
    "411060": 0.20,
}
CORE4_NAMES: dict[str, str] = {
    "418660": "TIGER 미국나스닥100레버리지(합성)",
    "390390": "KODEX 미국반도체MV",
    "476760": "ACE 미국30년국채액티브(H)",
    "411060": "ACE KRX금현물",
}
CORE4_CODES = ["418660", "390390", "476760", "411060"]
SIGNAL_CODE = "133690"
ZSCORE_WINDOW = 60
ZSCORE_LOOKBACK_DAYS = 180
INCOMPLETE_STATE_STOCK_RATIO_THRESHOLD = 0.9
INCOMPLETE_STATE_CASH_THRESHOLD = 100000.0

STAGE_ATTACK_DEFENSE_RATIOS: dict[int, tuple[float, float]] = {
    0: (0.5, 0.5),
    1: (0.6, 0.4),
    2: (0.7, 0.3),
    3: (0.8, 0.2),
    4: (0.9, 0.1),
    5: (1.0, 0.0),
}


def build_selection_snapshot(
    df_codes: pd.DataFrame,
    stocks: Mapping[str, Any],
    kis: PyKis,
    top_n: int,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    del stocks, kis, top_n

    df_snapshot = df_codes.loc[
        df_codes["단축코드"].isin(CORE4_CODES),
        ["단축코드", "한글명"],
    ].copy()
    if df_snapshot["단축코드"].nunique() != len(CORE4_WEIGHTS):
        found_codes = set(df_snapshot["단축코드"].tolist())
        missing = ", ".join(sorted(set(CORE4_WEIGHTS) - found_codes))
        raise KeyError(f"krx_us_core4 종목 정보를 찾을 수 없습니다: {missing}")

    df_snapshot["target_weight"] = df_snapshot["단축코드"].map(CORE4_WEIGHTS)
    df_snapshot = df_snapshot.sort_values("단축코드").reset_index(drop=True)
    return df_snapshot.copy(), df_snapshot


def build_runtime_selection() -> pd.DataFrame:
    df_selection = pd.DataFrame(
        {
            "단축코드": CORE4_CODES,
            "한글명": [CORE4_NAMES[code] for code in CORE4_CODES],
            "target_weight": [CORE4_WEIGHTS[code] for code in CORE4_CODES],
        }
    )
    return df_selection


def should_rebalance_today(kis: PyKis, base_date: date | None = None) -> bool:
    del kis, base_date
    return True


def compute_zscore(closes: pd.Series, window: int = ZSCORE_WINDOW) -> pd.Series:
    if len(closes) < window:
        raise ValueError(f"At least {window} closes are required to compute Z-score.")

    rolling_mean = closes.rolling(window=window, min_periods=window).mean()
    rolling_std = closes.rolling(window=window, min_periods=window).std(ddof=0)
    safe_std = rolling_std.replace(0, pd.NA)
    zscore = cast(pd.Series, (closes - rolling_mean) / safe_std)
    return zscore.astype(float)


def map_zscore_to_stage(zscore: float) -> int:
    if zscore >= 0:
        return 0
    if zscore >= -1.0:
        return 1
    if zscore >= -2.0:
        return 2
    if zscore >= -3.0:
        return 3
    if zscore >= -4.0:
        return 4
    return 5


def resolve_applied_stage(previous_stage: int, bucket_stage: int) -> int:
    del previous_stage
    return bucket_stage


def build_stage_target_weights(stage: int) -> dict[str, float]:
    if stage not in STAGE_ATTACK_DEFENSE_RATIOS:
        raise ValueError(f"Unknown stage: {stage}")

    attack_ratio, defense_ratio = STAGE_ATTACK_DEFENSE_RATIOS[stage]
    weights = {
        "418660": attack_ratio * 0.6,
        "390390": attack_ratio * 0.4,
        "476760": defense_ratio * 0.6,
        "411060": defense_ratio * 0.4,
    }
    total_weight = sum(weights.values())
    if abs(total_weight - 1.0) > 1e-9:
        raise ValueError(f"Stage weights must sum to 1.0, got {total_weight}")

    return weights


def get_signal_closes(
    kis: PyKis,
    *,
    base_date: date | None = None,
    lookback_days: int = ZSCORE_LOOKBACK_DAYS,
) -> pd.Series:
    current_date = base_date or today_kst()
    signal_stock = kis.stock(SIGNAL_CODE)
    chart = signal_stock.daily_chart(
        start=current_date - timedelta(days=lookback_days),
        end=current_date,
    )

    rows = [
        {
            "date": bar.time.date(),
            "close": float(bar.close),
        }
        for bar in chart.bars
        if bar.time.date() < current_date
    ]
    if len(rows) < ZSCORE_WINDOW:
        raise ValueError("Not enough completed bars to compute Z-score for krx_us_core4")

    df = pd.DataFrame(rows).drop_duplicates(subset=["date"]).sort_values("date")
    return pd.Series(df["close"].tolist(), index=df["date"].tolist(), dtype=float)


def get_target_weights(df_selection: pd.DataFrame) -> dict[str, float] | None:
    if "target_weight" not in df_selection.columns:
        raise KeyError("target_weight column is required for krx_us_core4")

    weights = {
        str(row["단축코드"]): float(row["target_weight"])
        for _, row in df_selection.iterrows()
    }
    total_weight = sum(weights.values())
    if abs(total_weight - 1.0) > 1e-9:
        raise ValueError(f"krx_us_core4 weights must sum to 1.0, got {total_weight}")

    return weights


def needs_cash_correction(initial_asset: float, stock_value: float) -> bool:
    if initial_asset <= 0:
        return False

    stock_ratio = stock_value / initial_asset
    remaining_cash = initial_asset - stock_value
    return stock_ratio <= INCOMPLETE_STATE_STOCK_RATIO_THRESHOLD and remaining_cash > INCOMPLETE_STATE_CASH_THRESHOLD


def run_trading_day(context: StrategyRuntimeContext) -> None:
    current_date = today_kst()
    scheduled_rebalance = is_rebalance_due_by_elapsed_week(
        load_strategy_runtime_state(
            STRATEGY.strategy_id,
            account_id=context.account.account_id,
        ).get("last_rebalance_date"),
        base_date=current_date,
    )
    context.account_logger.info("Scheduled rebalance day: %s", scheduled_rebalance)

    runtime_state = load_strategy_runtime_state(
        STRATEGY.strategy_id,
        account_id=context.account.account_id,
    )

    closes = get_signal_closes(context.kis, base_date=current_date)
    latest_signal_date = str(closes.index[-1])
    latest_zscore = float(compute_zscore(closes).iloc[-1])
    bucket_stage = map_zscore_to_stage(latest_zscore)
    previous_stage = int(runtime_state["stage"])
    applied_stage = resolve_applied_stage(previous_stage, bucket_stage)
    stage_changed = applied_stage != previous_stage

    balance = get_balance_safe(context.kis.account(), verbose=True)
    stock_value = sum(float(stock.amount) for stock in balance.stocks)
    stock_ratio = stock_value / context.initial_asset if context.initial_asset > 0 else 0.0
    rebalance_for_cash = needs_cash_correction(context.initial_asset, stock_value)
    should_rebalance_now = scheduled_rebalance or stage_changed or rebalance_for_cash

    context.account_logger.info(
        "Current Stock Ratio: %.2f%% (Value: %s)",
        stock_ratio * 100,
        f"{stock_value:,.0f}",
    )

    context.account_logger.info(
        "krx_us_core4 Z-score %.2f -> bucket=%d previous_stage=%d applied_stage=%d signal_date=%s stage_changed=%s cash_correction=%s",
        latest_zscore,
        bucket_stage,
        previous_stage,
        applied_stage,
        latest_signal_date,
        stage_changed,
        rebalance_for_cash,
    )

    if rebalance_for_cash:
        message = f"Incomplete state detected (Stock Ratio: {stock_ratio*100:.2f}%). Rebalancing Core4 holdings..."
        context.account_logger.info(message)
        context.notify(message, "Incomplete State Resolution", ("wrench",))

    if runtime_state.get("last_signal_date") == latest_signal_date and not rebalance_for_cash:
        context.account_logger.info("Signal date %s already processed. Skipping duplicate rebalance.", latest_signal_date)
        return

    if not should_rebalance_now:
        context.account_logger.info(
            "Skipping rebalance because today is not a scheduled day and applied stage did not change."
        )
        return

    df_selection = build_runtime_selection()

    stocks_selected = create_stock_objects(df_selection, context.kis)
    target_weights = build_stage_target_weights(applied_stage)
    success = execute_rebalance_safe(
        context.kis,
        stocks_selected,
        check_alive=lambda: context.monitor.is_active(timeout=context.market_check_timeout),
        context=f"Core4 rebalance ({context.account.account_id})",
        cash_ratio=context.strategy_profile.cash_ratio,
        target_weights=target_weights,
        order_timeout=context.order_timeout,
        execution_timeout=context.execution_timeout,
    )
    if success:
        save_strategy_runtime_state(
            STRATEGY.strategy_id,
            applied_stage,
            account_id=context.account.account_id,
            last_signal_date=latest_signal_date,
            last_rsi=latest_zscore,
            last_rebalance_date=current_date.isoformat(),
        )


STRATEGY = StrategyDefinition(
    strategy_id="krx_us_core4",
    rebalance_mode="scheduled_once",
    requires_selection=False,
    rerank_on_load=False,
    build_selection_snapshot=build_selection_snapshot,
    should_rebalance_today=should_rebalance_today,
    get_target_weights=get_target_weights,
    run_trading_day=run_trading_day,
)
