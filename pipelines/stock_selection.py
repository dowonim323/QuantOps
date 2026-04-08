import logging
import time
from pathlib import Path

from pykis import PyKis, KisAuth

from strategies import get_strategy_definition
from tools.market_master import (
    download_code_master,
    get_kospi_kosdaq_master_dataframe,
)
from tools.quant_utils import create_stock_objects
from tools.selection_store import save_stock_selection
from tools.financial_db import backup_quant_databases
from tools.notifications import send_notification
from tools.trading_profiles import (
    get_enabled_accounts,
    get_primary_selection_account,
    get_strategy_profile,
    get_unique_strategies,
    resolve_secret_path,
)


NOTIFICATION_CHANNEL = "stock_selection"
BASE_DIR = Path(__file__).resolve().parent.parent


def _format_elapsed(seconds: float) -> str:
    """경과 시간을 포맷팅합니다."""
    if seconds < 1:
        return f"{seconds * 1000:.0f} ms"

    hours = int(seconds // 3600)
    minutes = int((seconds % 3600) // 60)
    secs = seconds - (hours * 3600) - (minutes * 60)

    parts: list[str] = []
    if hours:
        parts.append(f"{hours}h")
    if minutes or hours:
        parts.append(f"{minutes}m")
    parts.append(f"{secs:.1f}s")

    return " ".join(parts)


def main() -> None:
    """종목 선정 프로세스를 실행합니다."""
    logging.getLogger("pykis").setLevel(logging.ERROR)
    start_time = time.perf_counter()

    try:
        accounts = get_enabled_accounts()
        if not accounts:
            raise ValueError("No enabled trading accounts configured.")

        primary_account = get_primary_selection_account(accounts)
        strategy_profiles = get_unique_strategies(accounts)
        code_dir = BASE_DIR / "codes"
        secret_path = resolve_secret_path(BASE_DIR, primary_account)

        kis = PyKis(KisAuth.load(secret_path), keep_token=True)

        # 1. 마스터 데이터 다운로드 및 로드
        download_code_master(str(code_dir), "kospi")
        download_code_master(str(code_dir), "kosdaq")
        df_codes = get_kospi_kosdaq_master_dataframe(str(code_dir))
        
        total_stocks = len(df_codes)

        # 시작 알림 (종목 수 확인 후 전송)
        send_notification(
            NOTIFICATION_CHANNEL,
            f"Stock selection process started.\nTotal candidates: {total_stocks}",
            title="Stock Selection Start",
            tags=("rocket",),
        )

        stocks = create_stock_objects(df_codes, kis)
        backup_quant_databases()

        strategy_results: list[str] = []
        total_selected = 0
        processed_strategies = 0
        for strategy in strategy_profiles:
            strategy_def = get_strategy_definition(strategy.strategy_id)
            if not strategy_def.requires_selection:
                strategy_results.append(
                    f"- {strategy.display_name} ({strategy.strategy_id}): skipped (selection not required)"
                )
                continue

            df_selected, df_snapshot = strategy_def.build_selection_snapshot(
                df_codes,
                stocks,
                kis,
                strategy.selection_top_n,
            )
            save_stock_selection(df_snapshot, strategy_id=strategy.strategy_id)
            selected_count = len(df_selected)
            total_selected += selected_count
            processed_strategies += 1
            strategy_results.append(
                f"- {strategy.display_name} ({strategy.strategy_id}): {selected_count} stocks"
            )

        elapsed_seconds = time.perf_counter() - start_time

        # 완료 알림
        summary_lines = [
            "Stock selection process completed.",
            f"Total candidates: {total_stocks}",
            f"Strategies processed: {processed_strategies}",
            f"Total selected stocks: {total_selected}",
            *strategy_results,
            f"Elapsed time: {_format_elapsed(elapsed_seconds)}",
        ]

        send_notification(
            NOTIFICATION_CHANNEL,
            "\n".join(summary_lines),
            title="Stock Selection Complete",
            tags=("white_check_mark",),
        )

    except Exception as exc:
        elapsed_seconds = time.perf_counter() - start_time
        send_notification(
            NOTIFICATION_CHANNEL,
            f"Stock selection process failed: {exc}\nElapsed time: {_format_elapsed(elapsed_seconds)}",
            title="Stock Selection Failed",
            priority="high",
            tags=("warning",),
        )
        raise


if __name__ == "__main__":
    main()
