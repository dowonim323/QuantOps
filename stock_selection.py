import logging
import time
from pathlib import Path

from pykis import PyKis, KisAuth

from tools.market_master import (
    download_code_master,
    get_kospi_kosdaq_master_dataframe,
)
from tools.quant_utils import (
    create_stock_objects,
    select_stocks,
)
from tools.selection_store import save_stock_selection
from tools.financial_db import backup_quant_databases
from tools.notifications import send_notification


NOTIFICATION_CHANNEL = "stock_selection"
BASE_DIR = Path(__file__).resolve().parent


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
        code_dir = BASE_DIR / "codes"
        secret_path = BASE_DIR / "secrets" / "real.json"

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

        # 2. 종목 객체 생성 및 선정
        stocks = create_stock_objects(df_codes, kis)
        df_selected, df_snapshot = select_stocks(
            df_codes,
            stocks,
            include_full_data=True,
        )

        # 3. 결과 저장
        backup_quant_databases()
        save_stock_selection(df_snapshot)
        
        selected_count = len(df_selected)
        elapsed_seconds = time.perf_counter() - start_time

        # 완료 알림
        summary_lines = [
            "Stock selection process completed.",
            f"Total candidates: {total_stocks}",
            f"Selected stocks: {selected_count}",
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