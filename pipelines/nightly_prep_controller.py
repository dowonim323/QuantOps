from __future__ import annotations

import logging
import sys
import time
from datetime import date, datetime, time as dt_time

from pipelines.financial_crawler import main as run_financial_crawler
from pipelines.stock_selection import main as run_stock_selection
from strategies import get_strategy_definition
from tools.notifications import send_notification
from tools.scheduler_state import (
    load_nightly_prep_state,
    save_nightly_prep_state,
    scheduler_lock,
)
from tools.selection_store import get_saved_selection_row_count
from tools.time_utils import now_kst, within_kst_window
from tools.trading_profiles import get_enabled_accounts

PREP_WINDOW_START = dt_time(hour=0, minute=0)
PREP_WINDOW_END = dt_time(hour=8, minute=20)
POLL_INTERVAL_SECONDS = 60
NOTIFICATION_CHANNEL = "stock_selection"

logger = logging.getLogger(__name__)


def _configure_logging() -> None:
    if logging.getLogger().handlers:
        return

    logging.basicConfig(
        level=logging.INFO,
        format="[%(asctime)s] %(levelname)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        handlers=[logging.StreamHandler(sys.stdout)],
        force=True,
    )


def _required_selection_strategy_ids() -> list[str]:
    strategy_ids: list[str] = []
    seen: set[str] = set()

    for account in get_enabled_accounts():
        strategy_id = account.strategy_id
        if strategy_id in seen:
            continue
        seen.add(strategy_id)

        if get_strategy_definition(strategy_id).requires_selection:
            strategy_ids.append(strategy_id)

    return strategy_ids


def _notify_failure(previous_state: dict[str, object], message: str) -> None:
    if previous_state.get("status") == "failed" and previous_state.get("error_text") == message:
        return

    send_notification(
        NOTIFICATION_CHANNEL,
        message,
        title="Nightly Prep Failed",
        priority="high",
        tags=("warning",),
    )


def _validate_selection_snapshots(run_date: date) -> dict[str, int]:
    counts: dict[str, int] = {}
    missing: list[str] = []
    empty: list[str] = []

    for strategy_id in _required_selection_strategy_ids():
        row_count = get_saved_selection_row_count(run_date, strategy_id=strategy_id)
        if row_count is None:
            missing.append(strategy_id)
            continue
        if row_count <= 0:
            empty.append(strategy_id)
            continue
        counts[strategy_id] = row_count

    if missing or empty:
        parts: list[str] = []
        if missing:
            parts.append(f"missing tables: {', '.join(missing)}")
        if empty:
            parts.append(f"empty tables: {', '.join(empty)}")
        raise RuntimeError("Selection validation failed - " + "; ".join(parts))

    return counts


def run_nightly_prep_once(current_dt: datetime | None = None) -> str:
    resolved_dt = current_dt or now_kst()
    if not within_kst_window(resolved_dt, start=PREP_WINDOW_START, end=PREP_WINDOW_END):
        return "outside_window"

    run_date = resolved_dt.date()
    with scheduler_lock("nightly_prep") as acquired:
        if not acquired:
            return "locked"

        state = load_nightly_prep_state(run_date)
        if state["status"] == "completed":
            return "completed"

        if state["crawler_finished_at"] is None:
            failure_state = state.copy()
            save_nightly_prep_state(
                run_date,
                status="running",
                crawler_started_at=state["crawler_started_at"] or resolved_dt.isoformat(),
                error_text=None,
            )
            try:
                run_financial_crawler()
            except Exception as exc:
                message = f"Crawler failed for {run_date.isoformat()}: {exc}"
                save_nightly_prep_state(run_date, status="failed", error_text=message)
                _notify_failure(failure_state, message)
                return "failed"

            save_nightly_prep_state(
                run_date,
                status="crawler_completed",
                crawler_finished_at=now_kst().isoformat(),
                error_text=None,
            )

        state = load_nightly_prep_state(run_date)
        if state["selection_finished_at"] is None:
            failure_state = state.copy()
            save_nightly_prep_state(
                run_date,
                status="running",
                selection_started_at=state["selection_started_at"] or now_kst().isoformat(),
                error_text=None,
            )
            try:
                run_stock_selection()
                counts = _validate_selection_snapshots(run_date)
            except Exception as exc:
                message = f"Selection failed for {run_date.isoformat()}: {exc}"
                save_nightly_prep_state(run_date, status="failed", error_text=message)
                _notify_failure(failure_state, message)
                return "failed"

            save_nightly_prep_state(
                run_date,
                status="completed",
                selection_finished_at=now_kst().isoformat(),
                error_text=None,
            )
            logger.info(
                "Nightly prep completed for %s with selection counts %s",
                run_date.isoformat(),
                counts,
            )
            return "completed"

        save_nightly_prep_state(run_date, status="completed", error_text=None)
        return "completed"


def main() -> None:
    _configure_logging()
    logger.info("Nightly prep controller started.")

    while True:
        try:
            run_nightly_prep_once()
        except Exception as exc:
            logger.error("Nightly prep controller loop failed: %s", exc, exc_info=True)
        time.sleep(POLL_INTERVAL_SECONDS)


if __name__ == "__main__":
    main()
