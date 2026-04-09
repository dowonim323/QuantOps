from __future__ import annotations

import logging
import threading
import time
from datetime import date, datetime, time as dt_time
from pathlib import Path
from typing import Mapping, get_args

from pipelines.trading_session import AccountRunStatus, run_trading_session
from strategies import get_strategy_definition
from tools.logger import configure_entrypoint_logging
from tools.notifications import send_notification
from tools.scheduler_state import (
    list_unresolved_trading_day_reviews,
    load_trading_day_state,
    save_trading_day_state,
    scheduler_lock,
)
from tools.selection_store import get_saved_selection_row_count
from tools.time_utils import now_kst, within_kst_window
from tools.trading_profiles import AccountProfile, get_enabled_accounts

LAUNCH_WINDOW_START = dt_time(hour=8, minute=30)
LAUNCH_WINDOW_END = dt_time(hour=15, minute=30)
POLL_INTERVAL_SECONDS = 60
HEARTBEAT_INTERVAL_SECONDS = 30
NOTIFICATION_CHANNEL = "trade_execution"
KNOWN_SESSION_STATUSES = frozenset(get_args(AccountRunStatus))
ABNORMAL_SESSION_STATUSES = frozenset({"error", "interrupted"})

logger = logging.getLogger(__name__)
BASE_DIR = Path(__file__).resolve().parent.parent


def _configure_logging() -> None:
    configure_entrypoint_logging(BASE_DIR)


def _load_states(
    run_date: date,
    accounts: list[AccountProfile],
) -> dict[str, dict[str, object]]:
    return {
        account.account_id: load_trading_day_state(run_date, account_id=account.account_id)
        for account in accounts
    }


def _save_states(
    run_date: date,
    accounts: list[AccountProfile],
    **fields: object,
) -> None:
    for account in accounts:
        save_trading_day_state(run_date, account_id=account.account_id, **fields)


def _notify_transition(
    previous_states: dict[str, dict[str, object]],
    *,
    status: str,
    error_text: str | None,
    manual_review_required: bool,
    title: str,
) -> None:
    if all(
        state["status"] == status
        and state["error_text"] == error_text
        and bool(state["manual_review_required"]) == manual_review_required
        for state in previous_states.values()
    ):
        return

    if error_text is None:
        return

    send_notification(
        NOTIFICATION_CHANNEL,
        error_text,
        title=title,
        priority="high",
        tags=("warning",),
    )


def _launch_metadata(
    run_date: date,
    accounts: list[AccountProfile],
) -> dict[str, dict[str, str | None]]:
    metadata: dict[str, dict[str, str | None]] = {}

    for account in accounts:
        launch_mode = "normal"
        launch_reason = None

        strategy = get_strategy_definition(account.strategy_id)
        if strategy.requires_selection:
            row_count = get_saved_selection_row_count(run_date, strategy_id=account.strategy_id)
            if row_count is None:
                launch_mode = "degraded_sell_only"
                launch_reason = (
                    f"Saved selection is unavailable for {account.strategy_id}. "
                    "Buy/rebalance paths may be skipped while sell-capable monitoring continues."
                )
            elif row_count <= 0:
                launch_mode = "degraded_sell_only"
                launch_reason = (
                    f"Saved selection is empty for {account.strategy_id}. "
                    "Buy/rebalance paths may be skipped while sell-capable monitoring continues."
                )

        if launch_mode != "normal":
            logger.warning(
                "Trading day launch degraded for %s account %s: %s (%s)",
                run_date.isoformat(),
                account.account_id,
                launch_mode,
                launch_reason,
            )

        metadata[account.account_id] = {
            "launch_mode": launch_mode,
            "launch_reason": launch_reason,
        }

    return metadata


def _incomplete_session_error(
    run_date: date,
    states: dict[str, dict[str, object]],
) -> str:
    affected_accounts = ", ".join(sorted(states))
    return (
        f"Trading day session for {run_date.isoformat()} requires manual review.\n"
        f"Accounts: {affected_accounts}\n"
        "A previous session started but did not finish cleanly."
    )


def _session_result_error(run_date: date, results: Mapping[str, str]) -> str:
    details = ", ".join(f"{account_id}={status}" for account_id, status in sorted(results.items()))
    return (
        f"Trading day session for {run_date.isoformat()} requires manual review.\n"
        f"Results: {details}"
    )


def _unknown_session_result_error(run_date: date, results: Mapping[str, str]) -> str:
    details = ", ".join(f"{account_id}={status}" for account_id, status in sorted(results.items()))
    return (
        f"Trading day session for {run_date.isoformat()} requires manual review.\n"
        f"Unknown results: {details}"
    )


def _restart_count(state: dict[str, object]) -> int:
    value = state.get("restart_count")
    return int(value) if isinstance(value, int) else 0


def _prior_review_error(run_date: date, pending_reviews: list[dict[str, object]]) -> str:
    details = ", ".join(
        f"{review['run_date']}:{review['account_id']}"
        for review in pending_reviews
    )
    return (
        f"Trading day launch blocked for {run_date.isoformat()}.\n"
        f"Manual review pending: {details}"
    )


def _save_running_states(
    run_date: date,
    accounts: list[AccountProfile],
    *,
    session_started_at: str,
    launch_metadata: dict[str, dict[str, str | None]],
) -> None:
    heartbeat_at = now_kst().isoformat()

    for account in accounts:
        account_metadata = launch_metadata[account.account_id]
        save_trading_day_state(
            run_date,
            account_id=account.account_id,
            status="running",
            phase="running",
            session_started_at=session_started_at,
            last_heartbeat_at=heartbeat_at,
            launch_mode=account_metadata["launch_mode"],
            launch_reason=account_metadata["launch_reason"],
            error_text=None,
            manual_review_required=False,
        )


def _start_heartbeat_thread(
    run_date: date,
    accounts: list[AccountProfile],
) -> tuple[threading.Event, threading.Thread]:
    stop_event = threading.Event()

    def _heartbeat_loop() -> None:
        while not stop_event.wait(HEARTBEAT_INTERVAL_SECONDS):
            heartbeat_at = now_kst().isoformat()
            for account in accounts:
                save_trading_day_state(
                    run_date,
                    account_id=account.account_id,
                    phase="running",
                    last_heartbeat_at=heartbeat_at,
                )

    thread = threading.Thread(
        target=_heartbeat_loop,
        name=f"trading-heartbeat-{run_date.isoformat()}",
        daemon=True,
    )
    thread.start()
    return stop_event, thread
    return (
        f"Trading day launch blocked for {run_date.isoformat()}.\n"
        f"Manual review pending: {details}"
    )


def run_trading_day_once(current_dt: datetime | None = None) -> str:
    resolved_dt = current_dt or now_kst()
    run_date = resolved_dt.date()
    accounts = get_enabled_accounts()
    if not accounts:
        raise ValueError("No enabled trading accounts configured.")

    with scheduler_lock("trading_day") as acquired:
        if not acquired:
            logger.info(
                "Trading day launch skipped for %s: controller lock not acquired.",
                run_date.isoformat(),
            )
            return "locked"

        states = _load_states(run_date, accounts)
        if all(state["status"] == "completed" for state in states.values()):
            logger.info(
                "Trading day launch skipped for %s: all account states already completed.",
                run_date.isoformat(),
            )
            return "completed"

        if any(
            bool(state["manual_review_required"])
            or (state["session_started_at"] and not state["session_finished_at"])
            for state in states.values()
        ):
            message = _incomplete_session_error(run_date, states)
            _save_states(
                run_date,
                accounts,
                status="blocked",
                phase="manual_review",
                manual_review_required=True,
                error_text=message,
            )
            _notify_transition(
                states,
                status="blocked",
                error_text=message,
                manual_review_required=True,
                title="Trading Day Manual Review Required",
            )
            logger.warning(
                "Trading day launch blocked for %s: current-day session requires manual review.",
                run_date.isoformat(),
            )
            return "blocked"

        if not within_kst_window(
            resolved_dt,
            start=LAUNCH_WINDOW_START,
            end=LAUNCH_WINDOW_END,
        ):
            logger.info(
                "Trading day launch skipped for %s at %s: outside launch window %s-%s.",
                run_date.isoformat(),
                resolved_dt.isoformat(),
                LAUNCH_WINDOW_START.isoformat(),
                LAUNCH_WINDOW_END.isoformat(),
            )
            return "outside_window"

        account_ids = [account.account_id for account in accounts]
        prior_reviews = list_unresolved_trading_day_reviews(
            before_run_date=run_date,
            account_ids=account_ids,
        )
        if prior_reviews:
            message = _prior_review_error(run_date, prior_reviews)
            _save_states(
                run_date,
                accounts,
                status="waiting_for_review",
                phase="prior_review_blocked",
                manual_review_required=False,
                error_text=message,
            )
            _notify_transition(
                states,
                status="waiting_for_review",
                error_text=message,
                manual_review_required=False,
                title="Trading Day Waiting For Manual Review",
            )
            logger.warning(
                "Trading day launch blocked for %s: unresolved prior manual reviews (%s).",
                run_date.isoformat(),
                ", ".join(
                    f"{review['run_date']}:{review['account_id']}"
                    for review in prior_reviews
                ),
            )
            return "blocked"

        launch_metadata = _launch_metadata(run_date, accounts)
        session_started_at = resolved_dt.isoformat()
        logger.info(
            "Trading day session starting for %s with launch modes: %s",
            run_date.isoformat(),
            ", ".join(
                f"{account_id}={metadata['launch_mode']}"
                for account_id, metadata in sorted(launch_metadata.items())
            ),
        )
        _save_running_states(
            run_date,
            accounts,
            session_started_at=session_started_at,
            launch_metadata=launch_metadata,
        )
        heartbeat_stop_event, heartbeat_thread = _start_heartbeat_thread(run_date, accounts)

        try:
            results = run_trading_session()
        except Exception as exc:
            heartbeat_stop_event.set()
            heartbeat_thread.join(timeout=HEARTBEAT_INTERVAL_SECONDS)
            previous_states = _load_states(run_date, accounts)
            message = f"Trading day controller failed for {run_date.isoformat()}: {exc}"
            logger.error(
                "Trading day session failed for %s: %s",
                run_date.isoformat(),
                exc,
                exc_info=True,
            )
            for account in accounts:
                state = previous_states[account.account_id]
                save_trading_day_state(
                    run_date,
                    account_id=account.account_id,
                    status="blocked",
                    phase="controller_exception",
                    manual_review_required=True,
                    restart_count=_restart_count(state) + 1,
                    error_text=message,
                )
            _notify_transition(
                previous_states,
                status="blocked",
                error_text=message,
                manual_review_required=True,
                title="Trading Day Manual Review Required",
            )
            return "failed"

        heartbeat_stop_event.set()
        heartbeat_thread.join(timeout=HEARTBEAT_INTERVAL_SECONDS)

        previous_states = _load_states(run_date, accounts)
        logger.info(
            "Trading day session returned results for %s: %s",
            run_date.isoformat(),
            results,
        )
        missing_accounts = [account.account_id for account in accounts if account.account_id not in results]
        if missing_accounts:
            message = (
                f"Trading day session for {run_date.isoformat()} requires manual review.\n"
                f"Missing account results: {', '.join(missing_accounts)}"
            )
            logger.error(
                "Trading day session blocked for %s: missing account results for %s.",
                run_date.isoformat(),
                ", ".join(missing_accounts),
            )
            for account in accounts:
                state = previous_states[account.account_id]
                save_trading_day_state(
                    run_date,
                    account_id=account.account_id,
                    status="blocked",
                    phase="missing_results",
                    manual_review_required=True,
                    restart_count=_restart_count(state) + 1,
                    error_text=message,
                )
            _notify_transition(
                previous_states,
                status="blocked",
                error_text=message,
                manual_review_required=True,
                title="Trading Day Manual Review Required",
            )
            return "blocked"

        unknown_results = {
            account_id: status
            for account_id, status in results.items()
            if status not in KNOWN_SESSION_STATUSES
        }
        if unknown_results:
            message = _unknown_session_result_error(run_date, unknown_results)
            logger.error(
                "Trading day session blocked for %s: unknown account results %s.",
                run_date.isoformat(),
                unknown_results,
            )
            for account in accounts:
                state = previous_states[account.account_id]
                save_trading_day_state(
                    run_date,
                    account_id=account.account_id,
                    status="blocked",
                    phase="manual_review",
                    manual_review_required=True,
                    restart_count=_restart_count(state) + 1,
                    error_text=message,
                )
            _notify_transition(
                previous_states,
                status="blocked",
                error_text=message,
                manual_review_required=True,
                title="Trading Day Manual Review Required",
            )
            return "blocked"

        if any(status in ABNORMAL_SESSION_STATUSES for status in results.values()):
            message = _session_result_error(run_date, results)
            logger.warning(
                "Trading day session blocked for %s: abnormal account results %s.",
                run_date.isoformat(),
                results,
            )
            for account in accounts:
                state = previous_states[account.account_id]
                save_trading_day_state(
                    run_date,
                    account_id=account.account_id,
                    status="blocked",
                    phase="manual_review",
                    manual_review_required=True,
                    restart_count=_restart_count(state) + 1,
                    error_text=message,
                )
            _notify_transition(
                previous_states,
                status="blocked",
                error_text=message,
                manual_review_required=True,
                title="Trading Day Manual Review Required",
            )
            return "blocked"

        session_finished_at = now_kst().isoformat()
        _save_states(
            run_date,
            accounts,
            status="completed",
            phase="completed",
            session_finished_at=session_finished_at,
            last_heartbeat_at=session_finished_at,
            error_text=None,
            manual_review_required=False,
        )
        logger.info(
            "Trading day session completed for %s.",
            run_date.isoformat(),
        )
        return "completed"


def main() -> None:
    _configure_logging()
    logger.info("Trading day controller started.")

    while True:
        try:
            run_trading_day_once()
        except Exception as exc:
            logger.error("Trading day controller loop failed: %s", exc, exc_info=True)
        time.sleep(POLL_INTERVAL_SECONDS)


if __name__ == "__main__":
    main()
