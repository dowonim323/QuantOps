from __future__ import annotations

import fcntl
import sqlite3
from contextlib import contextmanager
from datetime import date, datetime
from pathlib import Path
from typing import Any, Iterator, Sequence

from .time_utils import now_kst, today_kst

BASE_DIR = Path(__file__).resolve().parent.parent
DB_DIR = BASE_DIR / "db" / "scheduler"
DB_PATH = DB_DIR / "controller_state.db"
LOCK_DIR = DB_DIR / "locks"

_db_initialized: set[Path] = set()


def _resolve_db_path() -> Path:
    return DB_PATH


def _normalize_run_date(run_date: date | datetime | str | None = None) -> str:
    if run_date is None:
        return today_kst().isoformat()
    if isinstance(run_date, datetime):
        return run_date.date().isoformat()
    if isinstance(run_date, date):
        return run_date.isoformat()

    text = str(run_date).strip()
    if len(text) == 8 and text.isdigit():
        return f"{text[:4]}-{text[4:6]}-{text[6:]}"

    return date.fromisoformat(text).isoformat()


def _init_db() -> None:
    db_path = _resolve_db_path()
    if db_path in _db_initialized:
        return

    DB_DIR.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(db_path, timeout=30.0) as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS nightly_prep_runs (
                run_date TEXT PRIMARY KEY,
                status TEXT NOT NULL DEFAULT 'pending',
                crawler_started_at TEXT,
                crawler_finished_at TEXT,
                selection_started_at TEXT,
                selection_finished_at TEXT,
                error_text TEXT,
                updated_at TEXT NOT NULL
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS trading_day_runs (
                run_date TEXT NOT NULL,
                account_id TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'pending',
                session_started_at TEXT,
                session_finished_at TEXT,
                phase TEXT,
                launch_mode TEXT,
                launch_reason TEXT,
                last_heartbeat_at TEXT,
                restart_count INTEGER NOT NULL DEFAULT 0,
                manual_review_required INTEGER NOT NULL DEFAULT 0,
                error_text TEXT,
                updated_at TEXT NOT NULL,
                PRIMARY KEY (run_date, account_id)
            )
            """
        )
        _ensure_column(conn, "trading_day_runs", "launch_mode", "TEXT")
        _ensure_column(conn, "trading_day_runs", "launch_reason", "TEXT")
        _ensure_column(conn, "trading_day_runs", "last_heartbeat_at", "TEXT")

    _db_initialized.add(db_path)


def _ensure_column(
    conn: sqlite3.Connection,
    table_name: str,
    column_name: str,
    column_definition: str,
) -> None:
    rows = conn.execute(f"PRAGMA table_info({table_name})").fetchall()
    existing_columns = {row[1] for row in rows}
    if column_name in existing_columns:
        return

    conn.execute(
        f"ALTER TABLE {table_name} ADD COLUMN {column_name} {column_definition}",
    )


def get_db_connection() -> sqlite3.Connection:
    _init_db()
    return sqlite3.connect(_resolve_db_path(), timeout=30.0)


def load_nightly_prep_state(
    run_date: date | datetime | str | None = None,
) -> dict[str, Any]:
    run_date_str = _normalize_run_date(run_date)
    with get_db_connection() as conn:
        row = conn.execute(
            """
            SELECT status, crawler_started_at, crawler_finished_at,
                   selection_started_at, selection_finished_at,
                   error_text, updated_at
            FROM nightly_prep_runs
            WHERE run_date = ?
            """,
            (run_date_str,),
        ).fetchone()

    if row is None:
        return {
            "run_date": run_date_str,
            "status": "pending",
            "crawler_started_at": None,
            "crawler_finished_at": None,
            "selection_started_at": None,
            "selection_finished_at": None,
            "error_text": None,
            "updated_at": None,
        }

    return {
        "run_date": run_date_str,
        "status": row[0],
        "crawler_started_at": row[1],
        "crawler_finished_at": row[2],
        "selection_started_at": row[3],
        "selection_finished_at": row[4],
        "error_text": row[5],
        "updated_at": row[6],
    }


def save_nightly_prep_state(
    run_date: date | datetime | str | None = None,
    **fields: Any,
) -> dict[str, Any]:
    state = load_nightly_prep_state(run_date)
    state.update(fields)
    state["run_date"] = _normalize_run_date(run_date)
    state["updated_at"] = fields.get("updated_at") or now_kst().isoformat()

    with get_db_connection() as conn:
        conn.execute(
            """
            INSERT INTO nightly_prep_runs (
                run_date,
                status,
                crawler_started_at,
                crawler_finished_at,
                selection_started_at,
                selection_finished_at,
                error_text,
                updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(run_date) DO UPDATE SET
                status = excluded.status,
                crawler_started_at = excluded.crawler_started_at,
                crawler_finished_at = excluded.crawler_finished_at,
                selection_started_at = excluded.selection_started_at,
                selection_finished_at = excluded.selection_finished_at,
                error_text = excluded.error_text,
                updated_at = excluded.updated_at
            """,
            (
                state["run_date"],
                state["status"],
                state["crawler_started_at"],
                state["crawler_finished_at"],
                state["selection_started_at"],
                state["selection_finished_at"],
                state["error_text"],
                state["updated_at"],
            ),
        )

    return state


def load_trading_day_state(
    run_date: date | datetime | str | None = None,
    *,
    account_id: str,
) -> dict[str, Any]:
    run_date_str = _normalize_run_date(run_date)
    with get_db_connection() as conn:
        row = conn.execute(
            """
            SELECT status, session_started_at, session_finished_at,
                   phase, launch_mode, launch_reason, last_heartbeat_at,
                   restart_count, manual_review_required,
                   error_text, updated_at
            FROM trading_day_runs
            WHERE run_date = ? AND account_id = ?
            """,
            (run_date_str, account_id),
        ).fetchone()

    if row is None:
        return {
            "run_date": run_date_str,
            "account_id": account_id,
            "status": "pending",
            "session_started_at": None,
            "session_finished_at": None,
            "phase": None,
            "launch_mode": None,
            "launch_reason": None,
            "last_heartbeat_at": None,
            "restart_count": 0,
            "manual_review_required": False,
            "error_text": None,
            "updated_at": None,
        }

    return {
        "run_date": run_date_str,
        "account_id": account_id,
        "status": row[0],
        "session_started_at": row[1],
        "session_finished_at": row[2],
        "phase": row[3],
        "launch_mode": row[4],
        "launch_reason": row[5],
        "last_heartbeat_at": row[6],
        "restart_count": int(row[7]),
        "manual_review_required": bool(row[8]),
        "error_text": row[9],
        "updated_at": row[10],
    }


def save_trading_day_state(
    run_date: date | datetime | str | None = None,
    *,
    account_id: str,
    **fields: Any,
) -> dict[str, Any]:
    state = load_trading_day_state(run_date, account_id=account_id)
    state.update(fields)
    state["run_date"] = _normalize_run_date(run_date)
    state["account_id"] = account_id
    state["updated_at"] = fields.get("updated_at") or now_kst().isoformat()

    with get_db_connection() as conn:
        conn.execute(
            """
            INSERT INTO trading_day_runs (
                run_date,
                account_id,
                status,
                session_started_at,
                session_finished_at,
                phase,
                launch_mode,
                launch_reason,
                last_heartbeat_at,
                restart_count,
                manual_review_required,
                error_text,
                updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(run_date, account_id) DO UPDATE SET
                status = excluded.status,
                session_started_at = excluded.session_started_at,
                session_finished_at = excluded.session_finished_at,
                phase = excluded.phase,
                launch_mode = excluded.launch_mode,
                launch_reason = excluded.launch_reason,
                last_heartbeat_at = excluded.last_heartbeat_at,
                restart_count = excluded.restart_count,
                manual_review_required = excluded.manual_review_required,
                error_text = excluded.error_text,
                updated_at = excluded.updated_at
            """,
            (
                state["run_date"],
                state["account_id"],
                state["status"],
                state["session_started_at"],
                state["session_finished_at"],
                state["phase"],
                state["launch_mode"],
                state["launch_reason"],
                state["last_heartbeat_at"],
                state["restart_count"],
                int(bool(state["manual_review_required"])),
                state["error_text"],
                state["updated_at"],
            ),
        )

    state["manual_review_required"] = bool(state["manual_review_required"])
    return state


def list_unresolved_trading_day_reviews(
    *,
    before_run_date: date | datetime | str | None = None,
    account_ids: Sequence[str] | None = None,
) -> list[dict[str, Any]]:
    clauses = ["manual_review_required = 1"]
    params: list[Any] = []

    if before_run_date is not None:
        clauses.append("run_date < ?")
        params.append(_normalize_run_date(before_run_date))

    if account_ids:
        placeholders = ", ".join("?" for _ in account_ids)
        clauses.append(f"account_id IN ({placeholders})")
        params.extend(account_ids)

    query = (
        "SELECT run_date, account_id, status, session_started_at, session_finished_at, "
        "phase, launch_mode, launch_reason, last_heartbeat_at, "
        "restart_count, manual_review_required, error_text, updated_at "
        "FROM trading_day_runs WHERE "
        + " AND ".join(clauses)
        + " ORDER BY run_date ASC, account_id ASC"
    )

    with get_db_connection() as conn:
        rows = conn.execute(query, tuple(params)).fetchall()

    return [
        {
            "run_date": row[0],
            "account_id": row[1],
            "status": row[2],
            "session_started_at": row[3],
            "session_finished_at": row[4],
            "phase": row[5],
            "launch_mode": row[6],
            "launch_reason": row[7],
            "last_heartbeat_at": row[8],
            "restart_count": int(row[9]),
            "manual_review_required": bool(row[10]),
            "error_text": row[11],
            "updated_at": row[12],
        }
        for row in rows
    ]


def clear_trading_day_manual_review(
    run_date: date | datetime | str,
    *,
    account_id: str,
) -> dict[str, Any]:
    state = load_trading_day_state(run_date, account_id=account_id)

    status = state["status"]
    if status == "blocked":
        status = "reviewed"

    phase = state["phase"]
    if phase in {"manual_review", "controller_exception", "missing_results"}:
        phase = "reviewed"

    return save_trading_day_state(
        run_date,
        account_id=account_id,
        status=status,
        phase=phase,
        manual_review_required=False,
        error_text=None,
    )


@contextmanager
def scheduler_lock(name: str) -> Iterator[bool]:
    LOCK_DIR.mkdir(parents=True, exist_ok=True)
    lock_path = LOCK_DIR / f"{name}.lock"
    with lock_path.open("w", encoding="utf-8") as handle:
        try:
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            acquired = True
        except BlockingIOError:
            acquired = False

        try:
            yield acquired
        finally:
            if acquired:
                fcntl.flock(handle.fileno(), fcntl.LOCK_UN)


__all__ = [
    "clear_trading_day_manual_review",
    "load_nightly_prep_state",
    "load_trading_day_state",
    "list_unresolved_trading_day_reviews",
    "save_nightly_prep_state",
    "save_trading_day_state",
    "scheduler_lock",
]
