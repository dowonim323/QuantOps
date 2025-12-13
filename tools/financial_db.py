from __future__ import annotations

import shutil
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Dict, Iterable, Tuple

import pandas as pd

BASE_DIR = Path(__file__).resolve().parent.parent
DB_DIR = BASE_DIR / "db"
DB_HISTORY_DIR = BASE_DIR / "db_history"
DB_HISTORY_FIN_DIR = DB_HISTORY_DIR / "financial_data"
DB_HISTORY_QUANT_DIR = DB_HISTORY_DIR / "quant"
FINANCIAL_DATA_DIR = DB_DIR / "financial_data"
QUANT_DATA_DIR = DB_DIR / "quant"


def _resolve_financial_db_path(filename: str) -> Path:
    FINANCIAL_DATA_DIR.mkdir(parents=True, exist_ok=True)
    legacy_path = DB_DIR / filename
    new_path = FINANCIAL_DATA_DIR / filename

    if legacy_path.exists() and not new_path.exists():
        new_path.parent.mkdir(parents=True, exist_ok=True)
        shutil.move(legacy_path, new_path)

    return new_path


def _resolve_quant_db_path(filename: str) -> Path:
    QUANT_DATA_DIR.mkdir(parents=True, exist_ok=True)
    legacy_path = DB_DIR / filename
    new_path = QUANT_DATA_DIR / filename

    if legacy_path.exists() and not new_path.exists():
        shutil.move(legacy_path, new_path)

    return new_path

GroupKey = Tuple[str, str]

GROUP_DB_MAP: Dict[GroupKey, Path] = {
    ("ratio", "year"): _resolve_financial_db_path("financial_ratio_year.db"),
    ("ratio", "quarter"): _resolve_financial_db_path("financial_ratio_quarter.db"),
    ("income", "year"): _resolve_financial_db_path("income_statement_year.db"),
    ("income", "quarter"): _resolve_financial_db_path("income_statement_quarter.db"),
    ("balance", "year"): _resolve_financial_db_path("balance_sheet_year.db"),
    ("balance", "quarter"): _resolve_financial_db_path("balance_sheet_quarter.db"),
    ("cashflow", "year"): _resolve_financial_db_path("cash_flow_sheet_year.db"),
    ("cashflow", "quarter"): _resolve_financial_db_path("cash_flow_sheet_quarter.db"),
}

STOCK_SELECTION_DB_PATH = _resolve_quant_db_path("stock_selection.db")


def ensure_table(conn: sqlite3.Connection, code: str) -> None:
    conn.execute(f'CREATE TABLE IF NOT EXISTS "{code}" (metric TEXT PRIMARY KEY)')


def ensure_columns(
    conn: sqlite3.Connection,
    code: str,
    columns: Iterable[str],
) -> None:
    column_list = list(columns)
    if not column_list:
        return

    existing_columns = {
        row[1] for row in conn.execute(f'PRAGMA table_info("{code}")')
    }

    for column in column_list:
        if column in existing_columns:
            continue
        conn.execute(f'ALTER TABLE "{code}" ADD COLUMN "{column}" REAL')
        existing_columns.add(column)


def _ensure_history_structure() -> None:
    DB_HISTORY_FIN_DIR.mkdir(parents=True, exist_ok=True)
    DB_HISTORY_QUANT_DIR.mkdir(parents=True, exist_ok=True)
    for path in DB_HISTORY_DIR.iterdir():
        if path in {DB_HISTORY_FIN_DIR, DB_HISTORY_QUANT_DIR} or not path.is_dir():
            continue
        destination = DB_HISTORY_FIN_DIR / path.name
        if destination.exists():
            continue
        shutil.move(path, destination)


def _resolved_backup_dir(history_root: Path, timestamp: str) -> Path:
    candidate = history_root / timestamp
    if not candidate.exists():
        return candidate

    suffix = 1
    while True:
        deduped = history_root / f"{timestamp}_{suffix}"
        if not deduped.exists():
            return deduped
        suffix += 1


def _prune_history_entries(history_root: Path, limit: int) -> None:
    if limit <= 0 or not history_root.exists():
        return

    history_dirs = sorted(
        [path for path in history_root.iterdir() if path.is_dir()],
        key=lambda path: path.name,
    )

    overflow = len(history_dirs) - limit
    if overflow <= 0:
        return

    for directory in history_dirs[:overflow]:
        shutil.rmtree(directory, ignore_errors=True)


def _backup_db_group(
    source_dir: Path,
    history_root: Path,
    history_limit: int,
) -> Path | None:
    _ensure_history_structure()
    source_dir.mkdir(parents=True, exist_ok=True)
    db_files = sorted(source_dir.glob("*.db"))

    if not db_files:
        _prune_history_entries(history_root, history_limit)
        return None

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_dir = _resolved_backup_dir(history_root, timestamp)
    backup_dir.mkdir(parents=True, exist_ok=False)

    for db_file in db_files:
        destination = backup_dir / db_file.name
        shutil.copy2(db_file, destination)

    _prune_history_entries(history_root, history_limit)
    return backup_dir


def backup_databases(history_limit: int = 30) -> Path | None:
    return _backup_db_group(FINANCIAL_DATA_DIR, DB_HISTORY_FIN_DIR, history_limit)


def backup_quant_databases(history_limit: int = 30) -> Path | None:
    return _backup_db_group(QUANT_DATA_DIR, DB_HISTORY_QUANT_DIR, history_limit)


def update_db(report_type: str, period: str, code: str, df: pd.DataFrame) -> None:
    group_key = (report_type, period)
    if group_key not in GROUP_DB_MAP:
        raise ValueError(f"알 수 없는 그룹: {group_key}")

    db_path = GROUP_DB_MAP[group_key]

    db_path.parent.mkdir(parents=True, exist_ok=True)

    # 인덱스/컬럼이 문자열인지 확인
    df = df.copy()
    df.index = df.index.astype(str)
    df.columns = df.columns.astype(str)
    if df.empty:
        return

    with sqlite3.connect(db_path) as conn:
        ensure_table(conn, code)
        ensure_columns(conn, code, df.columns)

        insert_sql = (
            f'INSERT INTO "{code}" (metric) VALUES (?) '
            "ON CONFLICT(metric) DO NOTHING"
        )
        conn.executemany(
            insert_sql,
            ((metric,) for metric in df.index),
        )

        for metric, row in df.iterrows():
            assignments = []
            params = []
            for period_label, value in row.items():
                if value is None or pd.isna(value):
                    continue
                assignments.append(f'"{period_label}" = ?')
                params.append(float(value))

            if not assignments:
                continue

            params.append(metric)
            conn.execute(
                f'UPDATE "{code}" SET {", ".join(assignments)} WHERE metric = ?',
                params,
            )
        conn.commit()


def load_db(report_type: str, period: str, code: str) -> pd.DataFrame:
    group_key = (report_type, period)
    if group_key not in GROUP_DB_MAP:
        raise ValueError(f"알 수 없는 그룹: {group_key}")

    db_path = GROUP_DB_MAP[group_key]
    if not db_path.exists():
        return pd.DataFrame()

    with sqlite3.connect(db_path) as conn:
        try:
            df = pd.read_sql_query(
                f'SELECT * FROM "{code}"',
                conn,
                index_col="metric",
            )
        except (pd.errors.DatabaseError, sqlite3.OperationalError):
            return pd.DataFrame()

    df = df.apply(pd.to_numeric, errors="coerce")
    df.index = df.index.astype(str)
    df.columns = df.columns.astype(str)
    return df

